/* ──────────────────────────────────────────────────────────────────────────
   ChunkEditor — reusable audio + waveform + transcription + accept/reject UI.

   Presentational only: it knows how to render a chunk and decode its audio,
   but the network calls (accept/reject) and page navigation are delegated to
   the host page via async callbacks. One instance is reused across chunks by
   calling .load(chunk) again.

   Usage:
     const editor = new ChunkEditor(mountEl, {
       audioBase: '/recordings/validate/audio',
       onAccept: async (text) => { ... },   // resolve when done
       onReject: async ()     => { ... },
     });
     editor.load({ owner_id, date, filename, transcription, verified_transcription, status });
   ────────────────────────────────────────────────────────────────────────── */
(function (global) {
  const RTL_RE = /[؀-ۿݐ-ݿࢠ-ࣿﭐ-﷿ﹰ-﻿]/;
  const HANDLE_HIT = 10;    // px tolerance for grabbing a trim handle
  const MIN_KEEP   = 0.05;  // seconds — smallest keepable window

  const PLAY_ICON  = '<svg class="icon-play" viewBox="0 0 24 24"><path d="M8 5v14l11-7z"/></svg>';
  const PAUSE_ICON = '<svg class="icon-pause" viewBox="0 0 24 24"><path d="M6 19h4V5H6v14zm8-14v14h4V5h-4z"/></svg>';

  class ChunkEditor {
    constructor(mount, opts = {}) {
      this.mount     = mount;
      this.audioBase = opts.audioBase;
      this.onAccept  = opts.onAccept || (async () => {});
      this.onReject  = opts.onReject || (async () => {});
      this.onIssue   = opts.onIssue || null;   // optional 3rd action
      this.onTrimAccept = opts.onTrimAccept || null;  // optional "Trim & Accept"
      // Content-classification labels (multi-label chips). The host supplies the
      // taxonomy via opts.labels or setLabels(); verifying requires >=1 chosen.
      this.labels      = null;     // [{key,label,desc,exclusive?}] or null
      this._labelMeta  = {};       // key -> meta
      this.labelState  = {};       // key -> bool (current selection)
      this.acceptLabel = opts.acceptLabel || 'Accept';
      this.rejectLabel = opts.rejectLabel || 'Delete';
      this.issueLabel  = opts.issueLabel  || 'Issue';

      this.audio   = new Audio();
      this.audioCtx = null;
      this.peaks   = null;
      this.raf     = null;
      this.objUrl  = null;
      this.chunk   = null;

      // Edge-trim state. trimMode shows two draggable handles on the waveform;
      // [trimStart, trimEnd] (seconds) is the kept window. _dragHandle is the
      // handle being dragged ('start' | 'end' | null).
      this.trimMode    = false;
      this.trimStart   = 0;
      this.trimEnd     = 0;
      this._dragHandle = null;

      this._buildDom();
      this._wireAudio();
      this._wireKeys();
      if (opts.labels) this.setLabels(opts.labels);
    }

    // ── DOM ───────────────────────────────────────────────────────────────
    _buildDom() {
      this.el = document.createElement('div');
      this.el.className = 'ce';
      this.el.innerHTML = `
        <div class="ce-head">
          <div class="ce-file"><span class="ce-name"></span><span class="ce-date"></span></div>
          <span class="ce-status badge"></span>
        </div>
        <div class="ce-player">
          <button class="ce-play" title="Play / pause (Space)">${PLAY_ICON}${PAUSE_ICON}</button>
          <canvas class="ce-wave"></canvas>
          <span class="ce-time">0:00 / 0:00</span>
          <button class="ce-trimtoggle" type="button" title="Trim silence/noise off the start or end">✂</button>
        </div>
        <div class="ce-trim" style="display:none">
          <span class="ce-trim-hint">Drag the edges inward to cut off the start/end — the middle stays intact.</span>
          <span class="ce-trim-keep"></span>
        </div>
        <div class="ce-text-label">Transcription</div>
        <textarea class="ce-text" placeholder="Type what is said in this clip…"></textarea>
        <div class="ce-asr" style="display:none">
          <span class="ce-asr-label">ASR</span>
          <span class="ce-asr-text" dir="auto"></span>
          <button type="button" class="ce-asr-use" title="Copy this suggestion into the box">use</button>
        </div>
        <div class="ce-labels" style="display:none">
          <div class="ce-labels-label">Labels — pick at least one to accept</div>
          <div class="ce-chips"></div>
        </div>
        <div class="ce-actions">
          <button class="ce-btn ce-reject"></button>
          <button class="ce-btn ce-issue"></button>
          <button class="ce-btn ce-accept"></button>
        </div>`;
      this.mount.appendChild(this.el);

      this.nameEl   = this.el.querySelector('.ce-name');
      this.dateEl   = this.el.querySelector('.ce-date');
      this.statusEl = this.el.querySelector('.ce-status');
      this.playBtn  = this.el.querySelector('.ce-play');
      this.canvas   = this.el.querySelector('.ce-wave');
      this.timeEl   = this.el.querySelector('.ce-time');
      this.trimBtn  = this.el.querySelector('.ce-trimtoggle');
      this.trimEl   = this.el.querySelector('.ce-trim');
      this.trimKeepEl = this.el.querySelector('.ce-trim-keep');
      this.textEl   = this.el.querySelector('.ce-text');
      this.asrEl     = this.el.querySelector('.ce-asr');
      this.asrTextEl = this.el.querySelector('.ce-asr-text');
      this.asrUseBtn = this.el.querySelector('.ce-asr-use');
      this.labelsEl  = this.el.querySelector('.ce-labels');
      this.chipsEl   = this.el.querySelector('.ce-chips');
      this.acceptBtn = this.el.querySelector('.ce-accept');
      this.rejectBtn = this.el.querySelector('.ce-reject');
      this.issueBtn  = this.el.querySelector('.ce-issue');

      this.acceptBtn.textContent = this.acceptLabel;
      this.rejectBtn.textContent = this.rejectLabel;
      this.issueBtn.textContent  = this.issueLabel;
      if (!this.onIssue) this.issueBtn.style.display = 'none';  // hide if not wired
      if (!this.onTrimAccept) this.trimBtn.style.display = 'none';  // trim only where wired

      this.playBtn.addEventListener('click', () => this.togglePlay());
      this.trimBtn.addEventListener('click', () => this._toggleTrim());
      this.canvas.addEventListener('pointerdown', (e) => this._onPointerDown(e));
      window.addEventListener('pointermove', (e) => this._onPointerMove(e));
      window.addEventListener('pointerup', () => this._onPointerUp());
      this.textEl.addEventListener('input', () => this._applyDir());
      this.asrUseBtn.addEventListener('click', () => {
        this.textEl.value = this.asrTextEl.textContent;
        this._applyDir();
        this.textEl.focus();
      });

      this.acceptBtn.addEventListener('click', async () => {
        if (!this._labelsOk()) return;   // primary action verifies -> needs a label
        this._busy(true);
        try {
          if (this._hasTrim() && this.onTrimAccept) {
            await this.onTrimAccept(this.textEl.value, this.trimStart, this.trimEnd, this.getLabels());
          } else {
            await this.onAccept(this.textEl.value, this.getLabels());
          }
        } finally { this._busy(false); }
      });
      this.rejectBtn.addEventListener('click', async () => {
        this._busy(true);
        try { await this.onReject(); }
        finally { this._busy(false); }
      });
      this.issueBtn.addEventListener('click', async () => {
        if (!this.onIssue) return;
        this._busy(true);
        try { await this.onIssue(this.textEl.value); }
        finally { this._busy(false); }
      });

      this.chipsEl.addEventListener('click', (e) => {
        const b = e.target.closest('.ce-chip');
        if (b) this._toggleChip(b.dataset.key);
      });

      window.addEventListener('resize', () => this._drawWave());
    }

    _wireAudio() {
      this.audio.addEventListener('play',  () => { this.playBtn.classList.add('playing'); this._loop(); });
      this.audio.addEventListener('pause', () => { this.playBtn.classList.remove('playing'); this._stopLoop(); this._drawWave(); });
      this.audio.addEventListener('ended', () => { this.playBtn.classList.remove('playing'); this._stopLoop(); this._drawWave(); });
      this.audio.addEventListener('timeupdate', () => this._updateTime());
      this.audio.addEventListener('loadedmetadata', () => this._updateTime());
    }

    _wireKeys() {
      document.addEventListener('keydown', (e) => {
        if (e.code !== 'Space') return;
        const tag = (document.activeElement && document.activeElement.tagName) || '';
        if (tag === 'TEXTAREA' || tag === 'INPUT') return;  // don't hijack typing
        if (!this.chunk) return;
        e.preventDefault();
        this.togglePlay();
      });
    }

    // ── Public ────────────────────────────────────────────────────────────
    setActionLabels(acceptLabel, rejectLabel) {
      this.acceptLabel = acceptLabel; this.rejectLabel = rejectLabel;
      this.rejectBtn.textContent = rejectLabel;
      this._refreshPrimary();   // keeps the "Trim & Accept" morph intact
    }

    async load(chunk) {
      this.chunk = chunk;
      this.nameEl.textContent = chunk.filename;
      this.dateEl.textContent = chunk.date;
      this._setStatus(chunk.status);

      // Text shown in the box, in priority order:
      //   saved human text present  -> show it (covers a re-opened chunk: pending
      //       again but KEEPS its verified_transcription, so re-validating is
      //       "add labels + accept", never a retype; also a normal decided chunk).
      //   decided but no saved text -> empty (an explicit empty save stays empty;
      //       never fall back to ASR).
      //   fresh pending             -> prefill the ASR guess as a starting point.
      const decided = chunk.status && chunk.status !== 'pending';
      const saved = chunk.verified_transcription;
      const hasSaved = saved != null && saved !== '';
      const text = hasSaved ? saved : (decided ? '' : (chunk.transcription || ''));
      this.textEl.value = text;
      this._applyDir();

      // Greyed ASR reference: whenever the box shows saved human text, surface the
      // original machine guess underneath (with a "use" button).
      const asr = (chunk.transcription || '').trim();
      if ((decided || hasSaved) && asr) {
        this.asrTextEl.textContent = asr;
        this.asrEl.style.display = 'flex';
      } else {
        this.asrEl.style.display = 'none';
      }

      this._initLabels(chunk.labels);

      // reset playback + waveform + trim state
      this._stop();
      this._resetTrim(chunk.status);
      this.peaks = null;
      this._drawWave();
      this.timeEl.textContent = '0:00 / 0:00';

      const url = `${this.audioBase}/${chunk.owner_id}/${chunk.date}/${chunk.filename}`;
      const token = (this._loadToken = (this._loadToken || 0) + 1);
      try {
        const res = await fetch(url);
        if (!res.ok) throw new Error('audio ' + res.status);
        const buf = await res.arrayBuffer();
        if (token !== this._loadToken) return;  // a newer load() superseded us

        if (this.objUrl) URL.revokeObjectURL(this.objUrl);
        this.objUrl = URL.createObjectURL(new Blob([buf], { type: 'audio/wav' }));
        this.audio.src = this.objUrl;

        this.audioCtx = this.audioCtx || new (global.AudioContext || global.webkitAudioContext)();
        const decoded = await this.audioCtx.decodeAudioData(buf.slice(0));
        if (token !== this._loadToken) return;
        this.peaks = this._computePeaks(decoded);
        this._drawWave();
      } catch (err) {
        console.error('ChunkEditor: failed to load audio', err);
      }
    }

    focusText() { this.textEl.focus(); }

    // ── Playback ──────────────────────────────────────────────────────────
    togglePlay() {
      if (!this.audio.src) return;
      if (this.audio.paused) {
        if (this.audioCtx && this.audioCtx.state === 'suspended') this.audioCtx.resume();
        // In trim mode, keep playback inside the kept window so you preview
        // exactly what "Trim & Accept" will save.
        if (this.trimMode) {
          const t = this.audio.currentTime;
          if (t < this.trimStart || t >= this.trimEnd - 0.005) {
            try { this.audio.currentTime = this.trimStart; } catch (e) {}
          }
        }
        this.audio.play().catch(() => {});
      } else {
        this.audio.pause();
      }
    }

    _stop() {
      this.audio.pause();
      try { this.audio.currentTime = 0; } catch (e) {}
      this._stopLoop();
    }

    // Pointer: drag a trim handle if grabbed, otherwise seek. In trim mode a
    // plain click seeks only WITHIN the kept window.
    _onPointerDown(e) {
      const dur = this.audio.duration;
      if (!dur || !isFinite(dur)) return;
      const r = this.canvas.getBoundingClientRect();
      const px = e.clientX - r.left;
      const time = Math.min(dur, Math.max(0, (px / r.width) * dur));
      if (this.trimMode) {
        const xs = (this.trimStart / dur) * r.width;
        const xe = (this.trimEnd   / dur) * r.width;
        if (Math.abs(px - xs) <= HANDLE_HIT) this._dragHandle = 'start';
        else if (Math.abs(px - xe) <= HANDLE_HIT) this._dragHandle = 'end';
        if (this._dragHandle) {
          try { this.canvas.setPointerCapture(e.pointerId); } catch (_) {}
          e.preventDefault();
          return;
        }
        this.audio.currentTime = Math.min(this.trimEnd, Math.max(this.trimStart, time));
      } else {
        this.audio.currentTime = time;
      }
      this._drawWave();
    }

    _onPointerMove(e) {
      if (!this._dragHandle) return;
      const dur = this.audio.duration;
      if (!dur || !isFinite(dur)) return;
      const r = this.canvas.getBoundingClientRect();
      const time = Math.min(dur, Math.max(0, ((e.clientX - r.left) / r.width) * dur));
      if (this._dragHandle === 'start') {
        this.trimStart = Math.max(0, Math.min(time, this.trimEnd - MIN_KEEP));
      } else {
        this.trimEnd = Math.min(dur, Math.max(time, this.trimStart + MIN_KEEP));
      }
      this._drawWave();
      this._refreshTrimInfo();
      this._refreshPrimary();
    }

    _onPointerUp() { this._dragHandle = null; }

    _toggleTrim() {
      const dur = this.audio.duration;
      if (!this.onTrimAccept || !dur || !isFinite(dur)) return;
      this.trimMode = !this.trimMode;
      if (this.trimMode) { this.trimStart = 0; this.trimEnd = dur; }
      this._dragHandle = null;
      this.trimBtn.classList.toggle('active', this.trimMode);
      this.el.classList.toggle('trimming', this.trimMode);
      this.trimEl.style.display = this.trimMode ? 'flex' : 'none';
      this._refreshTrimInfo();
      this._drawWave();
      this._refreshPrimary();
    }

    // Trim is only offered on pending chunks (the backend rejects deciding an
    // already-decided one). load() calls this to clear state per chunk.
    _resetTrim(status) {
      this.trimMode = false;
      this._dragHandle = null;
      const canTrim = !!this.onTrimAccept && (!status || status === 'pending');
      this.trimBtn.style.display = canTrim ? '' : 'none';
      this.trimBtn.classList.remove('active');
      this.el.classList.remove('trimming');
      this.trimEl.style.display = 'none';
      this._refreshPrimary();
    }

    // A trim is "real" only once a handle has actually moved in from an edge.
    _hasTrim() {
      const dur = this.audio.duration;
      if (!this.trimMode || !dur || !isFinite(dur)) return false;
      return this.trimStart > 0.02 || this.trimEnd < dur - 0.02;
    }

    _refreshPrimary() {
      const trim = this._hasTrim() && !!this.onTrimAccept;
      this.acceptBtn.textContent = trim ? 'Trim & Accept' : this.acceptLabel;
      this.acceptBtn.classList.toggle('trim', trim);
      // The primary action always verifies -> require >=1 label (when the host
      // wired a taxonomy). issue/reject are never gated.
      this.acceptBtn.disabled = !this._labelsOk();
      this.acceptBtn.title = this.acceptBtn.disabled ? 'Pick at least one label first' : '';
    }

    // ── Labels (multi-label content classification) ───────────────────────────
    setLabels(list) {
      this.labels = (list && list.length) ? list.slice() : null;
      this._labelMeta = {};
      this.labelState = {};
      if (this.labels) {
        for (const m of this.labels) { this._labelMeta[m.key] = m; this.labelState[m.key] = false; }
        this.chipsEl.innerHTML = this.labels.map((m) =>
          `<button type="button" class="ce-chip" data-key="${escHtml(m.key)}" title="${escHtml(m.desc || '')}">${escHtml(m.label)}</button>`
        ).join('');
        this.labelsEl.style.display = '';
      } else {
        this.chipsEl.innerHTML = '';
        this.labelsEl.style.display = 'none';
      }
      if (this.chunk) this._initLabels(this.chunk.labels);
      this._refreshPrimary();
    }

    getLabels() { return this.labels ? { ...this.labelState } : null; }

    _labelsOk() {
      if (!this.labels) return true;                 // host didn't wire labels
      for (const k in this.labelState) if (this.labelState[k]) return true;
      return false;                                  // >=1 required to verify
    }

    // `normal` is the mutually-exclusive "nothing special" choice: turning it on
    // clears the others, and turning any other on clears it.
    _toggleChip(key) {
      const meta = this._labelMeta[key];
      if (!meta) return;
      const turningOn = !this.labelState[key];
      if (turningOn && meta.exclusive) {
        for (const k in this.labelState) this.labelState[k] = false;
      } else if (turningOn) {
        for (const m of this.labels) if (m.exclusive) this.labelState[m.key] = false;
      }
      this.labelState[key] = turningOn;
      this._renderChips();
      this._refreshPrimary();
    }

    _renderChips() {
      if (!this.labels) return;
      for (const b of this.chipsEl.querySelectorAll('.ce-chip'))
        b.classList.toggle('on', !!this.labelState[b.dataset.key]);
    }

    _initLabels(labels) {
      if (!this.labels) return;
      labels = labels || {};
      for (const m of this.labels) this.labelState[m.key] = !!labels[m.key];
      this._renderChips();
    }

    _refreshTrimInfo() {
      const dur = this.audio.duration || 0;
      const keep = Math.max(0, this.trimEnd - this.trimStart);
      this.trimKeepEl.textContent = `Keeping ${fmt(keep)} of ${fmt(dur)}`;
    }

    // Stop playback at the end handle so the preview never bleeds into the part
    // that's about to be cut. Runs every animation frame while playing.
    _enforceTrimBounds() {
      if (!this.trimMode || this.audio.paused) return;
      if (this.audio.currentTime >= this.trimEnd - 0.004) {
        this.audio.pause();
        try { this.audio.currentTime = this.trimEnd; } catch (e) {}
      }
    }

    _loop()     { this._stopLoop(); const step = () => { this._enforceTrimBounds(); this._drawWave(); this.raf = requestAnimationFrame(step); }; this.raf = requestAnimationFrame(step); }
    _stopLoop() { if (this.raf) { cancelAnimationFrame(this.raf); this.raf = null; } }

    _updateTime() {
      this.timeEl.textContent = `${fmt(this.audio.currentTime)} / ${fmt(this.audio.duration)}`;
    }

    // ── Waveform ──────────────────────────────────────────────────────────
    _computePeaks(audioBuffer, buckets = 320) {
      const data = audioBuffer.getChannelData(0);
      const block = Math.floor(data.length / buckets) || 1;
      const peaks = new Float32Array(buckets);
      let max = 0;
      for (let i = 0; i < buckets; i++) {
        let m = 0;
        const start = i * block;
        for (let j = 0; j < block; j++) {
          const v = Math.abs(data[start + j] || 0);
          if (v > m) m = v;
        }
        peaks[i] = m;
        if (m > max) max = m;
      }
      if (max > 0) for (let i = 0; i < buckets; i++) peaks[i] /= max;
      return peaks;
    }

    _drawWave() {
      const c = this.canvas, ctx = c.getContext('2d');
      const dpr = global.devicePixelRatio || 1;
      const w = c.clientWidth, h = c.clientHeight;
      if (!w || !h) return;
      c.width = w * dpr; c.height = h * dpr;
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      ctx.clearRect(0, 0, w, h);
      if (!this.peaks) return;

      const n = this.peaks.length;
      const slot = w / n;
      const bw = Math.max(1, slot - 1);
      const prog = this.audio.duration ? (this.audio.currentTime / this.audio.duration) : 0;

      for (let i = 0; i < n; i++) {
        const x = i * slot;
        const barH = Math.max(2, this.peaks[i] * h * 0.88);
        const y = (h - barH) / 2;
        ctx.fillStyle = (i / n) <= prog ? '#22c55e' : '#2e2e4e';
        ctx.fillRect(x, y, bw, barH);
      }

      // Trim overlay: dim the head/tail that will be cut, draw the two handles.
      if (this.trimMode && this.audio.duration && isFinite(this.audio.duration)) {
        const dur = this.audio.duration;
        const xs = (this.trimStart / dur) * w;
        const xe = (this.trimEnd / dur) * w;
        ctx.fillStyle = 'rgba(8,8,18,0.62)';
        ctx.fillRect(0, 0, xs, h);
        ctx.fillRect(xe, 0, w - xe, h);
        ctx.fillStyle = '#f59e0b';                 // gold = editing
        ctx.fillRect(xs - 1, 0, 2, h);
        ctx.fillRect(xe - 1, 0, 2, h);
        ctx.fillRect(xs - 4, (h - 18) / 2, 8, 18); // grips
        ctx.fillRect(xe - 4, (h - 18) / 2, 8, 18);
      }
    }

    // ── State / helpers ───────────────────────────────────────────────────
    _setStatus(status) {
      const s = status || 'pending';
      this.statusEl.className = 'ce-status badge ' + s;
      this.statusEl.textContent = s === 'verified' ? '✓ verified'
                               : s === 'rejected' ? '✕ rejected'
                               : s === 'issue'    ? '⚠ issue'
                               : 'pending';
    }

    _applyDir() {
      this.textEl.setAttribute('dir', RTL_RE.test(this.textEl.value) ? 'rtl' : 'ltr');
    }

    _busy(on) {
      this.rejectBtn.disabled = on;
      this.issueBtn.disabled = on;
      if (on) this.acceptBtn.disabled = true;
      else this._refreshPrimary();   // restore label/trim gating after an action
    }
  }

  function fmt(s) {
    if (!isFinite(s) || isNaN(s)) return '0:00';
    return Math.floor(s / 60) + ':' + String(Math.floor(s % 60)).padStart(2, '0');
  }

  function escHtml(s) {
    return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;')
                    .replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  }

  global.ChunkEditor = ChunkEditor;
})(window);

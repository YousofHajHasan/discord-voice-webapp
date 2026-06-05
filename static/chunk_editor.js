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

  const PLAY_ICON  = '<svg class="icon-play" viewBox="0 0 24 24"><path d="M8 5v14l11-7z"/></svg>';
  const PAUSE_ICON = '<svg class="icon-pause" viewBox="0 0 24 24"><path d="M6 19h4V5H6v14zm8-14v14h4V5h-4z"/></svg>';

  class ChunkEditor {
    constructor(mount, opts = {}) {
      this.mount     = mount;
      this.audioBase = opts.audioBase;
      this.onAccept  = opts.onAccept || (async () => {});
      this.onReject  = opts.onReject || (async () => {});
      this.onIssue   = opts.onIssue || null;   // optional 3rd action
      this.acceptLabel = opts.acceptLabel || 'Accept';
      this.rejectLabel = opts.rejectLabel || 'Delete';
      this.issueLabel  = opts.issueLabel  || 'Issue';

      this.audio   = new Audio();
      this.audioCtx = null;
      this.peaks   = null;
      this.raf     = null;
      this.objUrl  = null;
      this.chunk   = null;

      this._buildDom();
      this._wireAudio();
      this._wireKeys();
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
        </div>
        <div class="ce-text-label">Transcription</div>
        <textarea class="ce-text" placeholder="Type what is said in this clip…"></textarea>
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
      this.textEl   = this.el.querySelector('.ce-text');
      this.acceptBtn = this.el.querySelector('.ce-accept');
      this.rejectBtn = this.el.querySelector('.ce-reject');
      this.issueBtn  = this.el.querySelector('.ce-issue');

      this.acceptBtn.textContent = this.acceptLabel;
      this.rejectBtn.textContent = this.rejectLabel;
      this.issueBtn.textContent  = this.issueLabel;
      if (!this.onIssue) this.issueBtn.style.display = 'none';  // hide if not wired

      this.playBtn.addEventListener('click', () => this.togglePlay());
      this.canvas.addEventListener('click', (e) => this._seek(e));
      this.textEl.addEventListener('input', () => this._applyDir());

      this.acceptBtn.addEventListener('click', async () => {
        this._busy(true);
        try { await this.onAccept(this.textEl.value); }
        finally { this._busy(false); }
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
      this.acceptBtn.textContent = acceptLabel;
      this.rejectBtn.textContent = rejectLabel;
    }

    async load(chunk) {
      this.chunk = chunk;
      this.nameEl.textContent = chunk.filename;
      this.dateEl.textContent = chunk.date;
      this._setStatus(chunk.status);

      const text = chunk.verified_transcription != null && chunk.verified_transcription !== ''
        ? chunk.verified_transcription
        : (chunk.transcription || '');
      this.textEl.value = text;
      this._applyDir();

      // reset playback + waveform
      this._stop();
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

    _seek(e) {
      if (!this.audio.duration) return;
      const r = this.canvas.getBoundingClientRect();
      const ratio = Math.min(1, Math.max(0, (e.clientX - r.left) / r.width));
      this.audio.currentTime = ratio * this.audio.duration;
      this._drawWave();
    }

    _loop()     { this._stopLoop(); const step = () => { this._drawWave(); this.raf = requestAnimationFrame(step); }; this.raf = requestAnimationFrame(step); }
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
      this.acceptBtn.disabled = on;
      this.rejectBtn.disabled = on;
      this.issueBtn.disabled = on;
    }
  }

  function fmt(s) {
    if (!isFinite(s) || isNaN(s)) return '0:00';
    return Math.floor(s / 60) + ':' + String(Math.floor(s % 60)).padStart(2, '0');
  }

  global.ChunkEditor = ChunkEditor;
})(window);

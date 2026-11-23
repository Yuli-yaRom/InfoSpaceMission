// Простий SVG-графік ліній з перехрестям і підказкою (без зовнішніх бібліотек — UI працює офлайн).
// lineChart(container, { series: [{ name, color, points: [[x, y], ...], dash }], xFmt, yFmt, yMin })
// Кольори — CSS-змінні (--series-N), тож світла й темна теми перемикаються в одному місці.
(function () {
  const NS = 'http://www.w3.org/2000/svg';
  const svgEl = (tag, attrs) => {
    const e = document.createElementNS(NS, tag);
    for (const [k, v] of Object.entries(attrs || {})) e.setAttribute(k, v);
    return e;
  };

  function niceStep(range, ticks) {
    const raw = range / Math.max(1, ticks);
    const mag = Math.pow(10, Math.floor(Math.log10(raw || 1)));
    const n = raw / mag;
    return (n >= 5 ? 10 : n >= 2 ? 5 : n >= 1 ? 2 : 1) * mag;
  }

  function lineChart(container, opts) {
    const series = (opts.series || []).filter(s => s.points && s.points.length);
    const xFmt = opts.xFmt || (v => String(v));
    const yFmt = opts.yFmt || (v => v.toLocaleString('uk'));
    container.replaceChildren();
    container.classList.add('chart');
    if (!series.length) {
      const p = document.createElement('div'); p.className = 'chart-empty'; p.textContent = opts.empty || 'Ще немає даних';
      container.appendChild(p); return;
    }

    const W = container.clientWidth || 600, H = opts.height || 220;
    const m = { l: 64, r: 14, t: 10, b: 26 };
    let xMin = Infinity, xMax = -Infinity, yMax = -Infinity, yMin = opts.yMin ?? 0;
    for (const s of series) for (const [x, y] of s.points) {
      if (x < xMin) xMin = x; if (x > xMax) xMax = x;
      if (y != null && y > yMax) yMax = y;
      if (opts.yMin === undefined && y != null && y < yMin) yMin = y;
    }
    if (xMax === xMin) xMax = xMin + 1;
    if (!(yMax > yMin)) yMax = yMin + 1;
    const yStep = niceStep(yMax - yMin, 4);
    yMax = Math.ceil(yMax / yStep) * yStep;
    const X = x => m.l + (x - xMin) / (xMax - xMin) * (W - m.l - m.r);
    const Y = y => H - m.b - (y - yMin) / (yMax - yMin) * (H - m.t - m.b);

    const svg = svgEl('svg', { width: W, height: H, viewBox: `0 0 ${W} ${H}`, role: 'img',
                               'aria-label': opts.label || 'графік' });
    // Сітка й осі — приглушені, щоб не конкурувати з даними
    for (let v = yMin; v <= yMax + 1e-9; v += yStep) {
      svg.appendChild(svgEl('line', { x1: m.l, x2: W - m.r, y1: Y(v), y2: Y(v), class: 'grid' }));
      const t = svgEl('text', { x: m.l - 6, y: Y(v) + 4, 'text-anchor': 'end', class: 'tick' }); t.textContent = yFmt(v);
      svg.appendChild(t);
    }
    const xStep = niceStep(xMax - xMin, Math.max(2, Math.floor((W - m.l) / 90)));
    for (let v = Math.ceil(xMin / xStep) * xStep; v <= xMax; v += xStep) {
      const t = svgEl('text', { x: X(v), y: H - 8, 'text-anchor': 'middle', class: 'tick' }); t.textContent = xFmt(v);
      svg.appendChild(t);
    }
    svg.appendChild(svgEl('line', { x1: m.l, x2: W - m.r, y1: Y(yMin), y2: Y(yMin), class: 'axis' }));

    for (const s of series) {
      let d = '', pen = false;
      for (const [x, y] of s.points) {
        if (y == null) { pen = false; continue; }
        d += (pen ? 'L' : 'M') + X(x).toFixed(1) + ',' + Y(y).toFixed(1); pen = true;
      }
      const path = svgEl('path', { d, class: 'line', style: `stroke:${s.color}` });
      if (s.dash) path.setAttribute('stroke-dasharray', '5 4');
      svg.appendChild(path);
    }

    // Перехрестя + підказка: вертикальна лінія прив'язується до найближчого X
    const hair = svgEl('line', { y1: m.t, y2: H - m.b, class: 'hair', visibility: 'hidden' });
    svg.appendChild(hair);
    const dots = series.map(s => { const c = svgEl('circle', { r: 4, class: 'dot', style: `fill:${s.color}`, visibility: 'hidden' }); svg.appendChild(c); return c; });
    const tip = document.createElement('div'); tip.className = 'chart-tip'; tip.hidden = true;
    const nearest = (pts, x) => {
      let lo = 0, hi = pts.length - 1;
      while (hi - lo > 1) { const mid = (lo + hi) >> 1; if (pts[mid][0] < x) lo = mid; else hi = mid; }
      return Math.abs(pts[lo][0] - x) <= Math.abs(pts[hi][0] - x) ? pts[lo] : pts[hi];
    };
    svg.addEventListener('pointermove', (ev) => {
      const r = svg.getBoundingClientRect();
      const px = ev.clientX - r.left;
      if (px < m.l || px > W - m.r) return;
      const x = xMin + (px - m.l) / (W - m.l - m.r) * (xMax - xMin);
      const ref = nearest(series[0].points, x);
      hair.setAttribute('x1', X(ref[0])); hair.setAttribute('x2', X(ref[0])); hair.setAttribute('visibility', 'visible');
      tip.replaceChildren();
      const head = document.createElement('div'); head.className = 'tip-x'; head.textContent = xFmt(ref[0]); tip.appendChild(head);
      series.forEach((s, i) => {
        const p = nearest(s.points, ref[0]);
        if (p[1] == null) { dots[i].setAttribute('visibility', 'hidden'); return; }
        dots[i].setAttribute('cx', X(p[0])); dots[i].setAttribute('cy', Y(p[1])); dots[i].setAttribute('visibility', 'visible');
        const row = document.createElement('div'); row.className = 'tip-row';
        const sw = document.createElement('span'); sw.className = 'sw'; sw.style.background = s.color;
        const v = document.createElement('b'); v.textContent = yFmt(p[1]);
        const n = document.createElement('span'); n.textContent = ' ' + s.name;
        row.append(sw, v, n); tip.appendChild(row);
      });
      tip.hidden = false;
      const left = Math.min(X(ref[0]) + 12, W - tip.offsetWidth - 4);
      tip.style.left = Math.max(0, left) + 'px'; tip.style.top = m.t + 'px';
    });
    svg.addEventListener('pointerleave', () => {
      hair.setAttribute('visibility', 'hidden'); dots.forEach(d => d.setAttribute('visibility', 'hidden')); tip.hidden = true;
    });

    container.append(svg, tip);
    // Легенда (для 2+ серій): ідентичність не лише кольором — підпис поруч зі зразком лінії
    if (series.length > 1) {
      const lg = document.createElement('div'); lg.className = 'legend';
      for (const s of series) {
        const it = document.createElement('span');
        const sw = document.createElement('span'); sw.className = 'sw' + (s.dash ? ' dash' : ''); sw.style.background = s.dash ? 'none' : s.color;
        if (s.dash) sw.style.borderTopColor = s.color;
        it.append(sw, document.createTextNode(s.name)); lg.appendChild(it);
      }
      container.appendChild(lg);
    }
  }

  const fmtDuration = (s) => {
    s = Math.round(s);
    const h = Math.floor(s / 3600), mi = Math.floor((s % 3600) / 60), se = s % 60;
    return h ? `${h}:${String(mi).padStart(2, '0')}:${String(se).padStart(2, '0')}` : `${mi}:${String(se).padStart(2, '0')}`;
  };
  const fmtBytes = (b) => {
    if (b == null) return '–';
    const u = ['Б', 'КБ', 'МБ', 'ГБ', 'ТБ']; let i = 0;
    while (b >= 1024 && i < u.length - 1) { b /= 1024; i++; }
    return (i ? b.toFixed(b < 10 ? 2 : 1) : b) + ' ' + u[i];
  };
  const cssVar = (name) => getComputedStyle(document.documentElement).getPropertyValue(name).trim();

  window.Charts = { lineChart, fmtDuration, fmtBytes, cssVar };
})();

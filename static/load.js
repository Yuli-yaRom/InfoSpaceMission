// Вкладка «Навантаження»: генератор, графіки швидкості запису, заміри, видалення даних.
(function () {
  const $ = (id) => document.getElementById(id);
  const fmt = (n) => (n == null ? '–' : Math.round(n).toLocaleString('uk'));
  const el = (tag, attrs = {}, text) => {
    const e = document.createElement(tag);
    for (const [k, v] of Object.entries(attrs)) e.setAttribute(k, v);
    if (text !== undefined) e.textContent = text;
    return e;
  };
  async function api(url, opts = {}) {
    const res = await fetch(url, opts);
    let body = null;
    try { body = await res.json(); } catch (_) {}
    if (!res.ok) {
      const d = body && body.detail;
      throw new Error(Array.isArray(d) ? d.map(x => x.loc.slice(1).join('.') + ': ' + x.msg).join('; ') : (d || 'Помилка ' + res.status));
    }
    return body;
  }
  const json = (method, body) => ({ method, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
  const tabActive = () => $('tab-load').classList.contains('active');

  // ---------------- Генератор ----------------
  let formDirty = false;
  function fillForm(s) {
    const f = $('gen-form');
    f.mode.value = s.max_speed ? 'max' : (s.docs_per_second === 0 ? 'pause' : 'rate');
    if (s.docs_per_second) f.docs_per_second.value = s.docs_per_second;
    for (const k of ['batch_size', 'writers', 'updates_per_second', 'deletes_per_second', 'bulk_update_every_s']) f[k].value = s[k];
    f.index_search.checked = s.index_search;
  }
  const STATE_UA = { running: 'пише', paused: 'пауза', retrying: 'повторює запис (БД недоступна)' };

  function renderStatus(st) {
    const kv = $('gen-kv'); kv.replaceChildren();
    const add = (v, label) => { const d = el('div'); d.append(el('b', {}, v), el('span', {}, label)); kv.appendChild(d); };
    const stateBox = el('div'); stateBox.append(el('span', { class: 'state ' + st.state }, STATE_UA[st.state] || st.state), el('span', { style: 'display:block' }, 'стан'));
    kv.appendChild(stateBox);
    add(fmt(st.generated), 'згенеровано');
    add(fmt(st.inserted), 'записано в БД');
    add(`${fmt(st.pending)} / ${fmt(st.buffer_limit)}`, 'у буфері / ліміт');
    add(fmt(st.failed_attempts), 'невдалих спроб (повторено)');
    add(`${fmt(st.updated)} / ${fmt(st.deleted)} / ${fmt(st.bulk_updates)}`, 'UPDATE / DELETE / масових');
    if (st.es_failures) add(fmt(st.es_failures), 'помилок Elasticsearch');
    $('gen-error').textContent = st.last_error ? `Остання помилка (${new Date(st.last_error_at).toLocaleTimeString('uk')}): ${st.last_error}` : '';
  }

  async function loadGenerator() {
    try {
      const st = await api('/api/generator');
      if (!formDirty) fillForm(st.settings);
      renderStatus(st);
    } catch (_) {}
  }
  $('gen-form').addEventListener('input', () => { formDirty = true; });
  $('gen-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    const f = e.target, mode = f.mode.value;
    const patch = {
      max_speed: mode === 'max',
      docs_per_second: mode === 'pause' ? 0 : Number(f.docs_per_second.value || 0),
      batch_size: Number(f.batch_size.value), writers: Number(f.writers.value),
      index_search: f.index_search.checked,
      updates_per_second: Number(f.updates_per_second.value || 0),
      deletes_per_second: Number(f.deletes_per_second.value || 0),
      bulk_update_every_s: Number(f.bulk_update_every_s.value || 0),
    };
    if (mode === 'rate' && !patch.docs_per_second) { $('gen-error').textContent = 'Вкажіть швидкість (рядків/с)'; return; }
    try { renderStatus(await api('/api/generator', json('PUT', patch))); formDirty = false; }
    catch (err) { $('gen-error').textContent = err.message; }
  });

  // ---------------- Графіки ----------------
  let samples = [], metricsBusy = false;
  async function loadMetrics() {
    if (metricsBusy) return;   // вкладка й таймер можуть запросити одночасно — без дублікатів точок
    metricsBusy = true;
    try {
      const since = samples.length ? samples[samples.length - 1].t : undefined;
      const fresh = await api('/api/metrics' + (since !== undefined ? '?since=' + since : ''));
      const last = samples.length ? samples[samples.length - 1].t : -Infinity;
      samples = samples.concat(fresh.filter(s => s.t > last));
      if (samples.length > 40000) samples = samples.slice(-30000);
    } catch (_) {
    } finally { metricsBusy = false; }
    drawMetrics();
  }
  let clockSeconds = false;
  const clock = (t) => {
    const s = samples.length ? samples[samples.length - 1] : null;
    if (!s) return '';
    const d = new Date(new Date(s.at).getTime() - (s.t - t) * 1000);
    return d.toLocaleTimeString('uk', clockSeconds ? { hour: '2-digit', minute: '2-digit', second: '2-digit' }
                                                    : { hour: '2-digit', minute: '2-digit' });
  };
  const num = (v) => (Math.abs(v) < 10 && v % 1 ? v.toFixed(1) : Math.round(v)).toLocaleString('uk');
  function drawMetrics() {
    const win = Number($('metrics-window').value);
    const last = samples.length ? samples[samples.length - 1].t : 0;
    const pts = win ? samples.filter(s => s.t >= last - win) : samples;
    clockSeconds = pts.length > 1 && pts[pts.length - 1].t - pts[0].t < 600;
    const xy = (key) => pts.map(s => [s.t, s[key]]);
    const series = [{ name: 'INSERT, рядків/с', color: 'var(--series-1)', points: xy('insert_rate') }];
    if (pts.some(s => s.update_rate > 0)) series.push({ name: 'UPDATE, рядків/с', color: 'var(--series-2)', points: xy('update_rate') });
    if (pts.some(s => s.delete_rate > 0)) series.push({ name: 'DELETE, рядків/с', color: 'var(--series-3)', points: xy('delete_rate') });
    if (pts.some(s => s.target_rate != null && s.target_rate > 0))
      series.push({ name: 'задана швидкість', color: 'var(--muted)', dash: true, points: pts.map(s => [s.t, s.target_rate]) });
    Charts.lineChart($('chart-rate'), { series, xFmt: clock, yFmt: num, label: 'Швидкість запису', height: 240,
                                        empty: 'Ще немає вимірювань — точка з\'являється кожні 2 с' });

    const lat = [{ name: 'запис у БД', color: 'var(--series-1)', points: xy('sql_batch_ms') }];
    if (pts.some(s => s.es_batch_ms != null)) lat.push({ name: 'індексація в Elasticsearch', color: 'var(--series-2)', points: xy('es_batch_ms') });
    Charts.lineChart($('chart-latency'), { series: lat, xFmt: clock, yFmt: num, height: 180 });

    const sizePts = pts.filter(s => s.db_size_bytes != null).map(s => [s.t, s.db_size_bytes / 1024 ** 3]);
    Charts.lineChart($('chart-size'), { series: [{ name: 'ГБ', color: 'var(--series-1)', points: sizePts }], xFmt: clock,
                                        yFmt: v => v.toFixed(v < 1 ? 2 : 1) + ' ГБ', height: 180,
                                        empty: 'Розмір доступний лише з PostgreSQL / Citus' });
  }
  $('metrics-window').addEventListener('change', drawMetrics);
  window.addEventListener('resize', () => { if (tabActive()) { drawMetrics(); drawBenchChart(); } });

  $('recount').addEventListener('click', async () => {
    const out = $('recount-out'); out.textContent = 'Рахую count(*) (генератор на цей час призупиняє запис)…';
    try {
      const s = await api('/api/stats/recount', { method: 'POST' });
      out.textContent = `Точно: ${fmt(s.total_documents)} рядків (${new Date(s.counted_at).toLocaleTimeString('uk')}).`;
    } catch (err) { out.textContent = err.message; }
  });

  // Під час заміру налаштування генератора заблоковані (сервер теж відхиляє зміни — 409):
  // інакше в результаті було б записано не ті налаштування, з якими насправді вимірювали
  function lockGenerator(locked) {
    for (const e of $('gen-form').elements) e.disabled = locked;
    $('gen-lock').hidden = !locked;
  }

  // ---------------- Заміри ----------------
  let runs = [], shown = new Set(), current = null;
  const pct = (v) => (v == null ? '–' : (v > 0 ? '+' : '') + v.toFixed(1) + '%');

  function benchRow(r, colorIdx) {
    const tr = el('tr');
    const cb = el('input', { type: 'checkbox' }); cb.checked = shown.has(r.id);
    cb.addEventListener('change', () => { cb.checked ? shown.add(r.id) : shown.delete(r.id); drawBenchChart(); });
    const swatch = el('span', { class: 'sw' }); swatch.style.background = `var(--series-${colorIdx})`;
    const first = el('td'); first.append(cb, ' ', swatch);
    const del = el('button', { class: 'btn secondary', title: 'Видалити результат' }, '×');
    del.addEventListener('click', async () => { await fetch('/api/benchmarks/' + r.id, { method: 'DELETE' }); shown.delete(r.id); loadBench(); });
    const tdDel = el('td'); tdDel.appendChild(del);
    tr.append(first,
      el('td', {}, new Date(r.started_at).toLocaleString('uk', { dateStyle: 'short', timeStyle: 'short' })),
      el('td', {}, r.label + (r.status === 'stopped' ? ' (зупинено)' : '')),
      el('td', {}, `${r.topology} · ${r.nodes} вузл.`),
      el('td', { class: 'num' }, Charts.fmtDuration(r.duration_s)),
      el('td', { class: 'num' }, fmt(r.inserted)),
      el('td', { class: 'num' }, fmt(r.avg_rate)),
      el('td', { class: 'num', title: `${r.window_s} с після розгону (перші ${r.warmup_s} с пропущено)` }, fmt(r.first_min_rate)),
      el('td', { class: 'num', title: `останні ${r.window_s} с` }, fmt(r.last_min_rate)),
      el('td', { class: 'num' }, pct(r.change_pct)),
      el('td', { class: 'num' }, r.p95_batch_ms == null ? '–' : fmt(r.p95_batch_ms) + ' мс'),
      el('td', { class: 'num' }, Charts.fmtBytes(r.size_end)),
      tdDel);
    return tr;
  }

  // Колір іде за прогоном (за порядком появи), а не за позицією в таблиці — фільтр не перефарбовує решту
  const colorOf = (id) => {
    const ordered = [...runs].sort((a, b) => a.started_at.localeCompare(b.started_at));
    return (ordered.findIndex(r => r.id === id) % 8) + 1;
  };

  function drawBenchChart() {
    const sel = runs.filter(r => shown.has(r.id)).slice(0, 8);
    Charts.lineChart($('chart-bench'), {
      series: sel.map(r => ({ name: `${r.label} · ${r.topology}`, color: `var(--series-${colorOf(r.id)})`, points: r.series })),
      xFmt: (s) => Charts.fmtDuration(s), height: 240, label: 'Порівняння замірів',
      empty: 'Позначте прогони в таблиці, щоб порівняти їх на графіку',
    });
  }

  async function loadBench() {
    try {
      runs = await api('/api/benchmarks');
      if (!shown.size) runs.slice(0, 3).forEach(r => shown.add(r.id));
      const tb = $('bench-rows'); tb.replaceChildren();
      if (!runs.length) { const tr = el('tr'); tr.appendChild(el('td', { colspan: 13, class: 'placeholder' }, 'Ще немає завершених замірів')); tb.appendChild(tr); }
      for (const r of runs) tb.appendChild(benchRow(r, colorOf(r.id)));
      drawBenchChart();
    } catch (_) {}
  }

  async function loadCurrent() {
    try {
      const prev = current;
      current = await api('/api/benchmarks/current');
      const box = $('bench-current');
      const running = current && current.status === 'running';
      $('bench-stop').hidden = !running; $('bench-start').disabled = running;
      lockGenerator(running);
      if (running) {
        const left = Math.max(0, current.planned_s - current.duration_s);
        box.textContent = `Триває замір «${current.label}» (${current.topology}): ${Charts.fmtDuration(current.duration_s)} з ${Charts.fmtDuration(current.planned_s)}, ` +
          `лишилось ${Charts.fmtDuration(left)} · записано ${fmt(current.inserted)} · у середньому ${fmt(current.avg_rate)} р/с`;
      } else {
        box.textContent = '';
        if (prev && prev.status === 'running') loadBench();   // щойно завершився
      }
    } catch (_) {}
  }

  $('bench-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    const f = e.target;
    try {
      await api('/api/benchmarks', json('POST', {
        label: f.label.value, duration_s: Math.round(Number(f.minutes.value) * 60),
        max_speed: f.mode.value === 'max', docs_per_second: Number(f.docs_per_second.value || 1),
      }));
      formDirty = false; loadCurrent(); loadGenerator();
    } catch (err) { $('bench-current').textContent = err.message; }
  });
  $('bench-stop').addEventListener('click', async () => { await api('/api/benchmarks/stop', { method: 'POST' }); loadCurrent(); loadBench(); loadGenerator(); });

  // ---------------- Видалення ----------------
  const delParams = () => {
    const p = new URLSearchParams();
    for (const [k, v] of new FormData($('del-form'))) if (v !== '') p.set(k, v);
    return p;
  };
  let previewFor = null;
  $('del-form').addEventListener('input', () => { $('del-go').disabled = true; previewFor = null; $('del-go').textContent = 'Видалити'; });
  $('del-preview').addEventListener('click', async () => {
    const p = delParams(), out = $('del-out');
    if (![...p.keys()].length) { out.textContent = 'Вкажіть хоча б один фільтр (для повного видалення — кнопка нижче).'; return; }
    out.textContent = 'Рахую…';
    try {
      const { rows } = await api('/api/data/preview?' + p);
      previewFor = p.toString();
      out.textContent = `Під фільтр потрапляє ${fmt(rows)} рядків.`;
      $('del-go').disabled = rows === 0; $('del-go').textContent = `Видалити ${fmt(rows)} рядків`;
    } catch (err) { out.textContent = err.message; }
  });
  const showDelete = (r) => {
    const parts = [`Видалено ${fmt(r.deleted_rows)} рядків за ${(r.took_ms / 1000).toFixed(2)} с`];
    if (r.size_before != null) parts.push(`розмір: ${Charts.fmtBytes(r.size_before)} → ${Charts.fmtBytes(r.size_after)}`);
    if (r.caches_cleared) parts.push(`скинуто ключів кешу: ${r.caches_cleared}`);
    if (r.search_note) parts.push(r.search_note);
    $('del-out').textContent = parts.join(' · ');
  };
  $('del-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    if (!previewFor) return;
    $('del-go').disabled = true; $('del-out').textContent = 'Видаляю…';
    try { showDelete(await api('/api/data?' + previewFor, { method: 'DELETE' })); }
    catch (err) { $('del-out').textContent = err.message; }
    previewFor = null; $('del-go').textContent = 'Видалити';
  });
  $('del-all-confirm').addEventListener('input', (e) => { $('del-all').disabled = e.target.value.trim() !== 'ВИДАЛИТИ'; });
  $('del-all').addEventListener('click', async () => {
    $('del-all').disabled = true; $('del-out').textContent = 'Видаляю все (TRUNCATE)…';
    try { showDelete(await api('/api/data/all?confirm=yes', { method: 'DELETE' })); }
    catch (err) { $('del-out').textContent = err.message; }
    $('del-all-confirm').value = '';
  });
  for (const [id, full] of [['vacuum', false], ['vacuum-full', true]]) {
    $(id).addEventListener('click', async () => {
      $('del-out').textContent = full ? 'VACUUM FULL: таблиця заблокована на час виконання…' : 'VACUUM…';
      try {
        const r = await api('/api/data/vacuum?full=' + full, { method: 'POST' });
        $('del-out').textContent = `${full ? 'VACUUM FULL' : 'VACUUM'} за ${(r.took_ms / 1000).toFixed(1)} с · розмір: ${Charts.fmtBytes(r.size_before)} → ${Charts.fmtBytes(r.size_after)}`;
      } catch (err) { $('del-out').textContent = err.message; }
    });
  }

  // ---------------- Оновлення ----------------
  document.querySelector('#tabs button[data-tab="load"]').addEventListener('click', () => {
    loadGenerator(); loadBench(); loadCurrent(); setTimeout(() => { loadMetrics(); drawBenchChart(); });
  });
  setInterval(() => { if (tabActive()) { loadGenerator(); loadMetrics(); loadCurrent(); } }, 2000);
})();

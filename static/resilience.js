// Вкладка «Відмовостійкість»: стан вузлів, буфер генератора, знімок і перевірка, журнал подій.
(function () {
  const $ = (id) => document.getElementById(id);
  const fmt = (n) => (n == null ? '–' : Math.round(n).toLocaleString('uk'));
  const el = (tag, attrs = {}, text) => {
    const e = document.createElement(tag);
    for (const [k, v] of Object.entries(attrs)) e.setAttribute(k, v);
    if (text !== undefined) e.textContent = text;
    return e;
  };
  const time = (iso) => (iso ? new Date(iso).toLocaleTimeString('uk') : '–');
  const tabActive = () => $('tab-resilience').classList.contains('active');

  async function loadNodes() {
    try {
      const nodes = await (await fetch('/api/cluster/health')).json();
      const tb = $('nodes-rows'); tb.replaceChildren();
      if (!nodes.length) { const tr = el('tr'); tr.appendChild(el('td', { colspan: 7, class: 'placeholder' }, 'Без Citus стан вузлів недоступний')); tb.appendChild(tr); }
      for (const n of nodes) {
        const tr = el('tr');
        const st = el('td'); st.appendChild(el('span', { class: 'state ' + (n.up ? 'up' : 'down') }, n.up ? 'працює' : 'недоступний'));
        tr.append(el('td', {}, n.node), el('td', {}, n.role === 'coordinator' ? 'координатор' : 'воркер'), st,
          el('td', { class: 'num' }, n.latency_ms == null ? '–' : n.latency_ms + ' мс'),
          el('td', { class: 'num' }, n.shards), el('td', {}, time(n.since)),
          el('td', { style: 'white-space:normal;color:var(--muted);font-size:12px' }, n.error || ''));
        tb.appendChild(tr);
      }
    } catch (_) {}
  }

  async function loadGen() {
    try {
      const st = await (await fetch('/api/generator')).json();
      const kv = $('res-gen'); kv.replaceChildren();
      const add = (v, label) => { const d = el('div'); d.append(el('b', {}, v), el('span', {}, label)); kv.appendChild(d); };
      const box = el('div');
      box.append(el('span', { class: 'state ' + st.state }, { running: 'пише', paused: 'пауза', retrying: 'повторює запис' }[st.state]),
                 el('span', { style: 'display:block' }, st.retrying_since ? 'з ' + time(st.retrying_since) : 'генератор'));
      kv.appendChild(box);
      add(fmt(st.generated), 'згенеровано');
      add(fmt(st.inserted), 'записано в БД');
      add(fmt(st.pending), 'у буфері (чекає запису)');
      add(fmt(st.failed_attempts), 'невдалих спроб');
    } catch (_) {}
  }

  async function loadEvents() {
    try {
      const evs = await (await fetch('/api/cluster/events?limit=50')).json();
      const ul = $('events'); ul.replaceChildren();
      if (!evs.length) ul.appendChild(el('li', { style: 'display:block;color:var(--muted)' }, 'Подій ще немає'));
      for (const e of evs) {
        const li = el('li');
        li.append(el('time', {}, time(e.at)), el('span', { class: e.level }, e.message));
        ul.appendChild(li);
      }
    } catch (_) {}
  }

  function perNodeTable(before, after) {
    const nodes = [...new Set([...Object.keys(before || {}), ...Object.keys(after || {})])].sort();
    if (!nodes.length) return null;
    const t = el('table', { class: 'static' }), head = el('tr');
    ['Вузол', 'Рядків у знімку', 'Рядків зараз'].forEach((h, i) => head.appendChild(el('th', i ? { class: 'num' } : {}, h)));
    const th = el('thead'); th.appendChild(head);
    const tb = el('tbody');
    for (const n of nodes) {
      const tr = el('tr');
      tr.append(el('td', {}, n), el('td', { class: 'num' }, fmt(before?.[n])), el('td', { class: 'num' }, fmt(after?.[n])));
      tb.appendChild(tr);
    }
    t.append(th, tb);
    const w = el('div', { class: 'table-wrap', style: 'margin-top:8px' }); w.appendChild(t);
    return w;
  }

  async function post(url) {
    const res = await fetch(url, { method: 'POST' });
    const body = await res.json();
    if (!res.ok) throw new Error(body.detail || res.status);
    return body;
  }

  $('snap').addEventListener('click', async () => {
    const out = $('res-out'); out.textContent = 'Фіксую стан: count(*) (генератор на мить призупиняє запис)…';
    try {
      const s = await post('/api/resilience/snapshot');
      out.replaceChildren(el('div', {}, `Знімок ${time(s.at)}: ${fmt(s.rows)} рядків у БД. Тепер вимкніть вузол (крок 2).`));
      const t = perNodeTable(s.per_node, null); if (t) out.appendChild(t);
    } catch (err) { out.textContent = err.message; }
    loadEvents();
  });

  $('check').addEventListener('click', async () => {
    const out = $('res-out'); out.textContent = 'Перевіряю: count(*)…';
    try {
      const c = await post('/api/resilience/check');
      out.replaceChildren();
      out.appendChild(el('div', { class: 'big-check ' + (c.ok ? 'ok' : 'bad') },
        c.ok ? '✓ Жоден рядок не загубився' : `✗ Розбіжність: ${fmt(c.lost_rows)} рядків`));
      const kv = el('div', { class: 'kv' });
      const add = (v, label) => { const d = el('div'); d.append(el('b', {}, v), el('span', {}, label)); kv.appendChild(d); };
      add(fmt(c.snapshot.rows), 'рядків у знімку');
      add('+' + fmt(c.inserted_since), 'записано після знімка');
      add('−' + fmt(c.deleted_since), 'видалено після знімка');
      add(fmt(c.expected_rows), 'очікувано');
      add(fmt(c.actual_rows), 'фактично (count)');
      add(fmt(c.lost_rows), 'втрачено');
      add(fmt(c.generated_since), 'згенеровано після знімка');
      add(fmt(c.pending), 'ще в буфері');
      out.appendChild(kv);
      if (c.pending) out.appendChild(el('p', { class: 'hint' }, 'У буфері ще є рядки — вони вже не загублені, але ще не записані. Повторіть перевірку, коли буфер спорожніє.'));
      const t = perNodeTable(c.snapshot.per_node, c.per_node_after); if (t) out.appendChild(t);
    } catch (err) { out.textContent = err.message; }
    loadEvents();
  });

  const refresh = () => { loadNodes(); loadGen(); loadEvents(); };
  document.querySelector('#tabs button[data-tab="resilience"]').addEventListener('click', refresh);
  setInterval(() => { if (tabActive()) refresh(); }, 2000);
})();

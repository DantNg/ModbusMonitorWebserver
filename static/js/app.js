window.App = {
  changeGroup(group) {
    const u = new URL(window.location.href);
    if (group && group !== '__all__') u.searchParams.set('group', group);
    else u.searchParams.delete('group');
    window.location.href = u.toString();
  },

  changeDevice(device) {
    const u = new URL(window.location.href);
    if (device && device !== '__all__') u.searchParams.set('device', device);
    else u.searchParams.delete('device');
    window.location.href = u.toString();
  },

  filterTable(selector, q) {
    q = (q || '').toLowerCase();
    document.querySelectorAll(selector + ' tbody tr').forEach(tr => {
      tr.style.display = tr.innerText.toLowerCase().includes(q) ? '' : 'none';
    });
  },

  startDashboard(apiUrl) {
    async function refresh() {
      try {
        const res = await fetch(apiUrl || '/api/tags');
        const data = await res.json();
        const tags = data.tags || data; // Handle both {tags: [...]} and [...] formats
        for (const row of tags) {
          const v = Number(row.value);
          const bar = document.getElementById('tag-bar-' + row.id);
          const val = document.getElementById('tag-val-' + row.id);
          const ts  = document.getElementById('tag-ts-' + row.id);
          
          // For initial load, don't show activity bars
          if (bar) {
            bar.style.width = "0%";
            bar.style.backgroundColor = "";
          }
          if (val) val.textContent = isFinite(v) ? v.toFixed(2) : row.value;
          if (ts) ts.textContent = new Date(row.ts || Date.now()).toLocaleTimeString();
        }
      } catch (e) { /* bỏ qua lỗi nhỏ */ }
    }
    refresh();
    setInterval(refresh, 1000);  // Update every 1 second instead of 3
  }

  
};
// Theme toggle
(function(){
  const KEY='theme';
  const btn = document.getElementById('themeToggle');
  if (!btn) return;
  const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
  const saved = localStorage.getItem(KEY);
  const initTheme = saved || (prefersDark ? 'dark' : 'light');
  document.documentElement.setAttribute('data-bs-theme', initTheme);
  const setIcon = () => btn.textContent =
      (document.documentElement.getAttribute('data-bs-theme') === 'dark') ? '🌙' : '🌞';
  setIcon();
  btn.addEventListener('click', () => {
    const cur = document.documentElement.getAttribute('data-bs-theme');
    const next = (cur === 'dark') ? 'light' : 'dark';
    document.documentElement.setAttribute('data-bs-theme', next);
    localStorage.setItem(KEY, next);
    setIcon();
  });
})();

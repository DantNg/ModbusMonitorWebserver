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
          
          // Don't modify bar style - it's controlled by device status from server template
          if (val) val.textContent = isFinite(v) ? v.toFixed(2) : row.value;
          if (ts) ts.textContent = new Date(row.ts || Date.now()).toLocaleTimeString();
        }
      } catch (e) { /* bỏ qua lỗi nhỏ */ }
    }
    refresh();
    setInterval(refresh, 1000);  // Update every 1 second instead of 3
  }

  
};

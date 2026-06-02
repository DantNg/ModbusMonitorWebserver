// Vietnamese diacritics map for accent-insensitive search
const _vnMap={'à':'a','á':'a','ả':'a','ã':'a','ạ':'a','ă':'a','ằ':'a','ắ':'a','ẳ':'a','ẵ':'a','ặ':'a','â':'a','ầ':'a','ấ':'a','ẩ':'a','ẫ':'a','ậ':'a','è':'e','é':'e','ẻ':'e','ẽ':'e','ẹ':'e','ê':'e','ề':'e','ế':'e','ể':'e','ễ':'e','ệ':'e','ì':'i','í':'i','ỉ':'i','ĩ':'i','ị':'i','ò':'o','ó':'o','ỏ':'o','õ':'o','ọ':'o','ô':'o','ồ':'o','ố':'o','ổ':'o','ỗ':'o','ộ':'o','ơ':'o','ờ':'o','ớ':'o','ở':'o','ỡ':'o','ợ':'o','ù':'u','ú':'u','ủ':'u','ũ':'u','ụ':'u','ư':'u','ừ':'u','ứ':'u','ử':'u','ữ':'u','ự':'u','ỳ':'y','ý':'y','ỷ':'y','ỹ':'y','ỵ':'y','đ':'d','À':'A','Á':'A','Ả':'A','Ã':'A','Ạ':'A','Ă':'A','Ằ':'A','Ắ':'A','Ẳ':'A','Ẵ':'A','Ặ':'A','Â':'A','Ầ':'A','Ấ':'A','Ẩ':'A','Ẫ':'A','Ậ':'A','È':'E','É':'E','Ẻ':'E','Ẽ':'E','Ẹ':'E','Ê':'E','Ề':'E','Ế':'E','Ể':'E','Ễ':'E','Ệ':'E','Ì':'I','Í':'I','Ỉ':'I','Ĩ':'I','Ị':'I','Ò':'O','Ó':'O','Ỏ':'O','Õ':'O','Ọ':'O','Ô':'O','Ồ':'O','Ố':'O','Ổ':'O','Ỗ':'O','Ộ':'O','Ơ':'O','Ờ':'O','Ớ':'O','Ở':'O','Ỡ':'O','Ợ':'O','Ù':'U','Ú':'U','Ủ':'U','Ũ':'U','Ụ':'U','Ư':'U','Ừ':'U','Ứ':'U','Ử':'U','Ữ':'U','Ự':'U','Ỳ':'Y','Ý':'Y','Ỷ':'Y','Ỹ':'Y','Ỵ':'Y','Đ':'D'};
function _removeDiacritics(s){return s.replace(/./g,ch=>_vnMap[ch]||ch);}

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
    const qAscii = _removeDiacritics(q);
    document.querySelectorAll(selector + ' tbody tr').forEach(tr => {
      const text = tr.innerText.toLowerCase();
      tr.style.display = (text.includes(q) || _removeDiacritics(text).includes(qAscii)) ? '' : 'none';
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
          // Dùng display_value từ backend (đã format bởi shared/formatting); fallback về raw value
          if (val) val.textContent = row.display_value !== undefined ? row.display_value : (isFinite(v) ? String(v) : (row.value ?? '--'));
          if (ts) ts.textContent = new Date(row.ts || Date.now()).toLocaleTimeString();
        }
      } catch (e) { /* bỏ qua lỗi nhỏ */ }
    }
    refresh();
    setInterval(refresh, 1000);  // Update every 1 second instead of 3
  }

  
};

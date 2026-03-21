/* ==========================================================================
   Alarm Events Page - Main JavaScript
   Extracted from alarms/alarm_events.html inline <script>
   ========================================================================== */

// Read configuration passed from Jinja2 template
const ALARM_EVENTS_CONFIG = window.ALARM_EVENTS_CONFIG || {};

  // Vietnamese diacritics map for accent-insensitive search
  const _vnAlarmMap={'à':'a','á':'a','ả':'a','ã':'a','ạ':'a','ă':'a','ằ':'a','ắ':'a','ẳ':'a','ẵ':'a','ặ':'a','â':'a','ầ':'a','ấ':'a','ẩ':'a','ẫ':'a','ậ':'a','è':'e','é':'e','ẻ':'e','ẽ':'e','ẹ':'e','ê':'e','ề':'e','ế':'e','ể':'e','ễ':'e','ệ':'e','ì':'i','í':'i','ỉ':'i','ĩ':'i','ị':'i','ò':'o','ó':'o','ỏ':'o','õ':'o','ọ':'o','ô':'o','ồ':'o','ố':'o','ổ':'o','ỗ':'o','ộ':'o','ơ':'o','ờ':'o','ớ':'o','ở':'o','ỡ':'o','ợ':'o','ù':'u','ú':'u','ủ':'u','ũ':'u','ụ':'u','ư':'u','ừ':'u','ứ':'u','ử':'u','ữ':'u','ự':'u','ỳ':'y','ý':'y','ỷ':'y','ỹ':'y','ỵ':'y','đ':'d'};
  function _rmDiacritics(s){return s.replace(/./g,ch=>_vnAlarmMap[ch]||ch);}

  window.App = window.App || {};
  App.filterTable = function (tableSelector, keyword) {
    const table = document.querySelector(tableSelector);
    if (!table) return;
    const rows = table.querySelectorAll('tbody tr');
    const search = keyword.trim().toLowerCase();
    const searchAscii = _rmDiacritics(search);
    rows.forEach(row => {
      // Combine all cell text in the row
      const text = Array.from(row.cells).map(td => td.textContent.toLowerCase()).join(' ');
      // Show row if search is empty or found in text (with or without diacritics)
      row.style.display = (!search || text.includes(search) || _rmDiacritics(text).includes(searchAscii)) ? '' : 'none';
    });
    updateAlarmRowCount();
  };

  function getAlarmSearchQuery() {
    return (document.getElementById('alarmSearchInput')?.value || '').trim();
  }

  function reapplyAlarmSearch() {
    App.filterTable('#tbl', getAlarmSearchQuery());
  }

  function updateAlarmRowCount() {
    const countDiv = document.getElementById('rowCount');
    if (!countDiv) return;

    const activeSearch = getAlarmSearchQuery();
    const visibleRows = document.querySelectorAll('#tbl tbody tr:not([style*="display: none"])');
    const dataRows = Array.from(visibleRows).filter(row => !row.querySelector('td[colspan]'));

    if (activeSearch) {
      countDiv.textContent = `Showing ${dataRows.length} filtered rows on page ${currentPage}/${totalPages} (from ${totalRows} total rows)`;
      return;
    }

    const startRow = (currentPage - 1) * pageSize + 1;
    const endRow = Math.min(currentPage * pageSize, totalRows);
    countDiv.textContent = totalRows > 0
      ? `Showing ${startRow}-${endRow} of ${totalRows} rows (Page ${currentPage}/${totalPages})`
      : 'Showing 0 rows';
  }

  // ===== Pagination Variables =====
  let currentPage = 1;
  const pageSize = 100;
  let totalPages = 1;
  let totalRows = 0;
  let isLoading = false;

  let lastAlarmId = null;
  let selectedIds = new Set();
  let backToTopShown = false; // track first show state

  function toggleSelect(id, checked) {
    if (checked) selectedIds.add(String(id));
    else selectedIds.delete(String(id));
  }

  function toggleAll(source) {
    const checkboxes = document.querySelectorAll('input[name="ids"]');
    checkboxes.forEach(cb => {
      cb.checked = source.checked;
      toggleSelect(cb.value, cb.checked);
    });
  }

  // Load alarm events by page
  async function loadPage(page) {
    if (isLoading) return;

    isLoading = true;
    currentPage = page;

    // Forward the current page's time filter params to the API
    const apiUrl = new URL(window.location.origin + "/alarms/api/alarm-events");
    const pageParams = new URLSearchParams(window.location.search);
    const tf = pageParams.get('time_filter');
    const fd = pageParams.get('from_date');
    const td = pageParams.get('to_date');

    if (tf) apiUrl.searchParams.set('time_filter', tf);
    if (tf === 'custom') {
      if (fd) apiUrl.searchParams.set('from_date', fd);
      if (td) apiUrl.searchParams.set('to_date', td);
    }

    // Add pagination params
    apiUrl.searchParams.set('offset', (page - 1) * pageSize);
    apiUrl.searchParams.set('limit', pageSize);

    try {
      const res = await fetch(apiUrl.toString());
      const data = await res.json();

      if (data.success && data.items) {
        totalRows = data.total || 0;
        totalPages = Math.ceil(totalRows / pageSize);

        const tbody = document.querySelector("#tbl tbody");
        if (!tbody) return;

        tbody.innerHTML = "";

        if (data.items.length === 0) {
          const colCount = (window.ALARM_EVENTS_CONFIG && window.ALARM_EVENTS_CONFIG.isAdmin) ? 5 : 4;
          tbody.innerHTML = `<tr><td colspan="${colCount}" class="text-center text-muted py-4">No alarm events.</td></tr>`;
        } else {
          data.items.forEach(e => {
            const checked = selectedIds.has(String(e.id)) ? "checked" : "";
            const note = (e.note || "").toLowerCase();

            // 🌈 Màu nền cho cả row dựa trên note
            const rowClass =
              note.includes("clear") ? "table-success" :
                note.includes("activated") ? "table-danger" : "";

            // 🎯 Màu badge riêng cho note
            const noteBadgeClass =
              note.includes("clear") ? "text-bg-success" :
                note.includes("activated") ? "text-bg-danger" :
                  "text-bg-secondary";

            const row = document.createElement("tr");
            row.className = rowClass; // 👈 Nền của cả hàng

            const isAdmin = window.ALARM_EVENTS_CONFIG && window.ALARM_EVENTS_CONFIG.isAdmin;
            const checkboxTd = isAdmin
              ? `<td><input type="checkbox" name="ids" value="${e.id}" ${checked} onchange="toggleSelect(this.value,this.checked)"></td>`
              : '';
            row.innerHTML = `
            ${checkboxTd}
            <td>${escapeHtml(e.ts || "")}</td>
            <td>${escapeHtml(e.name || "")}</td>
            <td class="text-center">${escapeHtml(String(e.value ?? ""))}</td>
            <td><span class="badge ${noteBadgeClass}">${escapeHtml(e.note || "")}</span></td>
          `;

            tbody.appendChild(row);
          });
        }

        // Update pagination UI
        updatePaginationUI();
        reapplyAlarmSearch();

        // 🔒 Bảo vệ chống XSS
        function escapeHtml(s) {
          return String(s)
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#039;");
        }
      }
    } catch (error) {
      console.error('Error loading alarm events:', error);
    } finally {
      isLoading = false;
    }
  }

  // Pagination navigation functions
  function nextPage() {
    if (currentPage < totalPages) {
      loadPage(currentPage + 1);
    }
  }

  function prevPage() {
    if (currentPage > 1) {
      loadPage(currentPage - 1);
    }
  }

  function goToPage(page) {
    if (page >= 1 && page <= totalPages) {
      loadPage(page);
    }
  }

  // Update pagination UI
  function updatePaginationUI() {
    const paginationContainer = document.getElementById('paginationContainer');
    if (!paginationContainer) return;

    let html = '';

    // Previous button
    html += `<li class="page-item ${currentPage === 1 ? 'disabled' : ''}">`;
    html += `<a class="page-link" href="#" onclick="prevPage(); return false;">Previous</a>`;
    html += `</li>`;

    // Page numbers
    const maxVisible = 5;
    let startPage = Math.max(1, currentPage - Math.floor(maxVisible / 2));
    let endPage = Math.min(totalPages, startPage + maxVisible - 1);

    if (endPage - startPage < maxVisible - 1) {
      startPage = Math.max(1, endPage - maxVisible + 1);
    }

    if (startPage > 1) {
      html += `<li class="page-item"><a class="page-link" href="#" onclick="goToPage(1); return false;">1</a></li>`;
      if (startPage > 2) {
        html += `<li class="page-item disabled"><span class="page-link">...</span></li>`;
      }
    }

    for (let i = startPage; i <= endPage; i++) {
      html += `<li class="page-item ${i === currentPage ? 'active' : ''}">`;
      html += `<a class="page-link" href="#" onclick="goToPage(${i}); return false;">${i}</a>`;
      html += `</li>`;
    }

    if (endPage < totalPages) {
      if (endPage < totalPages - 1) {
        html += `<li class="page-item disabled"><span class="page-link">...</span></li>`;
      }
      html += `<li class="page-item"><a class="page-link" href="#" onclick="goToPage(${totalPages}); return false;">${totalPages}</a></li>`;
    }

    // Next button
    html += `<li class="page-item ${currentPage === totalPages ? 'disabled' : ''}">`;
    html += `<a class="page-link" href="#" onclick="nextPage(); return false;">Next</a>`;
    html += `</li>`;

    paginationContainer.innerHTML = html;

    // Update row count display
    updateAlarmRowCount();
  }

  function fetchAlarmEvents() {
    // Do not auto-refresh while the user is actively filtering the current page.
    // Otherwise the API render will keep replacing the filtered DOM every few seconds.
    if (getAlarmSearchQuery()) {
      return;
    }

    // Skip background refresh when tab is hidden to reduce noisy API calls.
    if (document.hidden) {
      return;
    }

    // Use loadPage instead
    loadPage(currentPage);
  }

  // Initial load only. New data is fetched on manual page refresh.
  loadPage(1);

  // Confirm and populate IDs before delete
  function confirmDelete(event) {
    if (selectedIds.size === 0) {
      event.preventDefault();
      alert('Please select at least one alarm event to delete');
      return false;
    }

    const idsArray = Array.from(selectedIds);
    document.getElementById('ids-input').value = idsArray.join(",");
    console.log('Deleting IDs:', idsArray.join(",")); // Debug log

    return confirm(`Delete ${selectedIds.size} selected alarm event(s)?`);
  }

  async function exportAlarms() {
    if (!window.ExcelJS || !window.saveAs) {
      alert('Không tải được thư viện Excel. Vui lòng kiểm tra mạng/CDN và thử lại.');
      return;
    }
    // Chuẩn hóa dấu thập phân về dạng x.x và buộc Excel giữ nguyên (đẩy dạng text)
    const normalizeNumber = (value) => {
      if (value === null || value === undefined) return "";
      const str = String(value).trim();
      if (!str) return "";
      const normalized = str.replace(/,/g, ".");
      // Thêm ' để Excel coi là text, không tự đổi sang dấu phẩy theo locale
      return `${normalized}`;
    };
    // Determine current filter and custom range BEFORE building API URL
    const filterSelect = document.getElementById('timeFilterSelect');
    const selectedFilter = filterSelect ? filterSelect.value : 'all';
    const fromInput = document.getElementById('fromDate');
    const toInput = document.getElementById('toDate');
    const qInput = document.getElementById('alarmSearchInput');
    const q = (qInput?.value || '').trim();

    // Build API URL with time filter params so backend returns filtered data
    const apiUrl = new URL(window.location.origin + "/alarms/api/alarm-report");
    apiUrl.searchParams.set('time_filter', selectedFilter);
    if (selectedFilter === 'custom') {
      const rawFrom = (fromInput?.value || '').trim();
      const rawTo = (toInput?.value || '').trim();
      if (rawFrom && rawTo) {
        apiUrl.searchParams.set('from_date', rawFrom);
        apiUrl.searchParams.set('to_date', rawTo);
      }
    }
    if (q) {
      apiUrl.searchParams.set('q', q);
    }
    const res = await fetch(apiUrl.toString());
    const data = await res.json();
    let exportItems = data.items || [];
    if (q) {
      const qLower = q.toLowerCase();
      exportItems = exportItems.filter(e => {
        const text = [
          e.code || "",
          e.incoming_date || "",
          e.incoming_value ?? "",
          e.outgoing_date || "",
          e.outgoing_value ?? "",
          e.operator || "",
          e.threshold ?? ""
        ].join(' ').toLowerCase();
        return text.includes(qLower);
      });
    }

    if (!exportItems.length) {
      alert("Không có dữ liệu alarm để xuất!");
      return;
    }

    const workbook = new ExcelJS.Workbook();
    const sheet = workbook.addWorksheet("Alarm Report");

    // ==== Compute FROM / TO similar to Reports ====

    const fmt = (d) => {
      if (!d) return '';
      // Format: DD/MM/YYYY HH:mm:ss (local)
      const pad = (n) => String(n).padStart(2, '0');
      return `${pad(d.getDate())}/${pad(d.getMonth() + 1)}/${d.getFullYear()} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
    };
    const startOfDay = (d) => new Date(d.getFullYear(), d.getMonth(), d.getDate(), 0, 0, 0, 0);
    const endOfDay = (d) => new Date(d.getFullYear(), d.getMonth(), d.getDate(), 23, 59, 59, 999);

    // Use input values if custom; otherwise compute window
    const normalizeLocal = (s) => (s || '').replace('T', ' ');
    let fromDateStr = normalizeLocal(fromInput?.value || '');
    let toDateStr = normalizeLocal(toInput?.value || '');

    if (selectedFilter !== 'custom') {
      const now = new Date();
      let fromD = null, toD = null;
      switch (selectedFilter) {
        case 'today':
          fromD = startOfDay(now); toD = now; break;
        case 'yesterday': {
          const y = new Date(now); y.setDate(now.getDate() - 1);
          fromD = startOfDay(y); toD = endOfDay(y); break;
        }
        case 'last7days': {
          const f = new Date(now); f.setDate(now.getDate() - 7);
          fromD = f; toD = now; break;
        }
        case 'last30days': {
          const f = new Date(now); f.setDate(now.getDate() - 30);
          fromD = f; toD = now; break;
        }
        default:
          // 'all' -> leave empty
          break;
      }
      fromDateStr = fromD ? fmt(fromD) : fromDateStr;
      toDateStr = toD ? fmt(toD) : toDateStr;
    }

    // ==== Title and Period Rows ====
    sheet.addRow(["BẢNG ALARM REPORT"]);
    sheet.addRow(["FROM DATE:", fromDateStr || (selectedFilter === 'all' ? 'All Time' : '')]);
    sheet.addRow(["TO DATE:", toDateStr || (selectedFilter === 'all' ? 'All Time' : '')]);
    sheet.addRow([]);
    sheet.addRow([]);

    const header = [
      "ALARM CODE", "INCOMING DATE", "INCOMING VALUE",
      "OUTGOING DATE", "OUTGOING VALUE", "MODE COMPARE", "VALUE COMPARE"
    ];
    const headerRow = sheet.addRow(header);

    headerRow.eachCell((cell, colNumber) => {
      cell.fill = { type: "pattern", pattern: "solid", fgColor: { argb: "FFD966" } };
      cell.font = { bold: true };
      cell.alignment = { horizontal: colNumber === 1 ? "left" : "center" };
      cell.border = {
        top: { style: "thin" },
        bottom: { style: "thin" },
        left: { style: "thin" },
        right: { style: "thin" }
      };
    });

    exportItems.forEach(e => {
      const row = sheet.addRow([
        e.code || "", e.incoming_date || "", normalizeNumber(e.incoming_value),
        e.outgoing_date || "", normalizeNumber(e.outgoing_value), e.operator || "", normalizeNumber(e.threshold)
      ]);
      // Căn trái cột ALARM CODE, các cột còn lại giữ center
      row.eachCell((cell, colNumber) => {
        cell.alignment = { horizontal: colNumber === 1 ? "left" : "center", vertical: "middle" };
      });
    });

    // Add thin borders for all cells (header + data) và căn giữa mặc định
    sheet.eachRow({ includeEmpty: true }, (row) => {
      row.eachCell((cell) => {
        cell.border = {
          top: { style: "thin" },
          bottom: { style: "thin" },
          left: { style: "thin" },
          right: { style: "thin" }
        };
        if (!cell.alignment || !cell.alignment.horizontal) {
          cell.alignment = { horizontal: "center", vertical: "middle" };
        }
      });
    });

    // Auto-fit chiều rộng theo nội dung (cộng thêm 5 ký tự đệm cho thoáng hơn)
    sheet.columns.forEach(col => {
      let max = 0;
      col.eachCell({ includeEmpty: true }, c => {
        const len = c.value ? c.value.toString().length : 0;
        max = Math.max(max, len);
      });
      col.width = Math.max(15, max + 5);
    });

    const buffer = await workbook.xlsx.writeBuffer();
    saveAs(new Blob([buffer]), "alarm_report.xlsx");
  }

  // ==== Time Filter Functions ====

  // Show/hide custom date range based on selection
  document.addEventListener('change', function (e) {
    if (e.target.id === 'timeFilterSelect') {
      const customDateRange = document.getElementById('customDateRange');
      if (e.target.value === 'custom') {
        customDateRange.style.display = 'block';
        // Set default dates if empty (format dd/mm/yyyy HH:mm)
        const now = new Date();
        const pad = (n) => String(n).padStart(2, '0');
        const todayStr = `${pad(now.getDate())}/${pad(now.getMonth() + 1)}/${now.getFullYear()} 00:00`;
        const tomorrowDate = new Date(now.getFullYear(), now.getMonth(), now.getDate() + 1);
        const tomorrowStr = `${pad(tomorrowDate.getDate())}/${pad(tomorrowDate.getMonth() + 1)}/${tomorrowDate.getFullYear()} 00:00`;

        const fromDate = document.getElementById('fromDate');
        const toDate = document.getElementById('toDate');

        if (!fromDate.value) {
          fromDate.value = todayStr;
          // Update flatpickr instance if available
          if (fromDate._flatpickr) fromDate._flatpickr.setDate(todayStr, true);
        }
        if (!toDate.value) {
          toDate.value = tomorrowStr;
          if (toDate._flatpickr) toDate._flatpickr.setDate(tomorrowStr, true);
        }
      } else {
        customDateRange.style.display = 'none';
        // Auto-apply for non-custom selections
        applyTimeFilter();
      }
    }
  });

  // On page load, show custom date range if filter is 'custom'
  document.addEventListener('DOMContentLoaded', function () {
    const timeFilter = ALARM_EVENTS_CONFIG.timeFilter || '';
    if (timeFilter === "custom") {
      document.getElementById('customDateRange').style.display = 'block';
    }
    document.body.style.cursor = 'default';

    // Đảm bảo nút export luôn gắn handler cả khi inline bị chặn bởi CSP
    const exportBtn = document.getElementById('exportBtn');
    if (exportBtn) {
      exportBtn.addEventListener('click', exportAlarms);
    }
  });

  // Apply time filter
  function applyTimeFilter() {
    const selectedFilter = document.getElementById('timeFilterSelect').value;
    console.log('Applying time filter:', selectedFilter); // Debug log

    const url = new URL(window.location);

    // Clear existing time parameters
    url.searchParams.delete('time_filter');
    url.searchParams.delete('from_date');
    url.searchParams.delete('to_date');

    // Set new parameters
    url.searchParams.set('time_filter', selectedFilter);

    if (selectedFilter === 'custom') {
      const fromDate = document.getElementById('fromDate').value;
      const toDate = document.getElementById('toDate').value;

      console.log('Custom dates:', fromDate, toDate); // Debug log

      if (!fromDate || !toDate) {
        alert('Please select both from and to dates for custom range');
        return;
      }

      // Validate date range
      if (new Date(fromDate) > new Date(toDate)) {
        alert('From date must be before to date');
        return;
      }

      url.searchParams.set('from_date', fromDate);
      url.searchParams.set('to_date', toDate);
    }

    console.log('Redirecting to:', url.toString()); // Debug log

    // Show loading indicator
    document.body.style.cursor = 'wait';

    // Reload page with new parameters
    window.location.href = url.toString();
  }

  // Reset cursor when page loads
  document.addEventListener('DOMContentLoaded', function () {
    document.body.style.cursor = 'default';
  });
  function scrollToTop() {
    const tableWrap = document.querySelector('.table-wrap');
    const useTable = tableWrap && (tableWrap.scrollHeight - tableWrap.clientHeight > 5);
    const target = useTable ? tableWrap : document.documentElement;
    const current = useTable
      ? tableWrap.scrollTop
      : (window.pageYOffset || document.documentElement.scrollTop || 0);
    const maxScroll = useTable
      ? Math.max(0, tableWrap.scrollHeight - tableWrap.clientHeight)
      : Math.max(0, document.documentElement.scrollHeight - window.innerHeight);

    const nearBottom = (maxScroll - current) <= 10;

    // If at bottom → go top, else go bottom (includes top/middle)
    const next = nearBottom ? 0 : maxScroll;

    if (useTable) {
      tableWrap.scrollTo({ top: next, left: 0, behavior: 'smooth' });
    } else {
      window.scrollTo({ top: next, behavior: 'smooth' });
    }
    updateBackToTopState();
  }

  function updateBackToTopState() {
    const btn = document.getElementById('backToTop');
    if (!btn) return;

    const tableWrap = document.querySelector('.table-wrap');
    const tableScroll = tableWrap ? tableWrap.scrollTop : 0;
    const pageScroll = window.pageYOffset || document.documentElement.scrollTop || 0;
    const shouldShow = tableScroll > 80 || pageScroll > 80;

    if (!shouldShow) {
      btn.classList.remove('show');
      backToTopShown = false;
      const iconReset = btn.querySelector('i');
      if (iconReset) {
        iconReset.classList.remove('bi-arrow-up', 'bi-arrow-down');
        iconReset.classList.add('bi-arrow-down');
      }
      return;
    }

    if (shouldShow && !backToTopShown) {
      // First appearance after scroll: force down arrow and show
      btn.classList.add('show');
      backToTopShown = true;
      const firstIcon = btn.querySelector('i');
      if (firstIcon) {
        firstIcon.classList.remove('bi-arrow-up', 'bi-arrow-down');
        firstIcon.classList.add('bi-arrow-down');
      }
      return;
    }

    btn.classList.add('show');

    const icon = btn.querySelector('i');
    if (!icon) return;

    const useTable = tableWrap && (tableWrap.scrollHeight - tableWrap.clientHeight > 5);
    const current = useTable
      ? tableWrap.scrollTop
      : (window.pageYOffset || document.documentElement.scrollTop || 0);
    const maxScroll = useTable
      ? Math.max(0, tableWrap.scrollHeight - tableWrap.clientHeight)
      : Math.max(0, document.documentElement.scrollHeight - window.innerHeight);
    const nearBottom = (maxScroll - current) <= 10 && current > 160; // chỉ đổi lên khi sát đáy

    icon.classList.remove('bi-arrow-up', 'bi-arrow-down');
    icon.classList.add(nearBottom ? 'bi-arrow-up' : 'bi-arrow-down');
  }

  document.addEventListener('DOMContentLoaded', () => {
    const tableWrap = document.querySelector('.table-wrap');
    if (tableWrap) {
      tableWrap.addEventListener('scroll', updateBackToTopState);
    }
    window.addEventListener('scroll', updateBackToTopState);
    updateBackToTopState();
  });

  // Mobile-friendly positioning & hit area
  document.addEventListener('DOMContentLoaded', () => {
    const btn = document.getElementById('backToTop');
    if (!btn) return;
    const applyMobileStyle = () => {
      const isMobile = window.matchMedia('(max-width: 576px)').matches;
      btn.style.bottom = isMobile ? '20px' : '16px';
      btn.style.right = isMobile ? '12px' : '20px';
      btn.style.width = isMobile ? '56px' : '50px';
      btn.style.height = isMobile ? '56px' : '50px';
      btn.style.borderRadius = isMobile ? '28px' : '25px';
      btn.style.backgroundImage = 'linear-gradient(135deg, #3b82f6, #2563eb)';
      btn.style.boxShadow = '0 8px 18px rgba(37, 99, 235, 0.35)';
      btn.style.opacity = '0.9';
      btn.style.transition = 'transform 0.2s ease, box-shadow 0.2s ease, opacity 0.2s ease';
    };
    applyMobileStyle();
    window.addEventListener('resize', applyMobileStyle);
    btn.addEventListener('pointerdown', () => { btn.style.transform = 'scale(0.96)'; });
    btn.addEventListener('pointerup', () => { btn.style.transform = 'scale(1)'; });
  });
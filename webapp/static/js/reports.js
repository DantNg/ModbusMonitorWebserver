/* ==========================================================================
   Reports Page - Main JavaScript
   Extracted from reports/reports.html inline <script>
   ========================================================================== */

// Read configuration passed from Jinja2 template
const REPORTS_CONFIG = window.REPORTS_CONFIG || {};

  // ===== Pagination Variables =====
  let currentPage = 1;
  const pageSize = 100;
  let isLoading = false;
  let totalPages = 1;
  let totalRows = 0;
  let allLoadedRows = []; // Store all loaded rows for filtering and comparison
  let currentLoggerId = REPORTS_CONFIG.currentLoggerId; // Store current logger ID

  // Back to top functionality
 function scrollToTop() {
    const tbl = document.getElementById('tbl');
    const scroller = tbl?.closest('.table-responsive') || tbl?.parentElement;
    const useTable = scroller && scroller.scrollHeight - scroller.clientHeight > 5;
    const current = useTable
      ? scroller.scrollTop
      : (window.pageYOffset || document.documentElement.scrollTop || 0);
    const maxScroll = useTable
      ? Math.max(0, scroller.scrollHeight - scroller.clientHeight)
      : Math.max(0, document.documentElement.scrollHeight - window.innerHeight);
    const nearBottom = (maxScroll - current) <= 10;
    const next = nearBottom ? 0 : maxScroll;

    setBackToTopIcon(nearBottom ? 'up' : 'down'); // gần đáy thì icon lên, còn lại xuống

    if (useTable) {
      scroller.scrollTo({ top: next, left: 0, behavior: 'smooth' });
    } else {
      window.scrollTo({ top: next, behavior: 'smooth' });
    }
    updateBackToTopState();
  }

  function updateBackToTopState() {
    const btn = document.getElementById('backToTop');
    if (!btn) return;
    const tbl = document.getElementById('tbl');
    const scroller = tbl?.closest('.table-responsive') || tbl?.parentElement;
    const tableScroll = scroller ? scroller.scrollTop : 0;
    const pageScroll = window.scrollY || document.documentElement.scrollTop || 0;
    const shouldShow = tableScroll > 80 || pageScroll > 80;
    btn.style.display = shouldShow ? 'block' : 'none';

    const icon = btn.querySelector('i');
    if (!icon) return;
    const useTable = scroller && scroller.scrollHeight - scroller.clientHeight > 5;
    const current = useTable
      ? scroller.scrollTop
      : (window.pageYOffset || document.documentElement.scrollTop || 0);
    const maxScroll = useTable
      ? Math.max(0, scroller.scrollHeight - scroller.clientHeight)
      : Math.max(0, document.documentElement.scrollHeight - window.innerHeight);
    const nearBottom = (maxScroll - current) <= 10;
    setBackToTopIcon(nearBottom ? 'up' : 'down');
  }

  function setBackToTopIcon(direction) {
    const icon = document.querySelector('#backToTop i');
    if (!icon) return;
    icon.classList.remove('bi-arrow-up', 'bi-arrow-down');
    if (direction === 'up') icon.classList.add('bi-arrow-up');
    else icon.classList.add('bi-arrow-down');
  }

  // Show/hide back to top button
  window.addEventListener('scroll', updateBackToTopState);

  // Initialize - show button if already scrolled
  document.addEventListener('DOMContentLoaded', function () {
    updateBackToTopState();
    // Poll in case scroll events miss (mobile inertia/overscroll)
    setInterval(updateBackToTopState, 250);
    
    // Load comparison config first, then load data
    loadComparisonConfig().then(() => {
      if (comparisonPairs.length > 0) {
        renderComparisonPairs();
      } else {
        addComparisonPair(); // Add first pair by default
      }
      
      // Load initial data after comparison config is loaded
      loadPage(1);
    });
    
    // Setup scroll listener for back to top button only
    const tableContainer = document.querySelector('.table-responsive');
    if (tableContainer) {
      tableContainer.addEventListener('scroll', updateBackToTopState);
    }
  });

  // Mobile-friendly sizing and touch feedback
  document.addEventListener('DOMContentLoaded', () => {
    const btn = document.getElementById('backToTop');
    if (!btn) return;
    const applyMobileStyle = () => {
      const isMobile = window.matchMedia('(max-width: 576px)').matches;
      btn.style.bottom = isMobile ? '12px' : '20px';
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

  // Load data from API by page
  async function loadPage(page) {
    if (isLoading) return;
    
    isLoading = true;
    currentPage = page;
    
    // Show loading indicator
    const loadingIndicator = document.getElementById('loadingIndicator');
    if (loadingIndicator) {
      loadingIndicator.classList.add('active');
    }
    
    // Build API URL with current filter params
    const url = new URL(window.location.origin + '/reports/api/data');
    const params = new URLSearchParams(window.location.search);
    params.set('offset', (page - 1) * pageSize);
    params.set('limit', pageSize);
    url.search = params.toString();
    
    try {
      const response = await fetch(url);
      const data = await response.json();
      
      if (data.success && data.items) {
        // Clear existing rows and replace with new page data
        const tbody = document.querySelector('#tbl tbody');
        if (tbody) tbody.innerHTML = '';
        
        if (data.items.length > 0) {
          appendRowsToTable(data.items, data.columns);
          allLoadedRows = data.items;
          
          // Calculate total pages
          totalRows = data.total || data.items.length;
          totalPages = Math.ceil(totalRows / pageSize);
        } else {
          // No data
          const tbody = document.querySelector('#tbl tbody');
          if (tbody) {
            tbody.innerHTML = '<tr><td colspan="100" class="text-center text-muted py-4">No data available.</td></tr>';
          }
          totalPages = 1;
        }
        
        // Update pagination UI
        updatePaginationUI();
        
        // Apply comparison if active
        if (comparisonPairs.length > 0) {
          setTimeout(() => applyComparison(), 100);
        }
      }
    } catch (error) {
      console.error('Error loading data:', error);
      alert('Error loading data. Please try again.');
    } finally {
      isLoading = false;
      
      // Hide loading indicator
      if (loadingIndicator) {
        loadingIndicator.classList.remove('active');
      }
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
    const countDiv = document.querySelector('.card-footer div');
    if (countDiv) {
      const startRow = (currentPage - 1) * pageSize + 1;
      const endRow = Math.min(currentPage * pageSize, totalRows);
      countDiv.textContent = `Showing ${startRow}-${endRow} of ${totalRows} rows (Page ${currentPage}/${totalPages})`;
    }
  }
  
  // Append rows to table
  function appendRowsToTable(items, columns) {
    const tbody = document.querySelector('#tbl tbody');
    if (!tbody) return;
    
    // Remove "No data" row if exists
    const noDataRow = tbody.querySelector('td[colspan]');
    if (noDataRow) {
      noDataRow.parentElement.remove();
    }
    
    items.forEach(row => {
      const tr = document.createElement('tr');
      
      // Timestamp cell
      const tdTs = document.createElement('td');
      tdTs.textContent = row.timestamp || '';
      tr.appendChild(tdTs);
      
      // Data cells
      columns.forEach(col => {
        if (col !== 'timestamp') {
          const td = document.createElement('td');
          td.className = 'text-center';
          td.textContent = row[col] !== null && row[col] !== undefined ? row[col] : '';
          tr.appendChild(td);
        }
      });
      
      tbody.appendChild(tr);
    });
  }
  
  // Update row count display (now handled by updatePaginationUI)
  function updateRowCount() {
    const countDiv = document.querySelector('.card-footer div');
    if (countDiv) {
      const visibleCount = document.querySelectorAll('#tbl tbody tr:not([style*="display: none"])').length;
      const startRow = (currentPage - 1) * pageSize + 1;
      const endRow = Math.min(currentPage * pageSize, totalRows);
      countDiv.textContent = `Showing ${startRow}-${endRow} of ${totalRows} rows (Page ${currentPage}/${totalPages})`;
    }
  }

  // Toggle custom date range visibility
  function toggleCustomRange() {
    const select = document.getElementById('timeFilterSelect');
    const customRange = document.getElementById('customDateRange');
    if (select.value === 'custom') {
      customRange.classList.remove('d-none');
    } else {
      customRange.classList.add('d-none');
    }
  }

  // Lọc nhanh client-side
  function filterRows(q) {
    q = (q || '').toLowerCase();
    document.querySelectorAll('#tbl tbody tr').forEach(tr => {
      if (tr.querySelector('td[colspan]')) return; // hàng "No data"
      tr.style.display = tr.innerText.toLowerCase().includes(q) ? '' : 'none';
    });
    updateRowCount();
  }

  // ===== Advanced Comparison Logic =====
  let comparisonPairs = [];
  const PRIMARY_TAG_COLOR = '#e3f2fd'; // Light blue for primary tags

  function addComparisonPair() {
    const pair = {
      id: Date.now(),
      primaryTag: '',
      compareTag1: '',
      condition1: 'eq',
      compareTag2: '',
      condition2: 'eq',
      color: '#ffeb3b'
    };
    comparisonPairs.push(pair);
    renderComparisonPairs();
    saveComparisonConfig();
  }

  function removeComparisonPair(id) {
    comparisonPairs = comparisonPairs.filter(p => p.id !== id);
    renderComparisonPairs();
    saveComparisonConfig();
    applyComparison();
  }

  function getConditionSymbol(condition) {
    const symbols = { eq: '=', ne: '≠', gt: '>', lt: '<', gte: '≥', lte: '≤' };
    return symbols[condition] || '=';
  }

  function generateConditionPreview(pair) {
    if (!pair.primaryTag) return '<span class="text-muted">No main tag selected</span>';
    
    let preview = '';
    const primaryTag = pair.primaryTag.toUpperCase();
    const color = pair.color || pair.color1 || '#ffeb3b'; // Fallback for old config
    
    // Helper function to reverse condition for display
    const reverseCondition = (condition) => {
      const reverseMap = { eq: '=', ne: '≠', gt: '<', lt: '>', gte: '≤', lte: '≥' };
      return reverseMap[condition] || '=';
    };
    
    // Condition 1
    if (pair.compareTag1) {
      const compareTag1 = pair.compareTag1.toUpperCase();
      const symbol1 = reverseCondition(pair.condition1);
      preview += `<span class="badge" style="background-color: ${color}; color: #000; font-size: 0.9rem; padding: 6px 10px;">`;
      preview += `${primaryTag} ${symbol1} ${compareTag1}`;
      preview += `</span>`;
    }
    
    // AND if both conditions exist
    if (pair.compareTag1 && pair.compareTag2) {
      preview += ` <span class="text-muted mx-1">AND</span> `;
    }
    
    // Condition 2
    if (pair.compareTag2) {
      const compareTag2 = pair.compareTag2.toUpperCase();
      const symbol2 = reverseCondition(pair.condition2);
      preview += `<span class="badge" style="background-color: ${color}; color: #000; font-size: 0.9rem; padding: 6px 10px;">`;
      preview += `${primaryTag} ${symbol2} ${compareTag2}`;
      preview += `</span>`;
    }
    
    if (!pair.compareTag1 && !pair.compareTag2) {
      preview = '<span class="text-muted">No compare tag selected</span>';
    }
    
    return preview;
  }

  function renderComparisonPairs() {
    const container = document.getElementById('comparisonPairsContainer');
    if (!container) return;

    const allTags = Array.from(document.querySelectorAll('#tbl thead th'))
      .map(th => th.textContent.trim())
      .filter(t => t.toLowerCase() !== 'timestamp');

    container.innerHTML = comparisonPairs.map((pair, index) => `
      <div class="card mb-3 shadow-sm" style="border-left: 4px solid #2196F3;">
        <div class="card-body p-3">
          <div class="d-flex justify-content-between align-items-center mb-3">
            <strong class="text-primary">Condition ${index + 1}</strong>
            ${comparisonPairs.length > 1 ? `
              <button type="button" class="btn btn-sm btn-outline-danger" onclick="removeComparisonPair(${pair.id})">
                <i class="bi bi-trash"></i>
              </button>
            ` : ''}
          </div>
          
          <!-- Condition Preview -->
          <div class="alert condition-preview-box border mb-3 py-2">
            <small class="text-muted d-block mb-1"><i class="bi bi-eye"></i> Condition Preview:</small>
            <div class="preview-condition">${generateConditionPreview(pair)}</div>
          </div>
          
          <div class="row g-2 mb-3">
            <div class="col-md-9">
              <label class="form-label small mb-1"><strong>Main Tag (will be highlighted)</strong></label>
              <select class="form-select form-select-sm" onchange="updatePairData(${pair.id}, 'primaryTag', this.value); renderComparisonPairs();">
                <option value="">-- Select tag --</option>
                ${allTags.map(t => `<option value="${t}" ${pair.primaryTag === t ? 'selected' : ''}>${t.toUpperCase()}</option>`).join('')}
              </select>
            </div>
            <div class="col-md-3">
              <label class="form-label small mb-1"><strong>Highlight Color</strong></label>
              <input type="color" class="form-control form-control-sm form-control-color w-100" 
                     value="${pair.color || pair.color1 || '#ffeb3b'}" 
                     onchange="updatePairData(${pair.id}, 'color', this.value); renderComparisonPairs();">
            </div>
          </div>

          <div class="row g-2 mb-2 align-items-end">
            <div class="col-md-6">
              <label class="form-label small mb-1">Compare Tag 1</label>
              <select class="form-select form-select-sm" onchange="updatePairData(${pair.id}, 'compareTag1', this.value); renderComparisonPairs();">
                <option value="">-- Not selected --</option>
                ${allTags.map(t => `<option value="${t}" ${pair.compareTag1 === t ? 'selected' : ''}>${t.toUpperCase()}</option>`).join('')}
              </select>
            </div>
            <div class="col-md-3">
              <label class="form-label small mb-1">Condition</label>
              <select class="form-select form-select-sm" onchange="updatePairData(${pair.id}, 'condition1', this.value); renderComparisonPairs();">
                <option value="eq" ${pair.condition1 === 'eq' ? 'selected' : ''}>=</option>
                <option value="ne" ${pair.condition1 === 'ne' ? 'selected' : ''}>≠</option>
                <option value="gt" ${pair.condition1 === 'gt' ? 'selected' : ''}>&gt;</option>
                <option value="lt" ${pair.condition1 === 'lt' ? 'selected' : ''}>&lt;</option>
                <option value="gte" ${pair.condition1 === 'gte' ? 'selected' : ''}>≥</option>
                <option value="lte" ${pair.condition1 === 'lte' ? 'selected' : ''}>≤</option>
              </select>
            </div>
            <div class="col-md-3">
              <small class="text-muted d-block mb-1">
                ${pair.compareTag1 && pair.primaryTag ? `Highlight <strong>${pair.primaryTag.toUpperCase()}</strong> when met` : 'No tag selected'}
              </small>
            </div>
          </div>

          <div class="row g-2 align-items-end">
            <div class="col-md-6">
              <label class="form-label small mb-1">Compare Tag 2 (optional)</label>
              <select class="form-select form-select-sm" onchange="updatePairData(${pair.id}, 'compareTag2', this.value); renderComparisonPairs();">
                <option value="">-- Not selected --</option>
                ${allTags.map(t => `<option value="${t}" ${pair.compareTag2 === t ? 'selected' : ''}>${t.toUpperCase()}</option>`).join('')}
              </select>
            </div>
            <div class="col-md-3">
              <label class="form-label small mb-1">Condition</label>
              <select class="form-select form-select-sm" onchange="updatePairData(${pair.id}, 'condition2', this.value); renderComparisonPairs();">
                <option value="eq" ${pair.condition2 === 'eq' ? 'selected' : ''}>=</option>
                <option value="ne" ${pair.condition2 === 'ne' ? 'selected' : ''}>≠</option>
                <option value="gt" ${pair.condition2 === 'gt' ? 'selected' : ''}>&gt;</option>
                <option value="lt" ${pair.condition2 === 'lt' ? 'selected' : ''}>&lt;</option>
                <option value="gte" ${pair.condition2 === 'gte' ? 'selected' : ''}>≥</option>
                <option value="lte" ${pair.condition2 === 'lte' ? 'selected' : ''}>≤</option>
              </select>
            </div>
            <div class="col-md-3">
              <small class="text-muted d-block mb-1">
                ${pair.compareTag2 && pair.primaryTag ? `Highlight <strong>${pair.primaryTag.toUpperCase()}</strong> when met` : 'No tag selected'}
              </small>
            </div>
          </div>
        </div>
      </div>
    `).join('');
  }

  function updatePairData(id, field, value) {
    const pair = comparisonPairs.find(p => p.id === id);
    if (pair) {
      pair[field] = value;
      saveComparisonConfig();
    }
  }

  function saveComparisonConfig() {
    // Save to database via API
    fetch(REPORTS_CONFIG.saveComparisonUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ 
        config: comparisonPairs,
        logger_id: currentLoggerId  // Gửi logger_id hiện tại
      })
    })
    .then(response => response.json())
    .then(data => {
      if (!data.success) {
        console.error('Failed to save comparison config:', data.error);
      }
    })
    .catch(error => {
      console.error('Error saving comparison config:', error);
    });
  }

  function loadComparisonConfig() {
    // Load from database via API
    return fetch(REPORTS_CONFIG.loadComparisonUrl + '?logger_id=' + encodeURIComponent(currentLoggerId))
      .then(response => response.json())
      .then(data => {
        if (data.success && data.config) {
          comparisonPairs = data.config;
        } else {
          comparisonPairs = [];
        }
      })
      .catch(error => {
        console.error('Error loading comparison config:', error);
        comparisonPairs = [];
      });
  }

  function applyComparison() {
    // Clear all previous highlights
    document.querySelectorAll('#tbl tbody td').forEach(td => {
      td.style.backgroundColor = '';
    });

    const headers = Array.from(document.querySelectorAll('#tbl thead th'));
    let totalMatches = 0;
    let appliedConditions = 0;

    // Apply each comparison pair
    comparisonPairs.forEach(pair => {
      if (!pair.primaryTag) return;

      const primaryIndex = headers.findIndex(th => 
        th.textContent.trim().toLowerCase() === pair.primaryTag.toLowerCase()
      );
      if (primaryIndex === -1) return;

      const color = pair.color || pair.color1 || '#ffeb3b'; // Fallback for old config

      // Process comparison 1
      if (pair.compareTag1) {
        const compareIndex1 = headers.findIndex(th => 
          th.textContent.trim().toLowerCase() === pair.compareTag1.toLowerCase()
        );
        if (compareIndex1 !== -1) {
          appliedConditions++;
          totalMatches += applyComparisonCondition(primaryIndex, compareIndex1, pair.condition1, color);
        }
      }

      // Process comparison 2
      if (pair.compareTag2) {
        const compareIndex2 = headers.findIndex(th => 
          th.textContent.trim().toLowerCase() === pair.compareTag2.toLowerCase()
        );
        if (compareIndex2 !== -1) {
          appliedConditions++;
          totalMatches += applyComparisonCondition(primaryIndex, compareIndex2, pair.condition2, color);
        }
      }
    });

    // Update status
    const statusEl = document.getElementById('comparisonStatus');
    if (appliedConditions > 0) {
      statusEl.innerHTML = `<span class="badge bg-success">${totalMatches} cells matched</span> from ${appliedConditions} comparison conditions`;
    } else {
      statusEl.textContent = 'No conditions';
    }

    saveComparisonConfig();
  }

  function applyComparisonCondition(primaryIndex, compareIndex, condition, color) {
    let matchCount = 0;
    
    document.querySelectorAll('#tbl tbody tr').forEach(tr => {
      if (tr.style.display === 'none') return;
      if (tr.querySelector('td[colspan]')) return;

      const cells = tr.querySelectorAll('td');
      if (cells.length <= Math.max(primaryIndex, compareIndex)) return;

      const primaryValue = parseFloat(cells[primaryIndex].textContent.trim());
      const compareValue = parseFloat(cells[compareIndex].textContent.trim());
      
      if (isNaN(primaryValue) || isNaN(compareValue)) return;

      // So sánh compareValue với primaryValue theo điều kiện
      let matches = false;
      switch (condition) {
        case 'eq': matches = compareValue === primaryValue; break;
        case 'ne': matches = compareValue !== primaryValue; break;
        case 'gt': matches = compareValue > primaryValue; break;
        case 'lt': matches = compareValue < primaryValue; break;
        case 'gte': matches = compareValue >= primaryValue; break;
        case 'lte': matches = compareValue <= primaryValue; break;
      }

      if (matches) {
        // Tô màu ô của tag chính (primaryTag) khi thỏa điều kiện so sánh
        cells[primaryIndex].style.backgroundColor = color;
        cells[primaryIndex].style.transition = 'background-color 0.3s ease';
        matchCount++;
      }
    });

    return matchCount;
  }

  function clearComparison() {
    if (!confirm('Delete all comparison pairs?')) return;
    
    comparisonPairs = [];
    
    // Clear all highlights
    document.querySelectorAll('#tbl tbody td').forEach(td => {
      td.style.backgroundColor = '';
    });

    // Delete from database
    fetch(REPORTS_CONFIG.deleteComparisonUrl + '?logger_id=' + encodeURIComponent(currentLoggerId), {
      method: 'DELETE'
    })
    .then(response => response.json())
    .then(data => {
      if (!data.success) {
        console.error('Failed to delete comparison config:', data.error);
      }
    })
    .catch(error => {
      console.error('Error deleting comparison config:', error);
    });
    
    // Re-render
    addComparisonPair();
    
    document.getElementById('comparisonStatus').textContent = 'No conditions';
  }

  // Reapply comparison after filtering
  const originalFilterRows = filterRows;
  filterRows = function(q) {
    originalFilterRows(q);
    if (comparisonPairs.length > 0) {
      setTimeout(() => applyComparison(), 50);
    }
  };



  async function exportReport(kind) {
    const currentLoggerName = (document.querySelector('select[name="logger"] option:checked')?.textContent || '').trim();
    const filterSelect = document.getElementById('timeFilterSelect');
    const selectedFilter = filterSelect ? filterSelect.value : 'all';

    // Compute FROM/TO based on selected filter
    const inputsFrom = document.querySelector('input[name="from"]');
    const inputsTo = document.querySelector('input[name="to"]');

    const fmt = (d) => {
      if (!d) return '';
      // Format: DD/MM/YYYY HH:mm:ss (local)
      const pad = (n) => String(n).padStart(2, '0');
      return `${pad(d.getDate())}/${pad(d.getMonth()+1)}/${d.getFullYear()} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
    };

    const startOfDay = (d) => new Date(d.getFullYear(), d.getMonth(), d.getDate(), 0, 0, 0, 0);
    const endOfDay = (d) => new Date(d.getFullYear(), d.getMonth(), d.getDate(), 23, 59, 59, 999);

    let fromDateStr = inputsFrom?.value || '';
    let toDateStr = inputsTo?.value || '';

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
          // 'all' or unknown -> leave empty
          break;
      }
      fromDateStr = fromD ? fmt(fromD) : fromDateStr;
      toDateStr = toD ? fmt(toD) : toDateStr;
    }

    // Show loading indicator
    const loadingIndicator = document.getElementById('loadingIndicator');
    if (loadingIndicator) {
      loadingIndicator.innerHTML = '<div class="spinner-border spinner-border-sm me-2" role="status"></div><span>Preparing export...</span>';
      loadingIndicator.classList.add('active');
    }

    // Fetch ALL data from API for export
    const url = new URL(window.location.origin + '/reports/api/data');
    const params = new URLSearchParams(window.location.search);
    params.set('offset', 0);
    params.set('limit', 999999); // Get all data
    url.search = params.toString();

    let allData = [];
    let header = [];
    let originalColumns = []; // Keep original column names for data mapping
    
    try {
      const response = await fetch(url);
      const data = await response.json();
      
      if (data.success && data.items) {
        allData = data.items;
        originalColumns = data.columns || Array.from(document.querySelectorAll('#tbl thead th')).map(th => th.textContent.trim());
        header = originalColumns.map(col => col.toUpperCase()); // Display names in uppercase
      } else {
        alert('Failed to fetch data for export');
        if (loadingIndicator) loadingIndicator.classList.remove('active');
        return;
      }
    } catch (error) {
      console.error('Error fetching data for export:', error);
      alert('Error fetching data for export');
      if (loadingIndicator) loadingIndicator.classList.remove('active');
      return;
    }

    // Build body rows from fetched data
    const bodyRows = allData.map(row => {
      return originalColumns.map(col => {
        // Use original column names to get values from row object
        const value = row[col] !== undefined ? row[col] : row[col.toLowerCase()];
        return value !== null && value !== undefined ? String(value) : '';
      });
    });

    if (kind === 'xlsx') {
      // Use ExcelJS to allow styling header row (yellow fill)
      const workbook = new ExcelJS.Workbook();
      const sheet = workbook.addWorksheet('Report');

      // Title and period rows
      sheet.addRow(['DATALOGGER QUERY', currentLoggerName]);
      sheet.addRow(['FROM DATE:', fromDateStr || (selectedFilter === 'all' ? 'All Time' : '')]);
      sheet.addRow(['TO DATE:', toDateStr || (selectedFilter === 'all' ? 'All Time' : '')]);
      
      // Add comparison info if active - đặt ở A5, A6, ...
      if (comparisonPairs && comparisonPairs.length > 0) {
        // Hiển thị hướng điều kiện theo trục PrimaryTag (bên trái) so với CompareTag (bên phải)
        // Vì logic so sánh thực tế là compareTag ? primaryTag nên cần đảo chiều ký hiệu cho các phép bất đẳng thức
        const conditionSymbols = { eq: '=', ne: '≠', gt: '<', lt: '>', gte: '≤', lte: '≥' };
        
        // Dòng A5: Label "COMPARISON CONDITIONS:"
        const labelRow = sheet.addRow(['COMPARISON CONDITIONS:']);
        labelRow.font = { bold: true };
        labelRow.getCell(1).alignment = { horizontal: 'left', vertical: 'middle' };
        
        // Chuẩn bị danh sách các conditions
        const conditionData = [];
        
        comparisonPairs.forEach((pair, idx) => {
          if (pair.primaryTag) {
            const color = 'FF' + (pair.color || pair.color1 || '#ffeb3b').replace('#', '').toUpperCase();
            let parts = [];
            
            if (pair.compareTag1) {
              parts.push(`${pair.primaryTag.toUpperCase()} ${conditionSymbols[pair.condition1]} ${pair.compareTag1.toUpperCase()}`);
            }
            if (pair.compareTag2) {
              parts.push(`${pair.primaryTag.toUpperCase()} ${conditionSymbols[pair.condition2]} ${pair.compareTag2.toUpperCase()}`);
            }
            
            if (parts.length > 0) {
              const conditionText = `${idx + 1}. ${parts.join(' AND ')}`;
              conditionData.push({ text: conditionText, color: color });
            }
          }
        });
        
        // Merge tất cả các cột có sẵn để đảm bảo text hiển thị đầy đủ
        const columnsNeeded = header.length;
        
        // Vẽ tất cả các conditions với merge toàn bộ chiều rộng
        conditionData.forEach(data => {
          const conditionRow = sheet.addRow([data.text]);
          
          // Merge tất cả các cột từ A đến cột cuối
          if (columnsNeeded > 1) {
            sheet.mergeCells(conditionRow.number, 1, conditionRow.number, columnsNeeded);
          }
          
          // Set màu và style cho ô đầu tiên (ô đã merge)
          const cell = conditionRow.getCell(1);
          cell.fill = {
            type: 'pattern',
            pattern: 'solid',
            fgColor: { argb: data.color }
          };
          cell.font = { bold: true, color: { argb: 'FF000000' } };
          cell.alignment = { horizontal: 'left', vertical: 'middle', wrapText: true };
          cell.border = {
            top: { style: 'thin' },
            bottom: { style: 'thin' },
            left: { style: 'thin' },
            right: { style: 'thin' }
          };
        });
      }
      
      sheet.addRow([]);
      sheet.addRow([]);

      // Header
      const headerRow = sheet.addRow(header);
      const timestampIndex = header.findIndex((h) => h.toLowerCase() === 'timestamp');
      headerRow.eachCell((cell, colNumber) => {
        cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFD966' } }; // Yellow like alarm_events
        cell.font = { bold: true };
        cell.alignment = { horizontal: (colNumber - 1) === timestampIndex ? 'left' : 'center', vertical: 'middle' };
        cell.border = {
          top: { style: 'thin' }, bottom: { style: 'thin' }, left: { style: 'thin' }, right: { style: 'thin' }
        };
      });

      // Build comparison map for Excel export
      const comparisonMap = [];
      if (comparisonPairs && comparisonPairs.length > 0) {
        comparisonPairs.forEach(pair => {
          if (!pair.primaryTag) return;
          
          const primaryIndex = header.findIndex(h => h.toLowerCase() === pair.primaryTag.toLowerCase());
          if (primaryIndex === -1) return;
          
          const color = 'FF' + (pair.color || pair.color1 || '#ffeb3b').replace('#', '').toUpperCase();

          // Add comparison 1
          if (pair.compareTag1) {
            const compareIndex1 = header.findIndex(h => h.toLowerCase() === pair.compareTag1.toLowerCase());
            if (compareIndex1 !== -1) {
              comparisonMap.push({
                type: 'compare',
                primaryIndex: primaryIndex,
                compareIndex: compareIndex1,
                condition: pair.condition1,
                color: color
              });
            }
          }

          // Add comparison 2
          if (pair.compareTag2) {
            const compareIndex2 = header.findIndex(h => h.toLowerCase() === pair.compareTag2.toLowerCase());
            if (compareIndex2 !== -1) {
              comparisonMap.push({
                type: 'compare',
                primaryIndex: primaryIndex,
                compareIndex: compareIndex2,
                condition: pair.condition2,
                color: color
              });
            }
          }
        });
      }

      // Data rows with comparison highlighting
      bodyRows.forEach((rowData, rowIdx) => {
        const excelRow = sheet.addRow(rowData);
        
        // Set borders and alignment for all cells in this row
        excelRow.eachCell({ includeEmpty: true }, (cell, colNumber) => {
          cell.border = {
            top: { style: 'thin' },
            bottom: { style: 'thin' },
            left: { style: 'thin' },
            right: { style: 'thin' }
          };
          cell.alignment = { horizontal: (colNumber - 1) === timestampIndex ? 'left' : 'center', vertical: 'middle' };
        });
        
        // Apply highlighting based on comparison map
        comparisonMap.forEach(comp => {
          if (comp.type === 'compare') {
            // Apply comparison condition
            const primaryValue = parseFloat(rowData[comp.primaryIndex]);
            const compareValue = parseFloat(rowData[comp.compareIndex]);
            
            if (!isNaN(primaryValue) && !isNaN(compareValue)) {
              // So sánh compareValue với primaryValue theo điều kiện
              let matches = false;
              switch (comp.condition) {
                case 'eq': matches = compareValue === primaryValue; break;
                case 'ne': matches = compareValue !== primaryValue; break;
                case 'gt': matches = compareValue > primaryValue; break;
                case 'lt': matches = compareValue < primaryValue; break;
                case 'gte': matches = compareValue >= primaryValue; break;
                case 'lte': matches = compareValue <= primaryValue; break;
              }
              
              if (matches) {
                // Tô màu ô của tag chính khi thỏa điều kiện so sánh
                const cell = excelRow.getCell(comp.primaryIndex + 1);
                cell.fill = { 
                  type: 'pattern', 
                  pattern: 'solid', 
                  fgColor: { argb: comp.color } 
                };
              }
            }
          }
        });
      });

      // Auto width per column chỉ dựa trên tên cột
      sheet.columns = header.map(colName => {
        const textLen = (colName || '').length;
        // Công thức: (độ dài tên * 1.2) + padding, min 12, max 80
        let width = Math.min(80, Math.max(12, textLen * 1.2 + 3));
        // Timestamp cần rộng hơn để hiển thị đầy đủ ngày giờ
        if ((colName || '').toLowerCase() === 'timestamp') {
          width = Math.max(width, 22);
        }
        return { width };
      });

      workbook.xlsx.writeBuffer().then(buffer => {
        saveAs(new Blob([buffer]), `report_${REPORTS_CONFIG.currentLoggerName}.xlsx`);
        
        // Hide loading indicator
        if (loadingIndicator) {
          loadingIndicator.innerHTML = '<div class="spinner-border spinner-border-sm me-2" role="status"></div><span>Loading more data...</span>';
          loadingIndicator.classList.remove('active');
        }
      });
    } else {
      // CSV via SheetJS or manual build
      const rows = [];
      rows.push(['DATALOGGER QUERY', currentLoggerName]);
      rows.push(['FROM DATE:', fromDateStr || (selectedFilter === 'all' ? 'All Time' : '')]);
      rows.push(['TO DATE:', toDateStr || (selectedFilter === 'all' ? 'All Time' : '')]);
      rows.push([]); rows.push([]); rows.push([]);
      rows.push(header);
      bodyRows.forEach(r => rows.push(r));

      const csv = rows.map(r => r.map(v => {
        const s = (v ?? '').toString();
        return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
      }).join(',')).join('\n');
      const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
      const url = URL.createObjectURL(blob);
      const a = Object.assign(document.createElement('a'), {
        href: url,
        download: `report_${REPORTS_CONFIG.currentLoggerName}.csv`
      });
      document.body.appendChild(a); a.click(); a.remove();
      URL.revokeObjectURL(url);
      
      // Hide loading indicator
      if (loadingIndicator) {
        loadingIndicator.innerHTML = '<div class="spinner-border spinner-border-sm me-2" role="status"></div><span>Loading more data...</span>';
        loadingIndicator.classList.remove('active');
      }
    }
  }
</script>

/* ==========================================================================
   Base UI - Theme Toggle, Sidebar Toggle, Subdashboard Handlers
   Extracted from base.html inline <script> blocks
   ========================================================================== */

// Theme toggle system
(function () {
  const KEY = 'theme';
  const root = document.documentElement;

  // Use DOMContentLoaded to ensure button exists
  document.addEventListener('DOMContentLoaded', function () {
    const btn = document.getElementById('themeToggle');
    if (!btn) {
      console.warn('Theme toggle button not found!');
      return;
    }

    // Set initial icon based on current theme (already set in head)
    const setIcon = () => {
      const currentTheme = root.getAttribute('data-bs-theme');
      const newIcon = (currentTheme === 'dark') ? '🌤️' : '🌙';
      btn.textContent = newIcon;
    };

    // Set initial icon
    setIcon();

    // Handle theme toggle
    btn.addEventListener('click', () => {
      const cur = root.getAttribute('data-bs-theme');
      const next = (cur === 'dark') ? 'light' : 'dark';
      root.setAttribute('data-bs-theme', next);
      localStorage.setItem(KEY, next);
      setIcon();
    });
  });
})();

// Desktop sidebar toggle
(function () {
  const sidebar = document.getElementById('sidebar');
  const main = document.querySelector('.app-main');
  const btn = document.getElementById('sidebarToggle');
  if (!sidebar || !btn) return;

  btn.addEventListener('click', () => {
    sidebar.classList.toggle('collapsed');
    main.classList.toggle('full');
    console.log("Sidebar toggled");
  });
})();

// Handle subdashboard deletion and rename
document.addEventListener('DOMContentLoaded', function () {
  const deleteButtons = document.querySelectorAll('.delete-subdash-btn');
  const renameButtons = document.querySelectorAll('.rename-subdash-btn');

  // Handle delete
  deleteButtons.forEach(button => {
    button.addEventListener('click', function (e) {
      e.preventDefault();
      e.stopPropagation(); // Prevent navigation to subdashboard

      const subdashId = this.dataset.subdashId;
      const subdashName = this.dataset.subdashName;

      if (confirm(`Are you sure you want to delete subdashboard "${subdashName}"?\n\nThis action cannot be undone.`)) {
        // Show loading state
        this.innerHTML = '<i class="fas fa-spinner fa-spin"></i>';
        this.disabled = true;

        // Create and submit form for deletion
        const form = document.createElement('form');
        form.method = 'POST';
        form.action = `/subdash/${subdashId}/delete`;

        document.body.appendChild(form);
        form.submit();
      }
    });
  });

  // Handle rename
  renameButtons.forEach(button => {
    button.addEventListener('click', function (e) {
      e.preventDefault();
      e.stopPropagation();

      const subdashId = this.dataset.subdashId;
      const currentName = this.dataset.subdashName;

      const newName = prompt(`Rename subdashboard:`, currentName);

      if (newName !== null && newName.trim() !== '' && newName.trim() !== currentName) {
        // Show loading state
        this.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Renaming...';
        this.disabled = true;

        // Send AJAX request
        fetch(`/subdash/${subdashId}/rename`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
          },
          body: `new_name=${encodeURIComponent(newName.trim())}`
        })
          .then(response => response.json())
          .then(data => {
            if (data.success) {
              // Reload page to reflect changes
              window.location.reload();
            } else {
              alert('Error: ' + (data.error || 'Failed to rename subdashboard'));
              // Restore button
              this.innerHTML = '<i class="bi bi-pencil-square me-2"></i>Rename';
              this.disabled = false;
            }
          })
          .catch(error => {
            console.error('Rename error:', error);
            alert('Network error occurred while renaming subdashboard');
            // Restore button
            this.innerHTML = '<i class="bi bi-pencil-square me-2"></i>Rename';
            this.disabled = false;
          });
      }
    });
  });
});

/* ==========================================================================
   Notification System
   Extracted from base.html inline <script> block
   Manages alarm notifications with localStorage + server API sync
   ========================================================================== */

// Notification System with "unread" logic
window.NotificationSystem = {
  notifications: [],
  maxNotifications: 50,
  // Track unique keys to aggressively deduplicate across sources
  seenKeys: new Set(),

  init() {
    this.loadNotifications();
    this.loadServerNotifications(); // Load notifications from server
    this.bindEvents();
    this.startPeriodicCheck();
  },

  // Load notifications from server API
  async loadServerNotifications() {
    try {
      const response = await fetch('/api/notifications');
      if (response.ok) {
        const serverNotifications = await response.json();
        console.log('[Notifications] Loaded from server:', serverNotifications?.length ?? 0);
        // Preserve local transient notifications (no serverId)
        const localTransient = this.notifications.filter(n => !n.serverId);

        // Reset cache to mirror server truth
        this.notifications = [];
        this.seenKeys.clear();

        // Hydrate from server (newest first)
        const sorted = [...serverNotifications].sort((a, b) => {
          const aTime = new Date(a.created_at || 0).getTime();
          const bTime = new Date(b.created_at || 0).getTime();
          return bTime - aTime;
        });

        sorted.forEach(notification => {
          const createdAt = notification.created_at ? new Date(notification.created_at) : new Date();
          const alarmId = notification.alarm_event_id;
          const tagId = notification.tag_id;

          this.addNotification({
            serverId: notification.id,
            alarmId,
            tagId,
            title: notification.title || 'System Notification',
            message: notification.message || '',
            level: notification.level || 'Medium',
            timestamp: createdAt,
            read: notification.is_read || false,
            type: 'server'
          }, false); // false = don't show animation for existing notifications
        });

        // Re-append transient (local) notifications without serverId
        localTransient.forEach(n => this.addNotification(n, false));

        // Ensure newest notifications stay on top across all sources
        this.sortNotificationsDesc();

        // After syncing, update UI
        this.updateUI();
      }
    } catch (e) {
      console.error('Error loading server notifications:', e);
    }
  },

  // Sort notifications by timestamp (newest first)
  sortNotificationsDesc() {
    this.notifications.sort((a, b) => {
      const at = new Date(a.timestamp || 0).getTime();
      const bt = new Date(b.timestamp || 0).getTime();
      return bt - at;
    });
  },

  bindEvents() {
    // Mark all as read when bell is clicked (dropdown shown)
    document.getElementById('notificationBell')?.addEventListener('click', () => {
      this.markAllAsRead();
      // Re-bind clearAllNotifications every time dropdown is shown (fix for dynamic DOM)
      setTimeout(() => {
        const clearBtn = document.getElementById('clearAllNotifications');
        if (clearBtn) {
          clearBtn.onclick = async () => {
            await this.clearOnServer();
            this.clearAll();
          };
        }
      }, 100);
    });

    // Also bind clear button on init
    const clearBtnInit = document.getElementById('clearAllNotifications');
    if (clearBtnInit) {
      clearBtnInit.onclick = async () => {
        await this.clearOnServer();
        this.clearAll();
      };
    }

    // Refresh from server when window/tab gains focus
    window.addEventListener('focus', () => {
      this.loadServerNotifications();
    });
  },

  // Clear everything on UI
  clearAll() {
    this.notifications = [];
    this.saveNotifications();
    this.updateUI();
  },

  // Tell server to dismiss all notifications for this user
  async clearOnServer() {
    try {
      await fetch('/api/notifications/clear', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      });
    } catch (e) {
      console.error('Error clearing notifications on server:', e);
    }
  },

  addNotification(notification, showAnimation = true) {
    // Add timestamp if not provided
    notification.timestamp = notification.timestamp || new Date();
    notification.id = notification.id || Date.now() + Math.random();

    // For server notifications, respect the is_read status
    if (notification.type === 'server' && notification.read) {
      notification.read = true;
    } else {
      notification.read = notification.read || false;
    }

    // Build a stable fingerprint for dedup (favor seconds precision) independent of serverId
    const createdAt = notification.timestamp instanceof Date ? notification.timestamp : new Date(notification.timestamp);
    const sec = Math.floor(createdAt.getTime() / 1000);
    const keyParts = [
      notification.alarmId || '',           // prefer alarm/event identity
      notification.tagId || '',             // fallback to tag identity
      sec,                                  // tolerate ms drift
      (notification.message || '').toString().trim().toLowerCase() // stable content
    ];
    const key = keyParts.join('|');
    notification._key = key;
    if (this.seenKeys.has(key)) {
      return; // already present
    }

    // Add to beginning of array (newest first)
    this.notifications.unshift(notification);
    this.seenKeys.add(key);

    // Limit number of notifications
    if (this.notifications.length > this.maxNotifications) {
      this.notifications = this.notifications.slice(0, this.maxNotifications);
    }

    this.saveNotifications();
    this.updateUI();

    // Only show animation for new notifications (not server loaded ones)
    if (showAnimation && notification.type !== 'server') {
      this.showBellAnimation();
    }
  },

  async markAllAsRead() {
    // Mark local notifications as read
    this.notifications.forEach(n => n.read = true);

    // Send request to mark server notifications as read
    try {
      await fetch('/api/notifications/mark-read', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        }
      });
    } catch (e) {
      console.error('Error marking notifications as read:', e);
    }

    this.saveNotifications();
    this.updateUI();
  },

  removeNotification(id) {
    this.notifications = this.notifications.filter(n => n.id !== id);
    this.saveNotifications();
    this.updateUI();
  },

  updateUI() {
    const badge = document.getElementById('notificationBadge');
    const list = document.getElementById('notificationList');
    if (!badge || !list) return;

    // Count unread notifications
    const unreadCount = this.notifications.filter(n => !n.read).length;
    if (unreadCount > 0) {
      const displayCount = unreadCount > 10 ? '10+' : unreadCount;
      badge.textContent = displayCount;
      badge.style.display = 'block';
    } else {
      badge.style.display = 'none';
    }

    // Update list
    if (this.notifications.length === 0) {
      list.innerHTML = `
        <div class="dropdown-item-text text-muted text-center py-3">
          <i class="bi bi-bell-slash"></i><br>
          No active alarms
        </div>
      `;
    } else {
      list.innerHTML = this.notifications.map(n => this.renderNotification(n)).join('');
    }
  },

  renderNotification(notification) {
    const timeStr = this.formatTime(notification.timestamp);
    const levelClass = this.getLevelClass(notification.level);
    const levelIcon = this.getLevelIcon(notification.level);
    const unreadDot = !notification.read ? '<span style="color:red;font-size:1.2em;vertical-align:middle;">•</span>' : '';

    // Determine background color based on level
    let bgStyle = '';
    if (notification.level === 'Critical') {
      // Red background for High/Critical alarms
      bgStyle = 'background-color: rgba(220, 38, 38, 0.15); border-left: 3px solid #dc2626;';
    } else if (notification.level === 'Warning') {
      // Yellow background for Low/Warning alarms
      bgStyle = 'background-color: rgba(245, 158, 11, 0.15); border-left: 3px solid #f59e0b;';
    } else if (notification.message && /activated/i.test(notification.message)) {
      // Keep existing logic for activated/cleared messages
      bgStyle = 'background-color: rgba(220, 38, 38, 0.1);';
    } else if (notification.message && /cleared/i.test(notification.message)) {
      bgStyle = 'background-color: rgba(34, 197, 94, 0.1);';
    }

    return `
      <div class="dropdown-item notification-item" data-id="${notification.id}" style="${bgStyle}">
        <div class="d-flex align-items-start gap-2">
          <div class="flex-shrink-0">
            <i class="bi ${levelIcon} text-${levelClass}"></i> ${unreadDot}
          </div>
          <div class="flex-grow-1 min-width-0">
            <div class="fw-semibold text-truncate">${notification.title || 'Alarm Alert'}</div>
            <div class="small text-muted mb-1">${notification.message || ''}</div>
            <div class="small text-muted">
              <i class="bi bi-clock"></i> ${timeStr}
              ${notification.tagName ? `• <i class="bi bi-tag"></i> ${notification.tagName}` : ''}
            </div>
          </div>
        </div>
      </div>
    `;
  },

  getLevelClass(level) {
    const levels = {
      'Critical': 'danger',
      'High': 'warning',
      'Medium': 'info',
      'Low': 'secondary'
    };
    return levels[level] || 'secondary';
  },

  getLevelIcon(level) {
    const icons = {
      'Critical': 'bi-exclamation-triangle-fill',
      'High': 'bi-exclamation-triangle',
      'Medium': 'bi-info-circle',
      'Low': 'bi-info-circle-fill'
    };
    return icons[level] || 'bi-bell';
  },

  formatTime(timestamp) {
    const date = new Date(timestamp);
    // Display absolute time: dd/mm/yyyy HH:MM:ss
    const pad = n => n.toString().padStart(2, '0');
    const day = pad(date.getDate());
    const month = pad(date.getMonth() + 1);
    const year = date.getFullYear();
    const hour = pad(date.getHours());
    const min = pad(date.getMinutes());
    const sec = pad(date.getSeconds());
    return `${day}/${month}/${year} ${hour}:${min}:${sec}`;
  },

  showBellAnimation() {
    const bell = document.querySelector('#notificationBell i');
    if (bell) {
      bell.classList.add('bell-shake');
      setTimeout(() => {
        bell.classList.remove('bell-shake');
      }, 1000);
    }
  },

  saveNotifications() {
    try {
      localStorage.setItem('alarmNotifications', JSON.stringify(this.notifications));
    } catch (e) {
      console.warn('Could not save notifications to localStorage:', e);
    }
  },

  loadNotifications() {
    try {
      const saved = localStorage.getItem('alarmNotifications');
      if (saved) {
        this.notifications = JSON.parse(saved);
        // Convert timestamp strings back to Date objects
        this.notifications.forEach(n => {
          if (typeof n.timestamp === 'string') {
            n.timestamp = new Date(n.timestamp);
          }
          if (typeof n.read === 'undefined') n.read = false;
          // Rebuild keys using normalized scheme (ignore serverId)
          const createdAt = n.timestamp instanceof Date ? n.timestamp : new Date(n.timestamp);
          const sec = Math.floor(createdAt.getTime() / 1000);
          const keyParts = [
            n.alarmId || '',
            n.tagId || '',
            sec,
            (n.message || '').toString().trim().toLowerCase()
          ];
          n._key = keyParts.join('|');
          if (n._key) this.seenKeys.add(n._key);
        });
      }
    } catch (e) {
      this.notifications = [];
      this.seenKeys.clear();
    }
    // Keep newest on top after loading from localStorage
    this.sortNotificationsDesc();
    this.updateUI();
  },

  async checkForNewAlarms() {
    try {
      const response = await fetch('/api/alarms/recent');
      if (response.ok) {
        const alarms = await response.json();
        alarms.forEach(alarm => {
          // Check if we already have this alarm
          const exists = this.notifications.some(n =>
            n.alarmId === alarm.id ||
            (n.tagId === alarm.tag_id && n.timestamp && new Date(n.timestamp).getTime() === new Date(alarm.created_at).getTime())
          );
          if (!exists) {
            this.addNotification({
              alarmId: alarm.id,
              tagId: alarm.tag_id,
              title: alarm.title || 'Alarm Triggered',
              message: alarm.message,
              level: alarm.level,
              tagName: alarm.tag_name,
              timestamp: new Date(alarm.created_at),
              read: false
            });
          }
        });
      }
    } catch (e) { }
  },

  // Periodically refresh notifications from server to avoid stale UI on subpages/refresh
  startPeriodicCheck() {
    if (this._interval) return;
    this._interval = setInterval(() => {
      this.loadServerNotifications();
    }, 15000); // every 15 seconds
  }
};

// Initialize notification system when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
  NotificationSystem.init();
  // Listen for real-time alarm events via Socket.IO
  if (typeof socket !== 'undefined') {
    socket.on('alarm_event', function (data) {
      console.log('Received alarm_event via Socket.IO:', data);
      const alarmId = data.id || data.alarm_event_id;
      const tagId = data.tag_id;
      const ts = data.created_at || data.ts || new Date().toISOString();
      const dt = new Date(ts);
      const ageSec = Math.floor((Date.now() - dt.getTime()) / 1000);
      // Drop stale socket events (e.g., backlog replay) older than 120s
      if (ageSec > 120) {
        console.log('[Notifications] Skip stale socket alarm (>', ageSec, 's):', data);
        return;
      }
      NotificationSystem.addNotification({
        id: Date.now() + Math.random(),
        serverId: undefined,
        alarmId,
        tagId,
        title: data.title || 'Alarm Alert',
        message: data.message || 'Alarm condition detected',
        level: data.level || 'Critical',
        timestamp: dt,
        status: data.status || 'Active',
        read: false
      });
    });

    // Quad alarm notifications are now handled directly in detail.html
    // This avoids duplicate notifications and allows proper group name display
  }
});

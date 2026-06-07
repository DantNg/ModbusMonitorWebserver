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
  // Track timestamp-seconds of notifications delivered via socket to suppress server duplicates
  _socketEventSecs: new Map(),

  _trackSocketEvent(timestamp) {
    const sec = Math.floor(new Date(timestamp).getTime() / 1000);
    this._socketEventSecs.set(sec, Date.now());
    // Cleanup entries older than 60s
    const cutoff = Date.now() - 60000;
    for (const [k, v] of this._socketEventSecs) {
      if (v < cutoff) this._socketEventSecs.delete(k);
    }
  },

  _wasDeliveredViaSocket(timestamp) {
    const sec = Math.floor(new Date(timestamp).getTime() / 1000);
    return this._socketEventSecs.has(sec);
  },

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
      if (response.status === 401) {
        // No authenticated user yet; silently keep local notifications only.
        return;
      }
      if (response.ok) {
        const serverNotifications = await response.json();
        console.log('[Notifications] Loaded from server:', serverNotifications?.length ?? 0);
        // Preserve RECENT local transient notifications (no serverId, < 30s old).
        // Older transient notifications are dropped because the server sync
        // will provide the authoritative version (avoids stale duplicates).
        const now = Date.now();
        const localTransient = this.notifications.filter(n => {
          if (n.serverId) return false;
          const age = now - new Date(n.timestamp || 0).getTime();
          return age < 60000; // keep last 60 seconds (matches socket suppression window)
        });

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

          // Skip server duplicates only for INCOMING events delivered via socket.
          // OUTGOING should still be accepted from server for persistence.
          if (notification.event_type !== 'OUTGOING' && this._wasDeliveredViaSocket(createdAt)) {
            return;
          }

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
            type: 'server',
            event_type: notification.event_type
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
            const ok = await this.clearOnServer();
            if (ok) {
              this.clearAll();
            } else {
              // Keep UI consistent with server state when clear request fails
              this.loadServerNotifications();
            }
          };
        }
      }, 100);
    });

    // Also bind clear button on init
    const clearBtnInit = document.getElementById('clearAllNotifications');
    if (clearBtnInit) {
      clearBtnInit.onclick = async () => {
        const ok = await this.clearOnServer();
        if (ok) {
          this.clearAll();
        } else {
          // Keep UI consistent with server state when clear request fails
          this.loadServerNotifications();
        }
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
    this.seenKeys.clear();
    this._socketEventSecs.clear();
    try {
      localStorage.removeItem('alarmNotifications');
    } catch (e) {
      // Fallback to normal save path if remove fails
      this.saveNotifications();
    }
    this.updateUI();
  },

  // Tell server to dismiss all notifications for this user
  async clearOnServer() {
    try {
      const response = await fetch('/api/notifications/clear', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      });
      if (response.status === 401) {
        return true;
      }
      if (!response.ok) {
        console.error('Failed to clear notifications on server. Status:', response.status);
        return false;
      }
      return true;
    } catch (e) {
      console.error('Error clearing notifications on server:', e);
      return false;
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
    const entityId = notification.tagId || notification.alarmId || '';
    const state = (notification.status || notification.event_type || '').toString().trim().toLowerCase();
    const keyParts = [
      entityId,
      sec,
      (notification.message || '').toString().trim().toLowerCase(),
      state
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

    // Determine background color based on level and status
    let bgStyle = '';
    const _isOutgoing = (notification.status === 'OUTGOING' || notification.event_type === 'OUTGOING');
    if (_isOutgoing) {
      // Green background for cleared/outgoing alarms
      bgStyle = 'background-color: rgba(34, 197, 94, 0.12); border-left: 3px solid #16a34a;';
    } else if (notification.level === 'Critical') {
      bgStyle = 'background-color: rgba(220, 38, 38, 0.15); border-left: 3px solid #dc2626;';
    } else if (notification.level === 'High' || notification.level === 'Warning') {
      bgStyle = 'background-color: rgba(245, 158, 11, 0.15); border-left: 3px solid #f59e0b;';
    } else if (notification.level === 'Medium') {
      bgStyle = 'background-color: rgba(59, 130, 246, 0.12); border-left: 3px solid #3b82f6;';
    } else if (notification.message && /activated/i.test(notification.message)) {
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
        let parsed = JSON.parse(saved);
        // Convert timestamp strings back to Date objects
        parsed.forEach(n => {
          if (typeof n.timestamp === 'string') {
            n.timestamp = new Date(n.timestamp);
          }
          if (typeof n.read === 'undefined') n.read = false;
        });

        // Purge stale transient (client-side) notifications older than 60s.
        // These were created by socket handlers and should have been replaced
        // by server-synced versions via loadServerNotifications().
        const now = Date.now();
        parsed = parsed.filter(n => {
          if (n.serverId) return true; // keep server notifications
          const age = now - new Date(n.timestamp || 0).getTime();
          return age < 60000; // discard transient > 60s
        });

        this.notifications = parsed;
        // Rebuild seenKeys
        this.notifications.forEach(n => {
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
    }, 10000); // every 10 seconds (reduced from 15s for faster sync)
  }
};

// Initialize notification system when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
  NotificationSystem.init();
  // Listen for real-time alarm events via Socket.IO
  if (typeof socket !== 'undefined' && socket !== null) {
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

      // Show clear notification from socket (server API filters OUTGOING).
      if (data.status === 'OUTGOING' || data.event_type === 'OUTGOING') {
        NotificationSystem.addNotification({
          id: Date.now() + Math.random(),
          serverId: undefined,
          alarmId,
          tagId,
          title: data.title || 'Alarm Cleared',
          message: data.message || 'Alarm condition cleared',
          level: data.level || 'Low',
          timestamp: dt,
          status: 'OUTGOING',
          read: false
        });
        console.log('[Notifications] Added OUTGOING alarm_event to bell:', data);
        return;
      }

      // Track this event's timestamp to suppress duplicate server notification
      NotificationSystem._trackSocketEvent(ts);
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

    // Listen for quad alarm events - update bell INSTANTLY without HTTP roundtrip
    socket.on('quad_alarm_event', function (data) {
      console.log('[Notifications] Received quad_alarm_event via Socket.IO:', data);
      const status = data.status; // 'INCOMING' or 'OUTGOING'
      const quadId = data.quad_id;
      const column = data.column;
      const alarmType = data.alarm_type; // 'High' or 'Low'
      const ts = data.timestamp || new Date().toISOString();
      const dt = new Date(ts);

      // Drop stale events older than 120s
      const ageSec = Math.floor((Date.now() - dt.getTime()) / 1000);
      if (ageSec > 120) {
        console.log('[Notifications] Skip stale quad alarm (>', ageSec, 's)');
        return;
      }

      // Dùng display_value từ backend (alarm_strategies.py build_event) — không format lại FE
      const tag1Val = data.tag1_display !== undefined && data.tag1_display !== null
        ? (data.tag1_display || 'N/A')
        : (data.tag1_value !== null && data.tag1_value !== undefined ? String(data.tag1_value) : 'N/A');
      const tag2Val = data.tag2_display !== undefined && data.tag2_display !== null
        ? (data.tag2_display || 'N/A')
        : (data.tag2_value !== null && data.tag2_value !== undefined ? String(data.tag2_value) : 'N/A');
      const threshold = data.threshold_display !== undefined && data.threshold_display !== null
        ? (data.threshold_display || 'N/A')
        : (data.threshold !== null && data.threshold !== undefined ? String(data.threshold) : 'N/A');
      const operator = data.operator || '>';
      const columnLabel = column === 'left' ? 'Left' : 'Right';

      // Try to get actual card title from DOM if on detail page, else use data from backend
      let cardTitle = `Quad #${quadId}`;
      let columnTitle = columnLabel;
      const quadCardEl = document.querySelector(`.quad-tag-card[data-quad-id="${quadId}"]`);
      if (quadCardEl) {
        const headerEl = quadCardEl.querySelector('.quad-card-header-title, .quad-card-title');
        if (headerEl && headerEl.textContent.trim()) {
          cardTitle = headerEl.textContent.trim();
        }
        const subCardEl = quadCardEl.querySelector(`.quad-sub-card[data-column="${column}"]`);
        if (subCardEl) {
          const subTitleEl = subCardEl.querySelector('.quad-sub-card-title');
          if (subTitleEl && subTitleEl.textContent.trim()) {
            columnTitle = subTitleEl.textContent.trim();
          }
        }
      }

      // Avoid duplicate bell entries for INCOMING: keep server-synced entry as source of truth.
      // OUTGOING is filtered by server API, so we show it directly from socket.
      if (status === 'INCOMING') {
        console.log('[Notifications] INCOMING quad_alarm_event received; waiting for server sync:', data);
        // Pull server notifications soon so user does not need to wait for the 10s interval.
        setTimeout(() => NotificationSystem.loadServerNotifications(), 400);
      } else {
        // NotificationSystem.addNotification({
        //   id: Date.now() + Math.random(),
        //   serverId: undefined,
        //   tagId: data.tag_id || null,
        //   title: columnTitle,
        //   message: `${cardTitle} - ${columnTitle} - Alarm cleared ${operator} ${threshold}`,
        //   level: 'Low',
        //   timestamp: dt,
        //   status: 'OUTGOING',
        //   read: false
        // });
        console.log('[Notifications] Added OUTGOING quad_alarm_event to bell:', data);
      }
    });
  }
});

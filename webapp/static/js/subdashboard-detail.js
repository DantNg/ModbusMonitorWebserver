/* ==========================================================================
   Subdashboard Detail - Main JavaScript
   Extracted from subdashboards/detail.html inline <script>
   ========================================================================== */

// Read configuration passed from Jinja2 template
const currentSubdashId = window.SUBDASH_CONFIG.subdashId;
const currentGroup = window.SUBDASH_CONFIG.currentGroup;

// Auto-refresh page at 3:00 AM daily to keep UI fresh after long uptime
(function scheduleAutoRefreshAt3AM() {
  function msUntil3AM() {
    const now = new Date();
    const target = new Date(now);
    target.setHours(3, 0, 0, 0);
    // If 3 AM already passed today, schedule for tomorrow
    if (target <= now) {
      target.setDate(target.getDate() + 1);
    }
    return target - now;
  }

  let refreshTimer = setTimeout(function doRefresh() {
    // console.log('[AutoRefresh] 3:00 AM reached — reloading page');
    window.location.reload();
  }, msUntil3AM());

  // Cleanup on page unload
  window.addEventListener('beforeunload', function () {
    clearTimeout(refreshTimer);
  });

  // console.log(`[AutoRefresh] Scheduled page reload in ${Math.round(msUntil3AM() / 60000)} minutes (at 3:00 AM)`);
})();

  // Set subdashboard ID as data attribute for use in quad_conditions.js
  document.addEventListener('DOMContentLoaded', function() {
    document.body.setAttribute('data-subdash-id', currentSubdashId);
    window.currentSubdashId = currentSubdashId;
  });
  
  // Wrap everything in DOMContentLoaded to ensure buttons exist
  document.addEventListener('DOMContentLoaded', function () {

    // App globals for subdashboard
    if (!window.App) {
      window.App = {};
    }

    const subdashApp = {
      changeGroup: function (groupId) {
        // Leave the previous socket room before switching group
        if (typeof socket !== 'undefined' && socket !== null) {
          socket.emit('leave', { room: `subdashboard_${currentSubdashId}` });
        }

        // Navigate to the new group
        const url = new URL(window.location);
        url.searchParams.set('group', groupId);
        window.location.href = url.toString();
      }
    };

    // Do not overwrite App; just extend it
    Object.assign(window.App, subdashApp);

    // Set meter bar widths immediately based on device_status
    function initializeMeterBars() {
      const allMeterBars = document.querySelectorAll('[id^="tag-bar-"]');
      allMeterBars.forEach(barEl => {
        const deviceStatus = barEl.getAttribute('data-device-status');
        if (deviceStatus === 'connected') {
          barEl.style.width = '100%';
        } else if (deviceStatus === 'disconnected') {
          barEl.style.width = '0%';
        } else {
          barEl.style.width = '0%';
        }
      });
    }
    initializeMeterBars();

    // Helper: dim a tag immediately without overwriting its current value text
    function dimTagInitial(tagId) {
      const valEl = document.getElementById('tag-val-' + tagId);
      const tsEl = document.getElementById('tag-ts-' + tagId);
      const barEl = document.getElementById('tag-bar-' + tagId);
      if (valEl) {
        valEl.style.opacity = '0.5';
        valEl.style.color = '#6c757d';
      }
      if (tsEl) {
        tsEl.textContent = 'Device offline';
        tsEl.style.color = '#dc3545';
        tsEl.style.fontWeight = 'bold';
      }
      if (barEl) {
        barEl.style.width = '0%';
        barEl.classList.remove('device-connected', 'device-unknown');
        barEl.classList.add('device-disconnected', 'tag-inactive');
      }
    }

    // Immediately gray-out tags for devices that are offline on page load
    (function initializeOfflineTags() {
      const bars = document.querySelectorAll('[id^="tag-bar-"]');
      bars.forEach(barEl => {
        const deviceStatus = barEl.getAttribute('data-device-status');
        if (deviceStatus === 'disconnected') {
          const tagIdStr = (barEl.id || '').replace('tag-bar-', '');
          const tagId = parseInt(tagIdStr);
          if (!isNaN(tagId)) {
            // Dim without overwriting existing displayed value
            dimTagInitial(tagId);
            // Force quad values for this tag to 0 at load if offline
            document.querySelectorAll(`[id^="quad-tag-val-"][id$="-${tagId}"]`).forEach(el => {
              el.textContent = '0';
            });
          }
        }
      });
    })();

    // currentSubdashId and currentGroup are defined at file top from SUBDASH_CONFIG

    // Initialize App for subdashboard
    // App.startDashboard('/api/tags?subdash=' + currentSubdashId);

    // Write functionality for subdashboard tags
    function handleWriteValue(tagId, tagName) {
      Swal.fire({
        title: `Write Value to ${tagName}`,
        input: 'number',
        inputLabel: 'Enter value:',
        inputPlaceholder: 'Enter new value',
        showCancelButton: true,
        confirmButtonText: 'Write',
        cancelButtonText: 'Cancel',
        inputValidator: (value) => {
          if (!value && value !== '0') {
            return 'Please enter a value';
          }
        }
      }).then((result) => {
        if (!result.isConfirmed) return;

        const value = result.value;

        // Show loading
        Swal.fire({
          title: 'Writing...',
          text: `Setting ${tagName} to ${value}`,
          allowOutsideClick: false,
          didOpen: () => {
            Swal.showLoading();
          }
        });

        // Send write command via Socket.IO
        if (typeof socket !== 'undefined' && socket !== null && socket.connected) {
          // Prepare write command payload
          const writeCommand = {
            tag_id: parseInt(tagId),
            tag_name: tagName,
            value: parseFloat(value),
            timestamp: new Date().toISOString(),
            source: 'subdashboard'
          };

          // Emit write command
          socket.emit('modbus_write_command', writeCommand);

          // Listen for write response (one-time listener)
          socket.once('modbus_write_response', (response) => {
            Swal.close();
            if (response.success && response.tag_id === parseInt(tagId)) {
              Swal.fire({
                icon: 'success',
                title: 'Success!',
                text: `Successfully wrote ${value} to ${tagName}`,
                timer: 2000,
                showConfirmButton: false
              });
            } else {
              Swal.fire({
                icon: 'error',
                title: 'Write Failed',
                text: response.error || 'Failed to write value'
              });
            }
          });

          // Timeout fallback
          setTimeout(() => {
            if (Swal.isLoading()) {
              Swal.close();
              Swal.fire({
                icon: 'warning',
                title: 'Timeout',
                text: 'Write command timeout - please check if value was written'
              });
            }
          }, 10000); // 10 second timeout

        } else {
          Swal.close();
          Swal.fire({
            icon: 'error',
            title: 'Connection Error',
            text: 'Socket.IO connection not available'
          });
        }
      });
    }

    document.addEventListener('click', function (e) {
      if (e.target.closest('.write-btn')) {
        const btn = e.target.closest('.write-btn');
        const tagId = btn.dataset.tagId;
        const tagName = btn.dataset.tagName;
        handleWriteValue(tagId, tagName);
      }
    });

    document.addEventListener('click', function (e) {
      const btn = e.target.closest('.write-quad-tag-btn');
      if (!btn) return;
      e.preventDefault();
      const tagId = btn.dataset.tagId;
      const tagName = btn.dataset.tagName;
      handleWriteValue(tagId, tagName);
    });

    // Handle Add Tag form submission
    const addTagForm = document.getElementById('addTagForm');
    if (addTagForm) {
      addTagForm.addEventListener('submit', function (e) {
        e.preventDefault();

        const formData = new FormData(addTagForm);
        const tagId = formData.get('tag_id');
        const targetGroup = formData.get('target_group');
        const newGroupName = formData.get('new_group_name');

        if (!tagId) {
          Swal.fire({
            icon: 'error',
            title: 'Error',
            text: 'Please select a tag'
          });
          return;
        }

        // Show loading
        Swal.fire({
          title: 'Adding Tag...',
          text: 'Adding tag to subdashboard',
          allowOutsideClick: false,
          didOpen: () => {
            Swal.showLoading();
          }
        });

        // Send AJAX request
        fetch(`/subdash/${currentSubdashId}/add_tag`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
          },
          body: `tag_id=${tagId}&target_group=${targetGroup}&new_group_name=${newGroupName}`
        })
          .then(response => response.json())
          .then(data => {
            if (data.success) {
              Swal.fire({
                icon: 'success',
                title: 'Success!',
                text: data.message || 'Tag added successfully',
                timer: 2000,
                showConfirmButton: false
              }).then(() => {
                // Close modal and reload page
                const modal = bootstrap.Modal.getInstance(document.getElementById('addTagModal'));
                modal.hide();
                window.location.href = window.location.href + '?t=' + Date.now();
              });
            } else {
              Swal.fire({
                icon: 'error',
                title: 'Error',
                text: data.error || 'Failed to add tag'
              });
            }
          })
          .catch(error => {
            console.error('Add tag error:', error);
            Swal.fire({
              icon: 'error',
              title: 'Error',
              text: 'Failed to add tag'
            });
          });
      });
    }

    // Handle Add Group form submission
    const addGroupForm = document.getElementById('addGroupForm');
    if (addGroupForm) {
      addGroupForm.addEventListener('submit', function (e) {
        e.preventDefault();

        const formData = new FormData(addGroupForm);
        const groupName = formData.get('group_name');
        const selectedTags = Array.from(document.getElementById('group_tags').selectedOptions).map(option => option.value);

        if (!groupName || selectedTags.length === 0) {
          Swal.fire({
            icon: 'error',
            title: 'Error',
            text: 'Please enter group name and select at least one tag'
          });
          return;
        }

        // Show loading
        Swal.fire({
          title: 'Creating Group...',
          text: 'Creating new tag group',
          allowOutsideClick: false,
          didOpen: () => {
            Swal.showLoading();
          }
        });

        // Prepare form data
        const formBody = `group_name=${encodeURIComponent(groupName)}&${selectedTags.map(tag => `group_tags=${tag}`).join('&')}`;

        // Send AJAX request
        fetch(`/subdash/${currentSubdashId}/add_group`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
          },
          body: formBody
        })
          .then(response => response.json())
          .then(data => {
            if (data.success) {
              Swal.fire({
                icon: 'success',
                title: 'Success!',
                text: data.message || `Group "${groupName}" created successfully`,
                timer: 2000,
                showConfirmButton: false
              }).then(() => {
                // Close modal and reload page
                const modal = bootstrap.Modal.getInstance(document.getElementById('addGroupModal'));
                modal.hide();
                window.location.href = window.location.href + '?t=' + Date.now();
              });
            } else {
              Swal.fire({
                icon: 'error',
                title: 'Error',
                text: data.error || 'Failed to create group'
              });
            }
          })
          .catch(error => {
            console.error('Add group error:', error);
            Swal.fire({
              icon: 'error',
              title: 'Error',
              text: 'Failed to create group'
            });
          });
      });
    }

    // Handle Add Quad Tag form submission
    const addQuadTagForm = document.getElementById('addQuadTagForm');
    if (addQuadTagForm) {
      addQuadTagForm.addEventListener('submit', function (e) {
        e.preventDefault();
        console.log('Quad tag form submitted');
        
        const tag1Id = document.getElementById('tag1_select').value;
        const tag2Id = document.getElementById('tag2_select').value;
        const tag3Id = document.getElementById('tag3_select').value;
        const tag4Id = document.getElementById('tag4_select').value;
        const groupId = document.getElementById('quad_target_group').value;
        const newGroupName = document.getElementById('new_quad_group_name').value.trim();
        const cardTitle = document.getElementById('quad_card_title').value.trim();
        const leftTitle = document.getElementById('quad_left_title').value.trim();
        const rightTitle = document.getElementById('quad_right_title').value.trim();
        
        console.log('Selected tags:', { tag1Id, tag2Id, tag3Id, tag4Id });
        console.log('Group info:', { groupId, newGroupName });
        
        // Validate that all 4 tags are selected
        if (!tag1Id || !tag2Id || !tag3Id || !tag4Id) {
          console.log('Validation failed: not all tags selected');
          showQuadError('Please select all 4 tags');
          return;
        }
        
        // Validate that all tags are different
        const tagIds = [tag1Id, tag2Id, tag3Id, tag4Id];
        const uniqueTagIds = [...new Set(tagIds)];
        if (uniqueTagIds.length !== 4) {
          showQuadError('All 4 tags must be different');
          return;
        }
        
        // Validate group selection
        if (!groupId && !newGroupName) {
          showQuadError('Please select a group or enter a new group name');
          return;
        }
        
        // Show loading
        Swal.fire({
          title: 'Adding Quad Tag Card...',
          allowOutsideClick: false,
          didOpen: () => {
            Swal.showLoading();
          }
        });
        
        // Prepare form data
        const formData = new FormData();
        formData.append('tag1_id', tag1Id);
        formData.append('tag2_id', tag2Id);
        formData.append('tag3_id', tag3Id);
        formData.append('tag4_id', tag4Id);
        if (groupId) {
          formData.append('group_id', groupId);
        }
        if (newGroupName) {
          formData.append('new_group_name', newGroupName);
        }
        // Add quad card titles
        if (cardTitle) {
          formData.append('card_title', cardTitle);
        }
        if (leftTitle) {
          formData.append('left_title', leftTitle);
        }
        if (rightTitle) {
          formData.append('right_title', rightTitle);
        }
        
        // Send request
        console.log('Sending request to:', `/subdash/${currentSubdashId}/add_quad_tag`);
        fetch(`/subdash/${currentSubdashId}/add_quad_tag`, {
          method: 'POST',
          body: formData
        })
        .then(response => {
          console.log('Response status:', response.status);
          if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
          }
          return response.json();
        })
        .then(data => {
          console.log('Response data:', data);
          if (data.success) {
            Swal.fire({
              title: 'Success!',
              text: 'Quad tag card added successfully',
              icon: 'success',
              timer: 2000
            }).then(() => {
              location.reload();
            });
          } else {
            Swal.fire({
              title: 'Error',
              text: data.message || data.error || 'Failed to add quad tag card',
              icon: 'error'
            });
          }
        })
        .catch(error => {
          console.error('Fetch error:', error);
          
          // Check if the endpoint is missing (404)
          if (error.message.includes('404')) {
            Swal.fire({
              title: 'Backend Route Missing',
              html: `
                <p>Quad Tag Card requires backend support.</p>
                <p>Please add the route <code>/subdash/&lt;id&gt;/add_quad_tag</code> to the backend.</p>
                <hr>
                <small>Error: ${error.message}</small>
              `,
              icon: 'warning',
              width: '500px'
            });
          } else {
            Swal.fire({
              title: 'Error',
              text: `An error occurred: ${error.message}`,
              icon: 'error'
            });
          }
        });
      });
    }

    // Request notification permission on page load (only when secure context)
    function canUseNativeNotifications() {
      return typeof Notification !== 'undefined' && window.isSecureContext && Notification.permission === 'granted';
    }

    if (typeof Notification !== 'undefined' && window.isSecureContext && Notification.permission === 'default') {
      Notification.requestPermission().catch(() => {
        console.warn('Notification permission request failed (likely insecure context).');
      });
    }

    // Handle remove tag from group
    document.addEventListener('click', function (e) {
      if (e.target.closest('.remove-tag-btn')) {
        e.preventDefault();
        const btn = e.target.closest('.remove-tag-btn');
        const tagId = btn.dataset.tagId;
        const tagName = btn.dataset.tagName;
        const groupId = btn.dataset.groupId;

        Swal.fire({
          title: 'Remove Tag',
          text: `Are you sure you want to remove "${tagName}" from this group?`,
          icon: 'warning',
          showCancelButton: true,
          confirmButtonColor: '#d33',
          cancelButtonColor: '#3085d6',
          confirmButtonText: 'Yes, remove it!'
        }).then((result) => {
          if (result.isConfirmed) {
            // Show loading
            Swal.fire({
              title: 'Removing...',
              text: 'Removing tag from group',
              allowOutsideClick: false,
              didOpen: () => {
                Swal.showLoading();
              }
            });

            // Send AJAX request
            fetch(`/subdash/${currentSubdashId}/remove_tag`, {
              method: 'POST',
              headers: {
                'Content-Type': 'application/x-www-form-urlencoded',
              },
              body: `tag_id=${tagId}&group_id=${groupId}`
            })
              .then(response => response.json())
              .then(data => {
                if (data.success) {
                  Swal.fire({
                    icon: 'success',
                    title: 'Success!',
                    text: data.message || `Tag "${tagName}" removed successfully`,
                    timer: 2000,
                    showConfirmButton: false
                  }).then(() => {
                    // Hard reload page to bypass cache
                    window.location.href = window.location.href + '?t=' + Date.now();
                  });
                } else {
                  Swal.fire({
                    icon: 'error',
                    title: 'Error',
                    text: data.error || 'Failed to remove tag'
                  });
                }
              })
              .catch(error => {
                console.error('Remove tag error:', error);
                Swal.fire({
                  icon: 'error',
                  title: 'Error',
                  text: 'Failed to remove tag'
                });
              });
          }
        });
      }
    });

    // Handle delete group
    document.addEventListener('click', function (e) {
      if (e.target.closest('.delete-group-btn')) {
        e.preventDefault();
        const btn = e.target.closest('.delete-group-btn');
        const groupId = btn.dataset.groupId;
        const groupName = btn.dataset.groupName;

        Swal.fire({
          title: 'Delete Group',
          text: `Are you sure you want to delete the group "${groupName}"?`,
          icon: 'warning',
          showCancelButton: true,
          confirmButtonColor: '#d33',
          cancelButtonColor: '#3085d6',
          confirmButtonText: 'Yes, delete it!'
        }).then((result) => {
          if (result.isConfirmed) {
            // Show loading
            Swal.fire({
              title: 'Deleting...',
              text: 'Deleting group',
              allowOutsideClick: false,
              didOpen: () => {
                Swal.showLoading();
              }
            });

            // Send AJAX request
            fetch(`/subdash/${currentSubdashId}/group/${groupId}/delete`, {
              method: 'POST',
              headers: {
                'Content-Type': 'application/x-www-form-urlencoded',
              }
            })
              .then(response => response.json())
              .then(data => {
                if (data.success) {
                  Swal.fire({
                    icon: 'success',
                    title: 'Deleted!',
                    text: data.message || `Group "${groupName}" deleted successfully`,
                    timer: 2000,
                    showConfirmButton: false
                  }).then(() => {
                    // Hard reload page to bypass cache
                    window.location.href = window.location.href + '?t=' + Date.now();
                  });
                } else {
                  Swal.fire({
                    icon: 'error',
                    title: 'Error',
                    text: data.error || 'Failed to delete group'
                  });
                }
              })
              .catch(error => {
                console.error('Delete group error:', error);
                Swal.fire({
                  icon: 'error',
                  title: 'Error',
                  text: 'Failed to delete group'
                });
              });
          }
        });
      }
    });

    // Handle rename group
    document.addEventListener('click', function (e) {
      if (e.target.closest('.rename-group-btn')) {
        e.preventDefault();
        const btn = e.target.closest('.rename-group-btn');
        const groupId = btn.dataset.groupId;
        const currentGroupName = btn.dataset.groupName;

        Swal.fire({
          title: 'Rename Group',
          input: 'text',
          inputLabel: 'New group name:',
          inputValue: currentGroupName,
          inputPlaceholder: 'Enter new group name',
          showCancelButton: true,
          confirmButtonText: 'Rename',
          cancelButtonText: 'Cancel',
          inputValidator: (value) => {
            if (!value || !value.trim()) {
              return 'Please enter a group name';
            }
            if (value.trim() === currentGroupName) {
              return 'Please enter a different name';
            }
          }
        }).then((result) => {
          if (result.isConfirmed) {
            const newGroupName = result.value.trim();

            // Show loading
            Swal.fire({
              title: 'Renaming...',
              text: 'Renaming group',
              allowOutsideClick: false,
              didOpen: () => {
                Swal.showLoading();
              }
            });

            // Send AJAX request
            fetch(`/subdash/${currentSubdashId}/group/${groupId}/rename`, {
              method: 'POST',
              headers: {
                'Content-Type': 'application/x-www-form-urlencoded',
              },
              body: `new_name=${encodeURIComponent(newGroupName)}`
            })
              .then(response => response.json())
              .then(data => {
                if (data.success) {
                  Swal.fire({
                    icon: 'success',
                    title: 'Renamed!',
                    text: data.message || `Group renamed to "${newGroupName}" successfully`,
                    timer: 2000,
                    showConfirmButton: false
                  }).then(() => {
                    // Hard reload page to bypass cache
                    window.location.href = window.location.href + '?t=' + Date.now();
                  });
                } else {
                  Swal.fire({
                    icon: 'error',
                    title: 'Error',
                    text: data.error || 'Failed to rename group'
                  });
                }
              })
              .catch(error => {
                console.error('Rename group error:', error);
                Swal.fire({
                  icon: 'error',
                  title: 'Error',
                  text: 'Failed to rename group'
                });
              });
          }
        });
      }
    });

    // Handle edit quad tags
    document.addEventListener('click', function (e) {
      if (e.target.closest('.edit-quad-tags-btn')) {
        e.preventDefault();
        
        const btn = e.target.closest('.edit-quad-tags-btn');
        const quadId = btn.dataset.quadId;
        const tag1Id = btn.dataset.tag1Id;
        const tag2Id = btn.dataset.tag2Id;
        const tag3Id = btn.dataset.tag3Id;
        const tag4Id = btn.dataset.tag4Id;
        
        // Set quad ID
        document.getElementById('editQuadId').value = quadId;
        
        // Set current tag selections
        document.getElementById('edit_tag1_select').value = tag1Id;
        document.getElementById('edit_tag2_select').value = tag2Id;
        document.getElementById('edit_tag3_select').value = tag3Id;
        document.getElementById('edit_tag4_select').value = tag4Id;
        
        // Show modal
        const modal = new bootstrap.Modal(document.getElementById('editQuadTagsModal'));
        modal.show();
      }
    });

    // Handle edit quad tags form submission
    const editQuadTagsForm = document.getElementById('editQuadTagsForm');
    if (editQuadTagsForm) {
      editQuadTagsForm.addEventListener('submit', function (e) {
        e.preventDefault();
        
        const quadId = document.getElementById('editQuadId').value;
        const tag1Id = document.getElementById('edit_tag1_select').value;
        const tag2Id = document.getElementById('edit_tag2_select').value;
        const tag3Id = document.getElementById('edit_tag3_select').value;
        const tag4Id = document.getElementById('edit_tag4_select').value;
        
        // Validate that all 4 tags are selected
        if (!tag1Id || !tag2Id || !tag3Id || !tag4Id) {
          showEditQuadError('Please select all 4 tags');
          return;
        }
        
        // Validate that all tags are different
        const tagIds = [tag1Id, tag2Id, tag3Id, tag4Id];
        const uniqueTagIds = [...new Set(tagIds)];
        if (uniqueTagIds.length !== 4) {
          showEditQuadError('All 4 tags must be different');
          return;
        }
        
        // Show loading
        Swal.fire({
          title: 'Updating Tags...',
          allowOutsideClick: false,
          didOpen: () => {
            Swal.showLoading();
          }
        });
        
        // Prepare form data
        const formData = new FormData();
        formData.append('tag1_id', tag1Id);
        formData.append('tag2_id', tag2Id);
        formData.append('tag3_id', tag3Id);
        formData.append('tag4_id', tag4Id);
        
        // Send request
        fetch(`/subdash/${currentSubdashId}/update_quad_tags/${quadId}`, {
          method: 'POST',
          body: formData
        })
        .then(response => response.json())
        .then(data => {
          if (data.success) {
            // ★ Xóa cache layout cũ vì tags đã thay đổi
            try { localStorage.removeItem(QUAD_CACHE_PREFIX + quadId); } catch(e) {}
            Swal.fire({
              title: 'Success!',
              text: 'Quad tags updated successfully',
              icon: 'success',
              timer: 2000
            }).then(() => {
              location.reload();
            });
          } else {
            Swal.fire({
              title: 'Error',
              text: data.message || data.error || 'Failed to update quad tags',
              icon: 'error'
            });
          }
        })
        .catch(error => {
          console.error('Error:', error);
          Swal.fire({
            title: 'Error',
            text: 'An error occurred while updating tags',
            icon: 'error'
          });
        });
      });
    }



    // Handle toggle new quad group button
    document.addEventListener('click', function (e) {
      if (e.target.closest('#toggleNewQuadGroup')) {
        e.preventDefault();
        toggleNewQuadGroupInput();
      }
    });

    // Handle delete quad card
    document.addEventListener('click', function (e) {
      if (e.target.closest('.delete-quad-btn')) {
        e.preventDefault();
        
        const btn = e.target.closest('.delete-quad-btn');
        const quadId = btn.dataset.quadId;
        
        Swal.fire({
          title: 'Delete Quad Card?',
          text: 'This will remove the quad tag card. This action cannot be undone.',
          icon: 'warning',
          showCancelButton: true,
          confirmButtonColor: '#dc3545',
          cancelButtonColor: '#6c757d',
          confirmButtonText: 'Yes, delete it!',
          cancelButtonText: 'Cancel'
        }).then((result) => {
          if (result.isConfirmed) {
            // Show loading
            Swal.fire({
              title: 'Deleting...',
              allowOutsideClick: false,
              didOpen: () => {
                Swal.showLoading();
              }
            });
            
            // Send delete request
            fetch(`/subdash/${currentSubdashId}/delete_quad_card/${quadId}`, {
              method: 'DELETE'
            })
            .then(response => response.json())
            .then(data => {
              if (data.success) {
                // ★ Xóa cache layout vì card đã bị xóa
                try { localStorage.removeItem(QUAD_CACHE_PREFIX + quadId); } catch(e) {}
                Swal.fire({
                  title: 'Deleted!',
                  text: 'Quad card has been deleted.',
                  icon: 'success',
                  timer: 2000
                }).then(() => {
                  location.reload();
                });
              } else {
                Swal.fire({
                  title: 'Error',
                  text: data.message || 'Failed to delete quad card',
                  icon: 'error'
                });
              }
            })
            .catch(error => {
              console.error('Error:', error);
              Swal.fire({
                title: 'Error',
                text: 'An error occurred while deleting the quad card',
                icon: 'error'
              });
            });
          }
        });
      }
    });

    // Rename quad card title and both columns (stored in DATABASE)
    document.addEventListener('click', function (e) {
      const btn = e.target.closest('.rename-quad-btn');
      if (!btn) return;

      e.preventDefault();
      const quadId = btn.dataset.quadId || '';
      const target = btn.dataset.target || 'card';
      const card = btn.closest('.quad-tag-card') || document.querySelector(`.quad-tag-card[data-quad-id="${quadId}"]`);

      if (!card) {
        console.warn('Quad card not found for rename');
        return;
      }

      const subTitles = card.querySelectorAll('.quad-sub-card-title');
      let titleEl = null;
      let defaultTitle = '';

      if (target === 'left') {
        titleEl = subTitles[0] || null;
        defaultTitle = (titleEl && titleEl.textContent.trim()) || 'Group A';
      } else if (target === 'right') {
        titleEl = subTitles[1] || null;
        defaultTitle = (titleEl && titleEl.textContent.trim()) || 'Group B';
      } else {
        titleEl = card.querySelector('.quad-card-title') || card.querySelector('.quad-card-header-title');
        defaultTitle = (titleEl && titleEl.textContent.trim()) || 'Quad Tag Card';
      }

      const currentTitle = (titleEl && titleEl.textContent.trim()) || defaultTitle;

      Swal.fire({
        title: 'Đổi tên',
        input: 'text',
        inputLabel: 'Tiêu đề mới:',
        inputValue: currentTitle,
        inputPlaceholder: 'Nhập tiêu đề mới',
        showCancelButton: true,
        confirmButtonText: 'Cập nhật',
        cancelButtonText: 'Hủy',
        inputValidator: (value) => {
          if (!value || !value.trim()) {
            return 'Vui lòng nhập tiêu đề';
          }
          if (value.trim() === currentTitle) {
            return 'Vui lòng nhập tiêu đề khác';
          }
        }
      }).then((result) => {
        if (!result.isConfirmed) return;

        const newTitle = result.value.trim();

        // Show loading
        Swal.fire({
          title: 'Đang cập nhật...',
          allowOutsideClick: false,
          didOpen: () => {
            Swal.showLoading();
          }
        });

        // Send AJAX request to update title in database
        fetch(`/subdash/${currentSubdashId}/quad_card/${quadId}/rename`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json'
          },
          body: JSON.stringify({
            target: target,
            title: newTitle
          })
        })
        .then(response => response.json())
        .then(data => {
          if (data.success) {
            // Update the title element immediately
            if (titleEl) {
              titleEl.textContent = newTitle;
            }

            Swal.fire({
              icon: 'success',
              title: 'Thành công',
              text: 'Đã cập nhật tiêu đề',
              timer: 1500,
              showConfirmButton: false
            }).then(() => {
              // ★ Xóa cache layout vì title đã thay đổi
              try { localStorage.removeItem(QUAD_CACHE_PREFIX + quadId); } catch(e) {}
              // Reload to ensure all clients see the change
              location.reload();
            });
          } else {
            Swal.fire({
              icon: 'error',
              title: 'Lỗi',
              text: data.message || 'Không thể cập nhật tiêu đề'
            });
          }
        })
        .catch(error => {
          console.error('Error:', error);
          Swal.fire({
            icon: 'error',
            title: 'Lỗi',
            text: 'Đã xảy ra lỗi khi cập nhật tiêu đề'
          });
        });
      });
    });

    // Open compare conditions modal
    document.addEventListener('click', function (e) {
      const btn = e.target.closest('.set-quad-condition-btn');
      if (!btn) return;

      e.preventDefault();
      const quadId = btn.dataset.quadId || '';
      
      // Store quad ID for save handler
      document.getElementById('conditionQuadId').value = quadId;
      
      // Load available tags for comparison
      loadAvailableTagsForComparison();
      
      // Load existing conditions from API
      loadConditionsFromAPI(quadId);
      
      // Show modal
      const modal = new bootstrap.Modal(document.getElementById('compareConditionsModal'));
      modal.show();
    });
    
    // Load available tags for comparison dropdowns
    function loadAvailableTagsForComparison() {
      // Get all tags from the current page
      const tagSelects = [
        document.getElementById('leftHighCompareTag'),
        document.getElementById('leftLowCompareTag'),
        document.getElementById('rightHighCompareTag'),
        document.getElementById('rightLowCompareTag')
      ];
      
      // Collect tags from page - Use Map to avoid duplicates
      const tagsMap = new Map();
      
      // 1. Collect regular tags - Find tag name from button data attributes or card structure
      document.querySelectorAll('.write-btn[data-tag-id]').forEach(btn => {
        const tagId = btn.getAttribute('data-tag-id');
        const tagName = btn.getAttribute('data-tag-name');
        
        if (tagId && tagName) {
          tagsMap.set(tagId, tagName);
        }
      });
      
      // 2. Also check remove-tag-btn in case write-btn is not present
      document.querySelectorAll('.remove-tag-btn[data-tag-id]').forEach(btn => {
        const tagId = btn.getAttribute('data-tag-id');
        const tagName = btn.getAttribute('data-tag-name');
        
        if (tagId && tagName && !tagsMap.has(tagId)) {
          tagsMap.set(tagId, tagName);
        }
      });
      
      // 3. Collect from quad tags - Look for write-quad-tag-btn buttons
      document.querySelectorAll('.write-quad-tag-btn[data-tag-id]').forEach(btn => {
        const tagId = btn.getAttribute('data-tag-id');
        const tagName = btn.getAttribute('data-tag-name');
        
        if (tagId && tagName && !tagsMap.has(tagId)) {
          tagsMap.set(tagId, tagName);
        }
      });
      
      // 4. Fallback: Get from select dropdowns if they exist (tag1_select, tag2_select, etc.)
      const tagSelectors = ['tag1_select', 'tag2_select', 'tag3_select', 'tag4_select'];
      tagSelectors.forEach(selectId => {
        const select = document.getElementById(selectId);
        if (select) {
          Array.from(select.options).forEach(option => {
            if (option.value && option.text && option.value !== '') {
              if (!tagsMap.has(option.value)) {
                tagsMap.set(option.value, option.text);
              }
            }
          });
        }
      });
      
      // 5. Convert Map to array and sort by tag name
      const tags = Array.from(tagsMap.entries()).map(([id, name]) => ({
        id: id,
        name: name
      })).sort((a, b) => a.name.localeCompare(b.name));
      
      console.log(`📋 Loaded ${tags.length} tags for comparison:`, tags);
      
      // Populate all tag dropdowns
      tagSelects.forEach(select => {
        if (!select) return;
        
        // Clear existing options except first one (placeholder)
        while (select.options.length > 1) {
          select.remove(1);
        }
        
        // Add tag options
        tags.forEach(tag => {
          const option = document.createElement('option');
          option.value = tag.id;
          option.textContent = tag.name;
          select.appendChild(option);
        });
      });
    }
    
    // Handle compare type change (static vs tag)
    document.addEventListener('change', function(e) {
      if (e.target.classList.contains('compare-type-selector')) {
        const target = e.target.dataset.target;
        const compareType = e.target.value;
        const valueContainer = document.getElementById(`${target}ValueContainer`);
        const tagContainer = document.getElementById(`${target}TagContainer`);
        
        if (compareType === 'static') {
          valueContainer.style.display = 'block';
          tagContainer.style.display = 'none';
        } else {
          valueContainer.style.display = 'none';
          tagContainer.style.display = 'block';
        }
      }
    });
    
    // Save conditions button handler - use API from quad_conditions.js
    document.getElementById('saveConditionsBtn').addEventListener('click', function() {
      saveConditionsToAPI();
    });

    // Reset quad tag modal when closed
    const addQuadTagModal = document.getElementById('addQuadTagModal');
    if (addQuadTagModal) {
      // Debug when the modal is opened
      addQuadTagModal.addEventListener('shown.bs.modal', function () {
        console.log('Quad tag modal opened');
        // Verify required elements exist
        const elements = [
          'tag1_select', 'tag2_select', 'tag3_select', 'tag4_select',
          'quad_target_group', 'new_quad_group_name'
        ];
        elements.forEach(id => {
          const el = document.getElementById(id);
          if (el) {
            console.log(`✓ Element found: ${id}`);
          } else {
            console.error(`✗ Element missing: ${id}`);
          }
        });
      });
      
      addQuadTagModal.addEventListener('hidden.bs.modal', function () {
        // Reset form
        document.getElementById('addQuadTagForm').reset();
        
        // Hide error message
        document.getElementById('quad-error-message').style.display = 'none';
        
        // Reset new group section
        const newQuadGroupSection = document.getElementById('newQuadGroupSection');
        const quadTargetGroupSelect = document.getElementById('quad_target_group');
        const toggleBtn = document.getElementById('toggleNewQuadGroup');
        const newQuadGroupInput = document.getElementById('new_quad_group_name');
        
        newQuadGroupSection.style.display = 'none';
        quadTargetGroupSelect.disabled = false;
        newQuadGroupInput.required = false;
        toggleBtn.innerHTML = '<i class=\"bi bi-plus-circle\"></i> New';
        toggleBtn.className = 'btn btn-outline-secondary';
        

      });
    }

  }); // End of DOMContentLoaded

  // Show error message for quad tag form
  function showQuadError(message) {
    const errorDiv = document.getElementById('quad-error-message');
    errorDiv.textContent = message;
    errorDiv.style.display = 'block';
    setTimeout(() => {
      errorDiv.style.display = 'none';
    }, 5000);
  }

  // Show error message for edit quad tags form
  function showEditQuadError(message) {
    const errorDiv = document.getElementById('edit-quad-error-message');
    errorDiv.textContent = message;
    errorDiv.style.display = 'block';
    setTimeout(() => {
      errorDiv.style.display = 'none';
    }, 5000);
  }

  // Toggle new group input for quad tags
  function toggleNewQuadGroupInput() {
    const newQuadGroupSection = document.getElementById('newQuadGroupSection');
    const quadTargetGroupSelect = document.getElementById('quad_target_group');
    const toggleBtn = document.getElementById('toggleNewQuadGroup');
    const newQuadGroupInput = document.getElementById('new_quad_group_name');

    if (newQuadGroupSection.style.display === 'none') {
      // Show new group input
      newQuadGroupSection.style.display = 'block';
      quadTargetGroupSelect.disabled = true;
      quadTargetGroupSelect.value = '';
      newQuadGroupInput.required = true;
      toggleBtn.innerHTML = '<i class=\"bi bi-x-circle\"></i> Cancel';
      toggleBtn.className = 'btn btn-outline-danger';
    } else {
      // Hide new group input
      newQuadGroupSection.style.display = 'none';
      quadTargetGroupSelect.disabled = false;
      newQuadGroupInput.required = false;
      newQuadGroupInput.value = '';
      toggleBtn.innerHTML = '<i class=\"bi bi-plus-circle\"></i> New';
      toggleBtn.className = 'btn btn-outline-secondary';
    }
  }

  // Format time to 24h format (HH:mm:ss)
  function formatTime24h(date) {
    if (!date) return '00:00:00';
    const hours = String(date.getHours()).padStart(2, '0');
    const minutes = String(date.getMinutes()).padStart(2, '0');
    const seconds = String(date.getSeconds()).padStart(2, '0');
    return `${hours}:${minutes}:${seconds}`;
  }

  // Format quad tag values for better display
  function formatQuadTagValue(value) {
    if (value === null || value === undefined || value === '') {
      return '0.0';
    }
    
    const num = parseFloat(value);
    if (isNaN(num)) {
      return '0.0';
    }
    
    // Format large numbers with appropriate suffixes
    if (Math.abs(num) >= 1000000) {
      return (num / 1000000).toFixed(1) + 'M';
    } else if (Math.abs(num) >= 1000) {
      return (num / 1000).toFixed(1) + 'K';
    } else {
      return num.toFixed(1);
    }
  }

  // Local title helpers to allow renaming headers and persisting in the browser
  function getStoredTitle(key) {
    return localStorage.getItem(key) || '';
  }

  function setStoredTitle(key, value) {
    if (value) {
      localStorage.setItem(key, value);
    } else {
      localStorage.removeItem(key);
    }
  }

  function attachTitleEditor(titleEl, storageKey, defaultValue) {
    if (!titleEl) return;
    titleEl.style.cursor = 'text';
    titleEl.title = 'Double-click to rename';

    titleEl.addEventListener('dblclick', function () {
      const current = titleEl.textContent.trim();
      const next = prompt('Enter new title', current) || '';
      const trimmed = next.trim();

      if (trimmed) {
        titleEl.textContent = trimmed;
        setStoredTitle(storageKey, trimmed);
      } else {
        titleEl.textContent = defaultValue;
        setStoredTitle(storageKey, '');
      }
    });
  }

  // ===== Quad Tag Layout Cache =====
  // Lưu layout HTML đã build vào localStorage, lần load sau inject trực tiếp → không flash
  const QUAD_CACHE_PREFIX = `quad_layout_${currentSubdashId}_`;

  function saveQuadGridCache(quadId, gridEl) {
    try {
      localStorage.setItem(QUAD_CACHE_PREFIX + quadId, gridEl.innerHTML);
    } catch (e) { /* quota exceeded – bỏ qua */ }
  }

  function loadQuadGridCache(quadId) {
    try {
      return localStorage.getItem(QUAD_CACHE_PREFIX + quadId);
    } catch (e) { return null; }
  }

  // Lưu giá trị mới nhất của từng quad tag vào cache riêng (cập nhật bởi socket)
  const QUAD_VALUES_KEY = `quad_values_${currentSubdashId}`;
  let _quadValuesCache = {};
  try {
    _quadValuesCache = JSON.parse(localStorage.getItem(QUAD_VALUES_KEY)) || {};
  } catch (e) { _quadValuesCache = {}; }

  function updateQuadValueCache(tagId, value) {
    _quadValuesCache[tagId] = value;
    try {
      localStorage.setItem(QUAD_VALUES_KEY, JSON.stringify(_quadValuesCache));
    } catch (e) { /* ignore */ }
  }

  // Fix quad tag layout by restructuring HTML using template structure
  function fixQuadTagLayout() {
    console.log('🔧 Starting fixQuadTagLayout...');
    
    document.querySelectorAll('.quad-tag-card').forEach((card, cardIndex) => {
      const cardId = card.dataset.quadId || `quad-${cardIndex + 1}`;
      
      // ★ Thử dùng cache trước – inject HTML đã build sẵn, skip parse/rebuild
      const cachedHTML = loadQuadGridCache(cardId);
      const body = card.querySelector('.card-body') || card;
      const items = body.querySelectorAll('.quad-tag-item');
      
      // Xây fingerprint từ tag IDs hiện tại để detect config thay đổi
      const currentTagIds = Array.from(items).map(it => it.getAttribute('data-tag-id')).join(',');
      
      if (cachedHTML && items.length >= 4) {
        // Kiểm tra fingerprint: tất cả tag IDs hiện tại đều phải có trong cache
        const tempDiv = document.createElement('div');
        tempDiv.innerHTML = cachedHTML;
        const cachedTagIds = Array.from(tempDiv.querySelectorAll('.quad-tag-item'))
          .map(it => it.getAttribute('data-tag-id')).sort().join(',');
        const sortedCurrentIds = currentTagIds.split(',').sort().join(',');
        
        if (cachedTagIds === sortedCurrentIds) {
          console.log(`⚡ Using cached layout for quad card ${cardId}`);
          let grid = card.querySelector('.quad-tag-grid');
          if (!grid) {
            grid = document.createElement('div');
            grid.className = 'quad-tag-grid';
            const cardBody = card.querySelector('.card-body');
            if (cardBody) {
              cardBody.innerHTML = '';
              cardBody.appendChild(grid);
            }
          } else {
            grid.innerHTML = '';
          }
          grid.innerHTML = cachedHTML;
          
          // Xóa items gốc để tránh duplicate IDs
          items.forEach(item => item.remove());
          
          // Áp dụng giá trị mới nhất từ value cache (nếu có)
          Object.keys(_quadValuesCache).forEach(tagId => {
            const el = grid.querySelector(`[id$="-${tagId}"]`);
            if (el) el.textContent = _quadValuesCache[tagId];
          });
          
          // Cập nhật thời gian
          grid.querySelectorAll('.quad-update-time').forEach(el => {
            el.textContent = `Last updated: ${formatTime24h(new Date())}`;
          });
          
          return; // Xong card này, skip rebuild
        }
      }
      
      // ★ Không có cache hoặc config đã thay đổi → rebuild bình thường
      console.log(`Processing quad card ${cardIndex + 1} (full rebuild)`);
      
      if (items.length >= 4) {
        // Get the grid container
        let grid = card.querySelector('.quad-tag-grid');
        if (!grid) {
          // Create grid if it doesn't exist
          grid = document.createElement('div');
          grid.className = 'quad-tag-grid';
          const cardBody = card.querySelector('.card-body');
          if (cardBody) {
            cardBody.innerHTML = '';
            cardBody.appendChild(grid);
          }
        }
        
        // Clear existing content
        grid.innerHTML = '';
        
        // Extract data from existing items - MUST BE IN ORDER
        const itemsData = [];
        for (let i = 0; i < 4; i++) {
          const item = items[i];
          if (!item) {
            console.warn(`Quad tag ${i} not found!`);
            continue;
          }
          
          // Get tag ID from data attribute
          const tagId = item.getAttribute('data-tag-id');
          if (!tagId) {
            console.warn(`Tag ID not found for item ${i}`);
            continue;
          }

          // Preserve device status so quad-only dashboards know current state
          const deviceStatus = item.getAttribute('data-device-status') || 'unknown';
          
          // Find the value element by exact ID
          const valElement = document.getElementById(`quad-tag-val-${tagId}`);
          const unitElement = item.querySelector('small, .text-muted');
          const nameElement = item.querySelector('.quad-tag-name');
          
          // Extract current value and unit — ưu tiên value cache nếu có
          let value = valElement ? valElement.textContent.trim() : '0';
          if (_quadValuesCache[tagId] !== undefined) {
            value = _quadValuesCache[tagId];
          }
          const unit = unitElement ? unitElement.textContent.trim() : 'Kwh';
          const name = nameElement ? nameElement.textContent.trim() : `Tag ${i + 1}`;
          
          // ✅ Use UNIQUE ID format: cardId + tagId để tránh duplicate IDs
          const id = `quad-tag-val-${cardId}-${tagId}`;
          
          console.log(`Quad tag ${i}: ID=${id}, CardID=${cardId}, TagID=${tagId}, Value=${value}, Unit=${unit}, Status=${deviceStatus}`);
          
          itemsData.push({
            value: value, // Keep original value without K/M formatting
            unit: unit,
            id: id,
            tagId: tagId,
            name: name,
            deviceStatus: deviceStatus
          });
        }
        
        // ✅ REMOVE all original items completely to avoid duplicate IDs
        items.forEach(item => {
          item.remove();  // Xóa hoàn toàn khỏi DOM thay vì chỉ hide
        });
        
        // Get titles from data attributes (from database) with fallback to default names
        const cardTitleFromDB = card.getAttribute('data-card-title') || 'Quad Tag Card';
        const leftTitleFromDB = card.getAttribute('data-left-title') || extractGroupName(itemsData[0].name, itemsData[1].name) || 'Group A';
        const rightTitleFromDB = card.getAttribute('data-right-title') || extractGroupName(itemsData[2].name, itemsData[3].name) || 'Group B';
        
        const title1 = leftTitleFromDB;
        const title2 = rightTitleFromDB;

        // Create first sub-card (Items 1 & 2)
        const subCard1 = document.createElement('div');
        subCard1.className = 'quad-sub-card';
        subCard1.setAttribute('data-quad-id', cardId);
        subCard1.setAttribute('data-column', 'left');
        subCard1.innerHTML = `
          <div class="quad-sub-card-head quad-group-1">
            <div class="quad-sub-card-title" data-title-slot="left">${title1}</div>
          </div>
          <div class="quad-tag-row">
            <div class="quad-tag-item pv-item" data-tag-id="${itemsData[0].tagId}" data-device-status="${itemsData[0].deviceStatus}">
              <div class="quad-tag-label">PV</div>
              <div class="quad-tag-value-row">
                <div class="quad-tag-value" id="${itemsData[0].id}">${itemsData[0].value}</div>
                <div class="quad-tag-unit">${itemsData[0].unit}</div>
              </div>
            </div>
            <div class="quad-tag-item sv-item" data-tag-id="${itemsData[2].tagId}" data-device-status="${itemsData[2].deviceStatus}">
              <div class="quad-tag-label">SV</div>
              <div class="quad-tag-value-row">
                <div class="quad-tag-value" id="${itemsData[2].id}">${itemsData[2].value}</div>
                <div class="quad-tag-unit">${itemsData[2].unit}</div>
              </div>
            </div>
          </div>
          <div class="quad-update-time">Last updated: ${formatTime24h(new Date())}</div>
        `;
        
        // Create second sub-card (Items 3 & 4)
        const subCard2 = document.createElement('div');
        subCard2.className = 'quad-sub-card';
        subCard2.setAttribute('data-quad-id', cardId);
        subCard2.setAttribute('data-column', 'right');
        subCard2.innerHTML = `
          <div class="quad-sub-card-head quad-group-2">
            <div class="quad-sub-card-title" data-title-slot="right">${title2}</div>
          </div>
          <div class="quad-tag-row">
            <div class="quad-tag-item pv-item" data-tag-id="${itemsData[1].tagId}" data-device-status="${itemsData[1].deviceStatus}">
              <div class="quad-tag-label">PV</div>
              <div class="quad-tag-value-row">
                <div class="quad-tag-value" id="${itemsData[1].id}">${itemsData[1].value}</div>
                <div class="quad-tag-unit">${itemsData[1].unit}</div>
              </div>
            </div>
            <div class="quad-tag-item sv-item" data-tag-id="${itemsData[3].tagId}" data-device-status="${itemsData[3].deviceStatus}">
              <div class="quad-tag-label">SV</div>
              <div class="quad-tag-value-row">
                <div class="quad-tag-value" id="${itemsData[3].id}">${itemsData[3].value}</div>
                <div class="quad-tag-unit">${itemsData[3].unit}</div>
              </div>
            </div>
          </div>
          <div class="quad-update-time">Last updated: ${formatTime24h(new Date())}</div>
        `;
        
        // Add sub-cards to grid
        grid.appendChild(subCard1);
        grid.appendChild(subCard2);
        
        // ★ Lưu layout HTML vào cache cho lần load sau
        saveQuadGridCache(cardId, grid);
        
        // Update card header title from database
        const cardTitle = cardTitleFromDB;

        let cardHeader = card.querySelector('.card-header');
        if (!cardHeader) {
          cardHeader = document.createElement('div');
          cardHeader.className = 'card-header bg-dark text-white d-flex justify-content-between align-items-center py-2';
          const cardBody = card.querySelector('.card-body');
          if (cardBody) {
            cardBody.className = 'card-body p-0';
            card.insertBefore(cardHeader, cardBody);
          }
        }

        let headerTitleEl = cardHeader.querySelector('.quad-card-title') || cardHeader.querySelector('.quad-card-header-title');
        if (!headerTitleEl) {
          headerTitleEl = document.createElement('span');
          headerTitleEl.className = 'quad-card-title fw-semibold';
          headerTitleEl.textContent = cardTitle;
          // Prepend to keep the gear icon on the right
          const flexWrapper = document.createElement('div');
          flexWrapper.className = 'd-flex align-items-center gap-2';
          flexWrapper.appendChild(headerTitleEl);
          cardHeader.prepend(flexWrapper);
        } else {
          headerTitleEl.textContent = cardTitle;
        }
        
        console.log(`✅ Successfully restructured quad card ${cardIndex + 1}`);
      }
    });
    
    console.log('🎯 fixQuadTagLayout completed');
  }
  
  // Extract meaningful group name from tag names
  function extractGroupName(tag1Name, tag2Name) {
    // Try to find common parts or meaningful names
    const name1 = tag1Name.toLowerCase();
    const name2 = tag2Name.toLowerCase();
    
    // Common patterns
    if (name1.includes('temp') || name2.includes('temp')) return 'Temperature';
    if (name1.includes('humid') || name2.includes('humid')) return 'Humidity';
    if (name1.includes('press') || name2.includes('press')) return 'Pressure';
    if (name1.includes('flow') || name2.includes('flow')) return 'Flow';
    if (name1.includes('level') || name2.includes('level')) return 'Level';
    
    // If no pattern found, use first tag name without numbers
    return tag1Name.replace(/\d+/g, '').trim() || 'Group';
  }

  // Initialize quad tag values formatting on page load
  document.addEventListener('DOMContentLoaded', function() {
    console.log('🚀 DOM loaded, starting quad layout fix...');

    // Chạy fixQuadTagLayout ngay lập tức (KHÔNG dùng requestAnimationFrame)
    // để hoàn thành trước first paint, tránh nháy layout cũ → mới
    fixQuadTagLayout();

    // Keep original values without K/M formatting
    document.querySelectorAll('[id^="quad-tag-val-"]').forEach(element => {
      // Values already loaded from server, no need to reformat
      console.log(`Quad tag ${element.id}: ${element.textContent}`);
    });

    // Hiện quad cards sau khi layout đã rebuild xong
    document.querySelectorAll('.quad-tag-card').forEach(card => {
      card.style.visibility = 'visible';
    });

    // Áp dụng trạng thái alarm sau khi layout đã dựng xong
    applyActiveQuadAlarms();

    console.log('✨ Quad layout initialization completed');
  });

  // Apply active quad alarm states on page load
  function applyActiveQuadAlarms() {
    console.log('🔔 Starting applyActiveQuadAlarms...');
    
    // Get active alarms from data attribute in HTML
    const dataEl = document.getElementById('quad-alarm-data');
    let activeAlarms = [];
    
    if (dataEl) {
      console.log('✓ Found quad-alarm-data element');
      try {
        const dataStr = dataEl.getAttribute('data-active-alarms');
        console.log('📊 Raw alarm data:', dataStr);
        activeAlarms = dataStr ? JSON.parse(dataStr) : [];
      } catch (e) {
        console.error('❌ Failed to parse active quad alarms:', e);
        activeAlarms = [];
      }
    } else {
      console.warn('⚠️ quad-alarm-data element not found!');
    }
    
    console.log('🔔 Applying active quad alarms:', activeAlarms);
    
    if (!activeAlarms || activeAlarms.length === 0) {
      console.log('ℹ️ No active quad alarms to apply');
      return;
    }
    
    activeAlarms.forEach((alarm, index) => {
      const quadId = alarm.quad_id;
      const column = alarm.column;
      const alarmType = alarm.alarm_type;
      
      console.log(`🔍 Processing alarm ${index + 1}:`, { quadId, column, alarmType });
      
      // Find the quad sub-card element
      const selector = `[data-quad-id="${quadId}"][data-column="${column}"]`;
      console.log(`🔍 Looking for selector: ${selector}`);
      const subCard = document.querySelector(selector);
      
      if (subCard) {
        console.log(`✓ Found sub-card for quad ${quadId} ${column}`);
        // Remove any existing alarm classes
        subCard.classList.remove('quad-alarm-high', 'quad-alarm-low');
        
        // Add the appropriate alarm class
        if (alarmType === 'High') {
          subCard.classList.add('quad-alarm-high');
          console.log(`✅ Applied High alarm to quad ${quadId} ${column} column`);
        } else if (alarmType === 'Low') {
          subCard.classList.add('quad-alarm-low');
          console.log(`✅ Applied Low alarm to quad ${quadId} ${column} column`);
        }
      } else {
        console.error(`❌ Could not find sub-card for quad ${quadId} ${column} column`);
        // Debug: list all available sub-cards
        const allSubCards = document.querySelectorAll('.quad-sub-card');
        console.log(`📋 Available sub-cards (${allSubCards.length}):`, 
          Array.from(allSubCards).map(card => ({
            quadId: card.getAttribute('data-quad-id'),
            column: card.getAttribute('data-column')
          }))
        );
      }
    });
    
    console.log('🎯 applyActiveQuadAlarms completed');
  }

  /**
   * Fetch active quad alarms from server API and apply CSS classes.
   * Called on socket reconnect and periodically to keep alarm visual state in sync.
   * This prevents alarm colors from being lost after long uptime / socket reconnections.
   */
  function fetchAndApplyQuadAlarms() {
    const subdashId = currentSubdashId;
    if (!subdashId) return;

    fetch(`/subdash/${subdashId}/api/active_quad_alarms`)
      .then(res => res.json())
      .then(data => {
        if (!data.success || !Array.isArray(data.alarms)) return;

        // Build a set of currently active alarms from server
        // Format: "quadId-column" -> alarmType
        const serverAlarms = new Map();
        data.alarms.forEach(alarm => {
          const key = `${alarm.quad_id}-${alarm.column}`;
          serverAlarms.set(key, alarm.alarm_type);
        });

        // Get all quad sub-cards and sync their alarm state
        document.querySelectorAll('.quad-sub-card').forEach(subCard => {
          const quadId = subCard.getAttribute('data-quad-id');
          const column = subCard.getAttribute('data-column');
          if (!quadId || !column) return;

          const key = `${quadId}-${column}`;
          const activeAlarmType = serverAlarms.get(key);

          // Remove existing alarm classes
          subCard.classList.remove('quad-alarm-high', 'quad-alarm-low');

          // Re-apply if alarm is still active on server
          if (activeAlarmType === 'High') {
            subCard.classList.add('quad-alarm-high');
          } else if (activeAlarmType === 'Low') {
            subCard.classList.add('quad-alarm-low');
          }
        });

        console.log(`[QuadAlarmSync] Synced ${data.alarms.length} active alarms from server`);
      })
      .catch(err => {
        console.warn('[QuadAlarmSync] Failed to fetch active quad alarms:', err);
      });
  }

  // Expose for external use
  window.fetchAndApplyQuadAlarms = fetchAndApplyQuadAlarms;

  // Periodic alarm state sync every 30 seconds to keep colors in sync
  // This prevents alarm colors from being lost during long uptime
  const _quadAlarmSyncInterval = setInterval(fetchAndApplyQuadAlarms, 30000);

  // Check if tag supports write operations
  function canWriteTag(functionCode) {
    const fc = getFunctionCodeInt(functionCode);
    return fc === 1 || fc === 3;
  }

  // Show error message for quad tag form
  function showQuadError(message) {
    const errorDiv = document.getElementById('quad-error-message');
    errorDiv.textContent = message;
    errorDiv.style.display = 'block';
    setTimeout(() => {
      errorDiv.style.display = 'none';
    }, 5000);
  }

  // Toggle new group input for quad tags
  function toggleNewQuadGroupInput() {
    const newQuadGroupSection = document.getElementById('newQuadGroupSection');
    const quadTargetGroupSelect = document.getElementById('quad_target_group');
    const toggleBtn = document.getElementById('toggleNewQuadGroup');
    const newQuadGroupInput = document.getElementById('new_quad_group_name');

    if (newQuadGroupSection.style.display === 'none') {
      // Show new group input
      newQuadGroupSection.style.display = 'block';
      quadTargetGroupSelect.disabled = true;
      quadTargetGroupSelect.value = '';
      newQuadGroupInput.required = true;
      toggleBtn.innerHTML = '<i class="bi bi-x-circle"></i> Cancel';
      toggleBtn.className = 'btn btn-outline-danger';
    } else {
      // Hide new group input
      newQuadGroupSection.style.display = 'none';
      quadTargetGroupSelect.disabled = false;
      newQuadGroupInput.required = false;
      newQuadGroupInput.value = '';
      toggleBtn.innerHTML = '<i class="bi bi-plus-circle"></i> New';
      toggleBtn.className = 'btn btn-outline-secondary';
    }
  }

  // Toggle new group input visibility (global function)
  function toggleNewGroupInput() {
    const newGroupSection = document.getElementById('newGroupSection');
    const targetGroupSelect = document.getElementById('target_group');
    const toggleBtn = document.getElementById('toggleNewGroup');
    const newGroupInput = document.getElementById('new_group_name');

    if (newGroupSection.style.display === 'none') {
      // Show new group input
      newGroupSection.style.display = 'block';
      targetGroupSelect.disabled = true;
      targetGroupSelect.value = '';
      newGroupInput.required = true;
      toggleBtn.innerHTML = '<i class="bi bi-x-circle"></i> Cancel';
      toggleBtn.className = 'btn btn-outline-danger';
    } else {
      // Hide new group input
      newGroupSection.style.display = 'none';
      targetGroupSelect.disabled = false;
      newGroupInput.required = false;
      newGroupInput.value = '';
      toggleBtn.innerHTML = '<i class="bi bi-plus-circle"></i> New';
      toggleBtn.className = 'btn btn-outline-secondary';
    }
  }

  // Handle Edit Unit double-click
  document.addEventListener('dblclick', function (e) {
    if (e.target.closest('.unit-editable')) {
      const span = e.target.closest('.unit-editable');
      const tagId = span.getAttribute('data-tag-id');
      const currentUnit = span.getAttribute('data-current-unit');

      // Populate modal
      document.getElementById('editUnitTagId').value = tagId;
      document.getElementById('editUnitValue').value = currentUnit || '';

      // Show modal
      const modal = new bootstrap.Modal(document.getElementById('editUnitModal'));
      modal.show();
    }
  });

  // Add hover effect for editable units
  document.addEventListener('mouseover', function (e) {
    if (e.target.closest('.unit-editable')) {
      const span = e.target.closest('.unit-editable');
      span.style.backgroundColor = '#e9ecef';
    }
  });

  document.addEventListener('mouseout', function (e) {
    if (e.target.closest('.unit-editable')) {
      const span = e.target.closest('.unit-editable');
      span.style.backgroundColor = '';
    }
  });

  // Handle Edit Unit form submission
  const editUnitForm = document.getElementById('editUnitForm');
  if (editUnitForm) {
    editUnitForm.addEventListener('submit', function (e) {
      e.preventDefault();

      const tagId = document.getElementById('editUnitTagId').value;
      const newUnit = document.getElementById('editUnitValue').value.trim();

      // Show loading
      Swal.fire({
        title: 'Updating...',
        text: 'Updating unit',
        allowOutsideClick: false,
        didOpen: () => {
          Swal.showLoading();
        }
      });

      // Send AJAX request
      fetch(`/tags/${tagId}/update-unit`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
        },
        body: `unit=${encodeURIComponent(newUnit)}`
      })
        .then(response => response.json())
        .then(data => {
          if (data.success) {
            Swal.fire({
              icon: 'success',
              title: 'Success',
              text: 'Unit updated successfully',
              timer: 2000,
              showConfirmButton: false
            });

            // Update display
            const unitDisplay = document.getElementById(`tag-unit-${tagId}`);
            if (unitDisplay) {
              unitDisplay.textContent = newUnit || 'No Unit';
              // Update data attribute for future edits
              unitDisplay.setAttribute('data-current-unit', newUnit);
            }

            // Close modal
            const modal = bootstrap.Modal.getInstance(document.getElementById('editUnitModal'));
            modal.hide();

          } else {
            Swal.fire({
              icon: 'error',
              title: 'Error',
              text: data.message || 'Failed to update unit'
            });
          }
        })
        .catch(error => {
          Swal.close();
          Swal.fire({
            icon: 'error',
            title: 'Error',
            text: 'Network error occurred'
          });
        });
    });
  }

  // Socket connection variables (global)
  let socketConnected = false;
  let pollingInterval = null;
  // currentSubdashId is defined at file top from SUBDASH_CONFIG

  // Chuẩn hóa trạng thái device để tránh khác biệt chữ hoa/thường
  function normalizeDeviceStatus(status) {
    if (status === true) return 'connected';
    if (status === false) return 'disconnected';
    const s = (status || '').toString().trim().toLowerCase();
    if (['connected', 'online', 'ok', 'good', 'true', '1'].includes(s)) return 'connected';
    if (['disconnected', 'offline', 'bad', 'false', '0'].includes(s)) return 'disconnected';
    return 'unknown';
  }

  // Simple tag timer system for 30s timeout - DECLARE EARLY
  const tagTimers = new Map(); // Store timeout IDs for each tag

  // Update quad status indicator based on PV tag device status
  function updateQuadStatusIndicator(tagId, deviceStatus) {
    const normalizedStatus = normalizeDeviceStatus(deviceStatus);
    // Find all quad cards that use this tag as PV (tag1)
    const indicators = document.querySelectorAll(`.quad-status-indicator[data-pv-tag-id="${tagId}"]`);
    
    indicators.forEach(indicator => {
      // Remove all status classes
      indicator.classList.remove('status-online', 'status-offline', 'status-unknown');
      indicator.setAttribute('data-device-status', normalizedStatus);
      
      // Add appropriate status class
      if (normalizedStatus === 'connected') {
        indicator.classList.add('status-online');
        indicator.title = 'Device online';
      } else if (normalizedStatus === 'disconnected') {
        indicator.classList.add('status-offline');
        indicator.title = 'Device offline';
        // Force all quad tag values tied to this device to 0 when offline
        document.querySelectorAll(`[id^="quad-tag-val-"][id$="-${tagId}"]`).forEach(el => {
          el.textContent = '0';
        });
      } else {
        indicator.classList.add('status-unknown');
        indicator.title = 'Device status unknown';
      }
    });
  }

  // Reset timer for a tag (called when ts is updated)
  function resetTagTimer(tagId) {
    // Clear existing timer if any
    if (tagTimers.has(tagId)) {
      clearTimeout(tagTimers.get(tagId));
    }

    // Set new 30-second timer
    const timeoutId = setTimeout(() => {
      markTagInactive(tagId);
    }, 30000); // 30 seconds

    // Store the timeout ID
    tagTimers.set(tagId, timeoutId);

    // Only restore if tag was actually inactive before
    const valEl = document.getElementById('tag-val-' + tagId);
    if (valEl && valEl.style.opacity === '0.5') {
      restoreTagToActive(tagId);
    } else {
      // Tag is active, update quad indicator to reflect current device status
      const barEl = document.getElementById('tag-bar-' + tagId);
      const indicator = document.querySelector(`.quad-status-indicator[data-pv-tag-id="${tagId}"]`);
      const deviceStatus = normalizeDeviceStatus(barEl?.getAttribute('data-device-status') || indicator?.getAttribute('data-device-status') || 'unknown');
      updateQuadStatusIndicator(tagId, deviceStatus);
    }
  }

  // Update tất cả ô giá trị quad liên quan tới cùng một tag
  function setQuadTagValueByTagId(tagId, valueText) {
    // New layout dùng data-tag-id và id dạng quad-tag-val-{cardId}-{tagId}
    document.querySelectorAll(`.quad-tag-item[data-tag-id="${tagId}"] .quad-tag-value`).forEach(span => {
      span.textContent = valueText;
    });

    // Fallback cho id kết thúc bằng -tagId
    document.querySelectorAll(`[id^="quad-tag-val-"][id$="-${tagId}"]`).forEach(span => {
      span.textContent = valueText;
    });
  }

  // Gỡ trạng thái alarm trên mọi quad sub-card chứa tag này.
  // Thay vì xóa ngay, gọi server để kiểm tra alarm còn active không.
  // Nếu alarm vẫn active trên server thì giữ nguyên CSS class.
  function clearQuadAlarmByTag(tagId) {
    // Debounce: schedule a server re-sync instead of immediately clearing.
    // This prevents momentary disconnections from wiping alarm colors.
    if (window._clearAlarmSyncTimer) {
      clearTimeout(window._clearAlarmSyncTimer);
    }
    window._clearAlarmSyncTimer = setTimeout(() => {
      fetchAndApplyQuadAlarms();
    }, 2000); // Wait 2s then sync from server to get accurate state
  }

  // Mark tag as inactive (no ts update for 30s)
  function markTagInactive(tagId) {
    const valEl = document.getElementById('tag-val-' + tagId);
    const tsEl = document.getElementById('tag-ts-' + tagId);
    const barEl = document.getElementById('tag-bar-' + tagId);
    const badgeEl = document.getElementById('tag-badge-' + tagId);

    if (valEl) {
      // Reset value to 0 and dim it
      valEl.textContent = '0';
      valEl.style.opacity = '0.5';
      valEl.style.color = '#6c757d';
    }

    // Đồng bộ giá trị về 0 cho tất cả quad tag hiển thị cùng tag này
    setQuadTagValueByTagId(tagId, '0');
    // Khi mất kết nối cũng tắt trạng thái alarm của quad tag đó
    clearQuadAlarmByTag(tagId);

    if (tsEl) {
      // Show timeout message
      tsEl.textContent = 'No data (30s+)';
      tsEl.style.color = '#dc3545';
      tsEl.style.fontWeight = 'bold';
    }

    if (barEl) {
      // Set progress bar to inactive
      barEl.style.width = '0%';
      barEl.className = barEl.className.replace(/(device-|tag-)(connected|disconnected|unknown|inactive)/g, '');
      barEl.classList.add('tag-inactive');
    }

    // Update badge to reflect inactive state (treat as Bad/red)
    if (badgeEl) {
      badgeEl.classList.remove('bg-success', 'bg-secondary');
      badgeEl.classList.add('bg-danger');
      badgeEl.textContent = 'Offline';
    }
    
    // Update quad status indicator to offline for this tag
    updateQuadStatusIndicator(tagId, 'disconnected');

    // console.log(`Tag ${tagId} marked inactive (no ts update for 30s)`);
  }

  // Restore tag to active state
  function restoreTagToActive(tagId) {
    const valEl = document.getElementById('tag-val-' + tagId);
    const tsEl = document.getElementById('tag-ts-' + tagId);
    const barEl = document.getElementById('tag-bar-' + tagId);
    const badgeEl = document.getElementById('tag-badge-' + tagId);

    if (valEl && valEl.style.opacity === '0.5') {
      // Restore normal styling only if it was inactive
      valEl.style.opacity = '';
      valEl.style.color = '';
    }

    if (tsEl && tsEl.textContent.includes('No data')) {
      // Clear timeout message styling
      tsEl.style.color = '';
      tsEl.style.fontWeight = '';
    }

    if (barEl && barEl.classList.contains('tag-inactive')) {
      // Restore normal progress bar based on device status
      const deviceStatus = normalizeDeviceStatus(barEl.getAttribute('data-device-status') || 'unknown');
      barEl.classList.remove('tag-inactive');
      
      // Update quad status indicator to match device status
      updateQuadStatusIndicator(tagId, deviceStatus);

      if (deviceStatus === 'connected') {
        barEl.style.width = '100%';
        barEl.classList.add('device-connected');
        if (badgeEl) {
          badgeEl.classList.remove('bg-danger', 'bg-secondary');
          badgeEl.classList.add('bg-success');
          badgeEl.textContent = 'Online';
        }
      } else if (deviceStatus === 'disconnected') {
        barEl.style.width = '0%';
        barEl.classList.add('device-disconnected');
        if (badgeEl) {
          badgeEl.classList.remove('bg-success', 'bg-secondary');
          badgeEl.classList.add('bg-danger');
          badgeEl.textContent = 'Offline';
        }
      } else {
        // unknown -> treat same as disconnected for badge, keep bar unknown
        barEl.style.width = '0%';
        barEl.classList.add('device-unknown');
        if (badgeEl) {
          badgeEl.classList.remove('bg-success', 'bg-secondary');
          badgeEl.classList.add('bg-danger');
          badgeEl.textContent = 'Offline';
        }
      }
    }
  }

  // Real-time socket listener for modbus updates (immediate connection)
  if (typeof socket !== 'undefined' && socket !== null) {
    socket.on("modbus_update", function (data) {
      if (!data) return;

      socketConnected = true;

      // 1) Derive default device status from payload (fallback only)
      const payloadDeviceStatus = normalizeDeviceStatus(
        data.status !== undefined ? data.status : (data.ok === true ? 'connected' : 'unknown')
      );

      // 3) Update each tag
      if (Array.isArray(data.tags)) {
        // Collect value updates (only when value changes)
        const pending = [];
        data.tags.forEach(tag => {
          // Stamp the quad cards even khi giá trị không đổi để hiển thị thời gian mới nhất
          const nowForTag = new Date();
          // Derive device status per tag (prefer tag fields, then DOM fallbacks)
          const barEl = document.getElementById('tag-bar-' + tag.id);
          const indicatorEl = document.querySelector(`.quad-status-indicator[data-pv-tag-id="${tag.id}"]`);
          const quadItemEl = document.querySelector(`.quad-tag-item[data-tag-id="${tag.id}"]`);
          const domStatus = normalizeDeviceStatus(
            barEl?.getAttribute('data-device-status') ||
            indicatorEl?.getAttribute('data-device-status') ||
            quadItemEl?.getAttribute('data-device-status')
          );

          let perTagStatus = normalizeDeviceStatus(
            tag.device_status || tag.status || tag.connection_status || domStatus || payloadDeviceStatus
          );

          // Nếu vẫn unknown/disconnected nhưng liên tục nhận giá trị thì coi như online
          if ((perTagStatus === 'unknown' || perTagStatus === 'disconnected') && (tag.value !== undefined && tag.value !== null)) {
            perTagStatus = 'connected';
          }

          // Update quad status indicators first so quad-only screens still work
          updateQuadStatusIndicator(tag.id, perTagStatus);
          if (perTagStatus === 'disconnected') {
            clearQuadAlarmByTag(tag.id);
          }

          // Update regular tags
          const valEl = document.getElementById('tag-val-' + tag.id);

          // ✅ Update quad tags - tìm TẤT CẢ elements với pattern này
          const quadValElements = document.querySelectorAll(`[id^="quad-tag-val-"][id$="-${tag.id}"]`);

          const updateQuadLastUpdated = () => {
            quadValElements.forEach(quadValEl => {
              const subCard = quadValEl.closest('.quad-sub-card');
              if (!subCard) return;
              const timeEl = subCard.querySelector('.quad-update-time');
              if (timeEl) {
                timeEl.textContent = `Last updated: ${formatTime24h(nowForTag)}`;
              }
            });
          };
          
          if (valEl) {
            const oldVal = valEl.textContent;
            const newVal = perTagStatus === 'disconnected' ? '0' : String(tag.value); // hoặc formatValue(tag.value, tag.datatype)

            if (oldVal !== newVal) {
              pending.push({ valEl, newVal, tag });
            }
          }
          
          // ✅ Loop qua TẤT CẢ quad tag elements với tag.id này
          quadValElements.forEach(quadValEl => {
            const oldQuadVal = quadValEl.textContent;
            // Khi offline thì ép về 0, tránh giữ giá trị cũ
            const newQuadVal = perTagStatus === 'disconnected' ? '0' : String(tag.value);
            
            if (oldQuadVal !== newQuadVal) {
              pending.push({ valEl: quadValEl, newVal: newQuadVal, tag });
              updateQuadLastUpdated();
            }
          });

          // Ngay cả khi giá trị không đổi, vẫn cập nhật thời gian nhận bản tin
          if (quadValElements.length > 0) {
            updateQuadLastUpdated();
          }

          // Always update timestamp and timer if ts exists
          const tsEl = document.getElementById('tag-ts-' + tag.id);
          if (tsEl && tag.ts) {
            tsEl.textContent = tag.ts;
            tsEl.style.color = '';
            tsEl.style.fontWeight = '';
          }
          
          // Reset timer for both regular and quad tags
          if (valEl || quadValElements.length > 0) {
            resetTagTimer(tag.id);
          }

          // 3) Update the meter bar per tag based on device status
          const meterBarEl = document.getElementById('tag-bar-' + tag.id);
          if (meterBarEl) {
            // update data attribute to stay correct on the next render
            meterBarEl.setAttribute('data-device-status', perTagStatus);

            // reset previous classes
            meterBarEl.classList.remove('device-connected', 'device-disconnected', 'device-unknown', 'tag-inactive');

            if (perTagStatus === 'connected') {
              meterBarEl.classList.add('device-connected');
              meterBarEl.style.width = '100%';
            } else if (perTagStatus === 'disconnected') {
              meterBarEl.classList.add('device-disconnected');
              meterBarEl.style.width = '0%';
            }
          }

          // 3b) Update badge Normal/Bad based on device status
          const badgeEl = document.getElementById('tag-badge-' + tag.id);
          if (badgeEl) {
            // Reset classes
            badgeEl.classList.remove('bg-success', 'bg-danger', 'bg-secondary');
            if (perTagStatus === 'connected') {
              badgeEl.classList.add('bg-success');
              badgeEl.textContent = 'Online';
            } else if (perTagStatus === 'disconnected') {
              badgeEl.classList.add('bg-danger');
              badgeEl.textContent = 'Offline';
            } else {
              // Treat unknown like disconnected for badge
              badgeEl.classList.add('bg-danger');
              badgeEl.textContent = 'Offline';
            }
          }
        });

        // 4) Apply value changes (batch) + cập nhật cache
        pending.forEach(({ valEl, newVal, tag }) => {
          valEl.textContent = newVal;
          // clear inactive styling if present
          valEl.style.opacity = '';
          valEl.style.color = '';
          // ★ Cập nhật value cache cho lần load sau
          if (tag && tag.id) {
            updateQuadValueCache(String(tag.id), newVal);
          }
        });
      }
    });

    // Listen for quad alarm events
    socket.on('quad_alarm_event', function (data) {
      console.log('🚨 Quad alarm event received:', data);
      
      const quadId = data.quad_id;
      const column = data.column; // 'left' or 'right'
      const alarmType = data.alarm_type; // 'High' or 'Low'
      const status = data.status; // 'INCOMING' or 'OUTGOING'
      
      // Tìm quad card
      const quadCard = document.querySelector(`.quad-tag-card[data-quad-id="${quadId}"]`);
      if (!quadCard) {
        console.warn(`Quad card ${quadId} not found`);
        return;
      }

      // Tìm sub-card chuẩn bằng data-attribute; fallback về index nếu cần
      const subCardSelector = `.quad-sub-card[data-quad-id="${quadId}"][data-column="${column}"]`;
      let subCard = document.querySelector(subCardSelector);
      if (!subCard) {
        const subCards = quadCard.querySelectorAll('.quad-sub-card');
        subCard = column === 'left' ? subCards[0] : subCards[1];
      }

      if (!subCard) {
        console.warn(`Sub-card for column ${column} not found`);
        return;
      }
      
      // Update alarm state
      if (status === 'INCOMING') {
        // Add alarm class based on type
        if (alarmType === 'High') {
          subCard.classList.add('quad-alarm-high');
          subCard.classList.remove('quad-alarm-low');
        } else {
          subCard.classList.add('quad-alarm-low');
          subCard.classList.remove('quad-alarm-high');
        }
        
        // Get card title from header or use default
        const cardHeader = quadCard.querySelector('.quad-card-title, .quad-card-header-title');
        const cardTitle = cardHeader ? cardHeader.textContent.trim() : `Quad ${quadId}`;
        
        // Get sub-card title
        const subCardTitle = subCard.querySelector('.quad-sub-card-title');
        const groupName = subCardTitle ? subCardTitle.textContent.trim() : (column === 'left' ? 'Group A' : 'Group B');
        
        // Get tag values for display
        const tag1Val = data.tag1_value !== null ? parseFloat(data.tag1_value).toFixed(1) : 'N/A';
        const tag2Val = data.tag2_value !== null ? parseFloat(data.tag2_value).toFixed(1) : 'N/A';
        const threshold = data.threshold !== null ? parseFloat(data.threshold).toFixed(1) : 'N/A';
        const operator = data.operator || '>';
        
        // Create detailed notification message
        const alarmTitle = `🚨 Quad Alarm: ${alarmType} Threshold`;
        const alarmBody = `${cardTitle} - ${groupName}\nPV: ${tag1Val} | SV: ${tag2Val}\nCondition: ${operator} ${threshold}`;
        
        // Show browser notification when allowed; otherwise fallback to bell list only
        if (canUseNativeNotifications()) {
          const notification = new Notification(alarmTitle, {
            body: alarmBody,
            icon: '/static/favicon.ico',
            tag: `quad-alarm-${quadId}-${column}`, // Prevent duplicate notifications
            requireInteraction: false,
            badge: '/static/favicon.ico'
          });
          
          // Auto-close notification after 10 seconds
          setTimeout(() => notification.close(), 10000);
          
          // Click notification to focus window
          notification.onclick = function() {
            window.focus();
            notification.close();
            // Scroll to the quad card
            quadCard.scrollIntoView({ behavior: 'smooth', block: 'center' });
          };
        } else {
          console.warn('Native notifications unavailable (permission denied or insecure context); using bell list only.');
        }
        
        // Add to notification bell list with proper color coding
        if (typeof NotificationSystem !== 'undefined') {
          NotificationSystem.addNotification({
            id: Date.now() + Math.random(),
            serverId: undefined,
            alarmId: `quad-${quadId}-${column}`,
            tagId: null,
            title: `🚨 ${cardTitle} - ${groupName}`,
            message: `${alarmType} alarm: PV=${tag1Val}, SV=${tag2Val} (${operator} ${threshold})`,
            level: 'High',
            timestamp: new Date(),
            status: 'Active',
            read: false
          });
        }
        
        console.log(`✅ Quad alarm notification shown: ${cardTitle} - ${groupName} (${alarmType})`);
        
      } else if (status === 'OUTGOING') {
        // Gỡ class cảnh báo
        subCard.classList.remove('quad-alarm-high', 'quad-alarm-low');

        // Extract card/group names for outgoing notification
        const outCardHeader = quadCard.querySelector('.quad-card-title, .quad-card-header-title');
        const outCardTitle = outCardHeader ? outCardHeader.textContent.trim() : `Quad ${quadId}`;
        const outSubCardTitle = subCard.querySelector('.quad-sub-card-title');
        const outGroupName = outSubCardTitle ? outSubCardTitle.textContent.trim() : (column === 'left' ? 'Group A' : 'Group B');
        const outTag1Val = data.tag1_value !== null && data.tag1_value !== undefined ? parseFloat(data.tag1_value).toFixed(1) : 'N/A';
        const outTag2Val = data.tag2_value !== null && data.tag2_value !== undefined ? parseFloat(data.tag2_value).toFixed(1) : 'N/A';
        const outOperator = data.operator || '>';
        const outThreshold = data.threshold !== null && data.threshold !== undefined ? parseFloat(data.threshold).toFixed(1) : 'N/A';

        // Hiển thị thông báo clear (nếu NotificationSystem sẵn có)
        if (typeof NotificationSystem !== 'undefined') {
          NotificationSystem.addNotification({
            id: Date.now() + Math.random(),
            serverId: undefined,
            alarmId: `quad-${quadId}-${column}`,
            tagId: null,
            title: `✅ ${outCardTitle} - ${outGroupName}`,
            message: `${alarmType} alarm cleared: PV=${outTag1Val}, SV=${outTag2Val} (${outOperator} ${outThreshold})`,
            level: 'Info', 
            timestamp: new Date(),
            status: 'Cleared',
            read: false
          });
        }
      }
    });


    // Join subdashboard room for targeted updates
    socket.emit('join', { room: `subdashboard_${currentSubdashId}` });

    // Initialize quad status indicators based on current device status
    document.querySelectorAll('.quad-status-indicator').forEach(indicator => {
      const pvTagId = indicator.getAttribute('data-pv-tag-id');
      if (pvTagId) {
        const barEl = document.getElementById('tag-bar-' + pvTagId);
        const indicatorStatus = indicator.getAttribute('data-device-status');
        const quadItem = document.querySelector(`.quad-tag-item[data-tag-id="${pvTagId}"]`);
        const quadItemStatus = quadItem ? quadItem.getAttribute('data-device-status') : null;
        const deviceStatus = normalizeDeviceStatus(
          indicatorStatus || quadItemStatus || (barEl ? barEl.getAttribute('data-device-status') : 'unknown')
        );
        updateQuadStatusIndicator(parseInt(pvTagId), deviceStatus);
      }
    });

    // Initialize tag tracking and start timeout checker
    document.querySelectorAll('[id^="tag-val-"]').forEach(el => {
      const tagId = parseInt(el.id.replace('tag-val-', ''));
      if (isNaN(tagId)) return;
      const barEl = document.getElementById('tag-bar-' + tagId);
      const deviceStatus = barEl ? barEl.getAttribute('data-device-status') : null;
      // If device is offline, we already dimmed it and skip timer start here
      if (deviceStatus === 'disconnected') return;
      // For connected/unknown, keep current displayed value and start timer
      resetTagTimer(tagId);
    });

    // Handle reconnection - rejoin room
    socket.on('connect', function () {
      console.log('[Subdash] Socket connected, joining room subdashboard_' + currentSubdashId);
      socket.emit('join', { room: `subdashboard_${currentSubdashId}` });
      socketConnected = false; // Reset flag to redetect updates

      // Re-sync quad alarm colors from server after reconnection.
      // Alarm events may have been missed while socket was disconnected.
      setTimeout(fetchAndApplyQuadAlarms, 1500);

      // Restart polling check: if no socket data within 5s after connect, start polling
      _startPollingFallbackCheck();
    });

    socket.on('disconnect', function () {
      console.warn('[Subdash] Socket disconnected');
      socketConnected = false;
      // Start polling immediately on disconnect to keep UI alive
      _ensurePolling();
    });

    // Listen for global reconnect event (from visibilitychange / heartbeat in base.html)
    window.addEventListener('socket_reconnected', function () {
      console.log('[Subdash] Global reconnect event, rejoining room subdashboard_' + currentSubdashId);
      socket.emit('join', { room: `subdashboard_${currentSubdashId}` });
      socketConnected = false;
      _startPollingFallbackCheck();

      // Re-sync quad alarm colors from server after global reconnect
      setTimeout(fetchAndApplyQuadAlarms, 1500);
    });

    // Update global heartbeat timestamp whenever we receive modbus data
    socket.on('modbus_update', function () {
      if (typeof _lastSocketDataTime !== 'undefined') {
        _lastSocketDataTime = Date.now();
      }
    });

  } else {
    console.warn('Socket not available, falling back to polling only');
  }

  // ---- Polling fallback helpers ----
  function _ensurePolling() {
    if (!pollingInterval) {
      console.warn('[Subdash] Starting polling fallback');
      pollingInterval = setInterval(function () {
        if (window.App && window.App.refreshTags) {
          window.App.refreshTags();
        }
      }, 2000);
    }
  }

  function _stopPolling() {
    if (pollingInterval) {
      console.log('[Subdash] Stopping polling fallback (socket active)');
      clearInterval(pollingInterval);
      pollingInterval = null;
    }
  }

  function _startPollingFallbackCheck() {
    // Wait 5s, then check if socket is delivering data; if not, start polling
    setTimeout(function () {
      if (socketConnected) {
        _stopPolling();
      } else {
        console.warn('[Subdash] No socket data after 5s, enabling polling');
        _ensurePolling();
      }
    }, 5000);
  }

  // Polling fallback - check after DOM is ready
  document.addEventListener('DOMContentLoaded', function () {
    _startPollingFallbackCheck();

    // Periodic check: switch between socket and polling as needed
    setInterval(function () {
      if (socketConnected) {
        _stopPolling();
      } else {
        _ensurePolling();
      }
    }, 15000); // Re-evaluate every 15 seconds
  });

  // Cleanup on page unload to prevent memory leaks
  window.addEventListener('beforeunload', function () {
    if (pollingInterval) {
      clearInterval(pollingInterval);
    }

    // Clear quad alarm sync interval
    if (typeof _quadAlarmSyncInterval !== 'undefined') {
      clearInterval(_quadAlarmSyncInterval);
    }

    // Clear all tag timers
    tagTimers.forEach((timeoutId) => {
      clearTimeout(timeoutId);
    });
    tagTimers.clear();
  });

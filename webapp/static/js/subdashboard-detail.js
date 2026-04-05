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

    // Apply per-tag alarm states for Qtag6, Single3, PV Only cards
    applyActiveTagAlarms();

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

  // Periodic alarm state sync every 10 seconds to keep colors in sync
  // Reduced from 30s to 10s to minimize delay when socket.io events are missed
  const _quadAlarmSyncInterval = setInterval(fetchAndApplyQuadAlarms, 10000);

  // ============================================================
  // ★ PER-TAG ALARM STATE for Qtag6, Single3, PV Only cards
  // Reuses existing alarm_events / alarm_rules system.
  // ============================================================

  /**
   * Determine alarm CSS class based on alarm level from alarm_rules.
   * Priority: Critical/High → alarm-high, Low/Medium → alarm-low
   */
  function getAlarmClassForLevel(level) {
    if (!level) return 'high';
    const l = level.toLowerCase();
    if (l === 'critical' || l === 'high') return 'high';
    if (l === 'low' || l === 'medium') return 'low';
    return 'high'; // Default to high for unknown levels
  }

  /**
   * Apply alarm visual to a specific tag element and its parent card.
   * @param {number} tagId - The tag ID
   * @param {string} alarmClass - 'high' or 'low'
   * @param {object} alarmInfo - Optional alarm details for tooltip {alarm_name, level, value, operator, threshold}
   */
  function applyTagAlarmVisual(tagId, alarmClass, alarmInfo) {
    // Find all elements displaying this tag across all Qtag card types
    const tagElements = document.querySelectorAll(
      `.qtag6-tag-item[data-tag-id="${tagId}"],` +
      `.qtag-single-tag-item[data-tag-id="${tagId}"]`
    );

    tagElements.forEach(tagEl => {
      // Add per-tag alarm indicator
      tagEl.classList.add('tag-alarm-active', `tag-alarm-${alarmClass}`);

      // Add tooltip with alarm info
      if (alarmInfo) {
        const tooltipText = `⚠️ ${alarmInfo.alarm_name || 'Alarm'}\nLevel: ${alarmInfo.level || 'High'}\nValue: ${alarmInfo.value ?? 'N/A'}\nCondition: ${alarmInfo.operator || '>'} ${alarmInfo.threshold ?? 'N/A'}`;
        tagEl.setAttribute('title', tooltipText);
      }

      // Find and mark the parent card with highest-severity alarm
      const card = tagEl.closest('.qtag6-card, .qtag-single-sub-card');
      if (card) {
        applyCardAlarmState(card, alarmClass);
      }
    });
  }

  /**
   * Remove alarm visual from a tag element and update parent card state.
   * @param {number} tagId - The tag ID
   */
  function removeTagAlarmVisual(tagId) {
    const tagElements = document.querySelectorAll(
      `.qtag6-tag-item[data-tag-id="${tagId}"],` +
      `.qtag-single-tag-item[data-tag-id="${tagId}"]`
    );

    tagElements.forEach(tagEl => {
      tagEl.classList.remove('tag-alarm-active', 'tag-alarm-high', 'tag-alarm-low');
      tagEl.removeAttribute('title');

      // Re-evaluate parent card alarm state
      const card = tagEl.closest('.qtag6-card, .qtag-single-sub-card');
      if (card) {
        recalcCardAlarmState(card);
      }
    });
  }

  /**
   * Apply alarm state to a card based on priority rule.
   * If card already has higher severity, keep it.
   * Priority: high > low > none
   */
  function applyCardAlarmState(card, newAlarmClass) {
    // Determine card type and appropriate CSS prefix
    const isQtag6 = card.classList.contains('qtag6-card');
    const isSingle = card.classList.contains('qtag-single-sub-card');

    if (isQtag6) {
      // For Qtag6: alarm goes on sub-cards (left/right columns)
      // Find which sub-card contains the alarming tag
      const subCards = card.querySelectorAll('.qtag6-sub-card');
      subCards.forEach(sub => {
        const hasAlarmTag = sub.querySelector('.tag-alarm-active');
        if (hasAlarmTag) {
          const hasHigh = sub.querySelector('.tag-alarm-high');
          if (hasHigh) {
            sub.classList.remove('qtag6-alarm-low');
            sub.classList.add('qtag6-alarm-high');
          } else {
            if (!sub.classList.contains('qtag6-alarm-high')) {
              sub.classList.add('qtag6-alarm-low');
            }
          }
        }
      });
    } else if (isSingle) {
      // For Single3/PV Only: alarm goes on the card itself
      const hasHigh = card.querySelector('.tag-alarm-high');
      if (hasHigh) {
        card.classList.remove('qtag-single-alarm-low');
        card.classList.add('qtag-single-alarm-high');
      } else {
        if (!card.classList.contains('qtag-single-alarm-high')) {
          card.classList.add('qtag-single-alarm-low');
        }
      }
    }
  }

  /**
   * Recalculate alarm state for a card after a tag alarm is removed.
   * Checks remaining alarming tags and applies highest severity.
   */
  function recalcCardAlarmState(card) {
    const isQtag6 = card.classList.contains('qtag6-card');
    const isSingle = card.classList.contains('qtag-single-sub-card');

    if (isQtag6) {
      card.querySelectorAll('.qtag6-sub-card').forEach(sub => {
        sub.classList.remove('qtag6-alarm-high', 'qtag6-alarm-low');
        const hasHigh = sub.querySelector('.tag-alarm-high');
        const hasLow = sub.querySelector('.tag-alarm-low');
        if (hasHigh) {
          sub.classList.add('qtag6-alarm-high');
        } else if (hasLow) {
          sub.classList.add('qtag6-alarm-low');
        }
      });
    } else if (isSingle) {
      card.classList.remove('qtag-single-alarm-high', 'qtag-single-alarm-low');
      const hasHigh = card.querySelector('.tag-alarm-high');
      const hasLow = card.querySelector('.tag-alarm-low');
      if (hasHigh) {
        card.classList.add('qtag-single-alarm-high');
      } else if (hasLow) {
        card.classList.add('qtag-single-alarm-low');
      }
    }
  }

  /**
   * Apply active tag alarms on page load from embedded data attribute.
   */
  function applyActiveTagAlarms() {
    const dataEl = document.getElementById('tag-alarm-data');
    let activeAlarms = [];
    if (dataEl) {
      try {
        const dataStr = dataEl.getAttribute('data-active-alarms');
        activeAlarms = dataStr ? JSON.parse(dataStr) : [];
      } catch (e) {
        console.error('Failed to parse active tag alarms:', e);
        activeAlarms = [];
      }
    }

    console.log(`🔔 [TagAlarm] Applying ${activeAlarms.length} active tag alarms on page load`);

    activeAlarms.forEach(alarm => {
      const tagId = alarm.tag_id;
      const alarmClass = getAlarmClassForLevel(alarm.level);
      applyTagAlarmVisual(tagId, alarmClass, alarm);
    });
  }

  /**
   * Fetch active tag alarms from server API and sync visual state.
   * Similar to fetchAndApplyQuadAlarms but for per-tag alarms.
   */
  function fetchAndApplyTagAlarms() {
    const subdashId = currentSubdashId;
    if (!subdashId) return;

    fetch(`/subdash/${subdashId}/api/active_tag_alarms`)
      .then(res => res.json())
      .then(data => {
        if (!data.success || !Array.isArray(data.alarms)) return;

        // Build a set of tag IDs with active alarms
        const serverAlarmMap = new Map();
        data.alarms.forEach(alarm => {
          serverAlarmMap.set(alarm.tag_id, alarm);
        });

        // Clear all existing tag alarm visuals first
        document.querySelectorAll('.tag-alarm-active').forEach(el => {
          el.classList.remove('tag-alarm-active', 'tag-alarm-high', 'tag-alarm-low');
          el.removeAttribute('title');
        });

        // Clear card-level alarm classes
        document.querySelectorAll('.qtag6-sub-card').forEach(el => {
          el.classList.remove('qtag6-alarm-high', 'qtag6-alarm-low');
        });
        document.querySelectorAll('.qtag-single-sub-card').forEach(el => {
          el.classList.remove('qtag-single-alarm-high', 'qtag-single-alarm-low');
        });

        // Re-apply from server data
        data.alarms.forEach(alarm => {
          const alarmClass = getAlarmClassForLevel(alarm.level);
          applyTagAlarmVisual(alarm.tag_id, alarmClass, alarm);
        });

        // After system alarms applied, run Single3 PV vs SV fallback check
        evaluateSingle3PvSvFallback(serverAlarmMap);

        console.log(`[TagAlarmSync] Synced ${data.alarms.length} active tag alarms from server`);
      })
      .catch(err => {
        console.warn('[TagAlarmSync] Failed to fetch active tag alarms:', err);
      });
  }

  /**
   * Single3 PV vs SV HIGH/LOW fallback comparison.
   * If PV tag does NOT have a system alarm active, compare PV value with
   * SV HIGH and SV LOW displayed on the card. If PV > SV HIGH → alarm-high,
   * if PV < SV LOW → alarm-low.
   * Priority: system alarm > PV vs SV comparison.
   */
  function evaluateSingle3PvSvFallback(systemAlarmMap) {
    document.querySelectorAll('.qtag-single-sub-card[data-qtag-single3-id]').forEach(card => {
      const pvTagId = card.getAttribute('data-pv-tag-id');
      if (!pvTagId) return;

      // Skip if PV tag already has a system alarm active
      if (systemAlarmMap && systemAlarmMap.has(parseInt(pvTagId))) return;

      // Get PV value
      const pvEl = card.querySelector('.qtag-single-tag-value');
      if (!pvEl) return;
      const pvVal = parseFloat(pvEl.textContent);
      if (isNaN(pvVal)) return;

      // Get SV HIGH value
      const svHighEl = card.querySelector('.qtag-single-sv-high');
      const svHighVal = svHighEl ? parseFloat(svHighEl.textContent) : NaN;

      // Get SV LOW value
      const svLowEl = card.querySelector('.qtag-single-sv-low');
      const svLowVal = svLowEl ? parseFloat(svLowEl.textContent) : NaN;

      // Compare PV against SV limits
      let fallbackAlarm = null;
      if (!isNaN(svHighVal) && pvVal > svHighVal) {
        fallbackAlarm = 'high';
      } else if (!isNaN(svLowVal) && pvVal < svLowVal) {
        fallbackAlarm = 'low';
      }

      // Apply or remove fallback alarm visual
      const pvTagItem = card.querySelector(`.qtag-single-tag-item[data-tag-id="${pvTagId}"]`);
      if (fallbackAlarm && pvTagItem) {
        pvTagItem.classList.add('tag-alarm-active', `tag-alarm-${fallbackAlarm}`);
        const condText = fallbackAlarm === 'high'
          ? `PV (${pvVal}) > SV HIGH (${svHighVal})`
          : `PV (${pvVal}) < SV LOW (${svLowVal})`;
        pvTagItem.setAttribute('title', `⚠️ PV vs SV: ${condText}`);

        // Apply to card
        if (fallbackAlarm === 'high') {
          card.classList.remove('qtag-single-alarm-low');
          card.classList.add('qtag-single-alarm-high');
        } else {
          if (!card.classList.contains('qtag-single-alarm-high')) {
            card.classList.add('qtag-single-alarm-low');
          }
        }
      }
    });
  }

  // Expose for external use
  window.fetchAndApplyTagAlarms = fetchAndApplyTagAlarms;

  // Periodic tag alarm sync alongside quad alarm sync
  const _tagAlarmSyncInterval = setInterval(fetchAndApplyTagAlarms, 10000);

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

          // ✅ Update Qtag6 values - elements with pattern qtag6-val-{cardId}-{tagId}
          const qtag6ValElements = document.querySelectorAll(`[id^="qtag6-val-"][id$="-${tag.id}"]`);
          qtag6ValElements.forEach(el => {
            const newVal = perTagStatus === 'disconnected' ? '0' : String(tag.value);
            if (el.textContent !== newVal) {
              pending.push({ valEl: el, newVal: newVal, tag });
            }
            // Update last updated time for the sub-card
            const subCard = el.closest('.qtag6-sub-card');
            if (subCard) {
              const timeEl = subCard.querySelector('.qtag6-update-time');
              if (timeEl) {
                timeEl.textContent = `Last updated: ${formatTime24h(nowForTag)}`;
              }
            }
          });

          // ✅ Update Qtag Single3 PV values
          const single3PvElements = document.querySelectorAll(`[id^="qtag-single3-pv-"][id$="-${tag.id}"]`);
          single3PvElements.forEach(el => {
            const newVal = perTagStatus === 'disconnected' ? '0' : String(tag.value);
            if (el.textContent !== newVal) {
              pending.push({ valEl: el, newVal: newVal, tag });
            }
            // Update last updated time
            const card = el.closest('.qtag-single-sub-card');
            if (card) {
              const timeEl = card.querySelector('.qtag-single-update-time');
              if (timeEl) {
                timeEl.textContent = `Last updated: ${formatTime24h(nowForTag)}`;
              }
            }
          });

          // ✅ Update Qtag Single3 SV HIGH/LOW (matched by data-tag-id attribute)
          const single3SvElements = document.querySelectorAll(`.qtag-single-sv-low[data-tag-id="${tag.id}"], .qtag-single-sv-high[data-tag-id="${tag.id}"]`);
          single3SvElements.forEach(el => {
            const newVal = perTagStatus === 'disconnected' ? '0' : String(tag.value);
            if (el.textContent !== newVal) {
              pending.push({ valEl: el, newVal: newVal, tag });
            }
          });

          // ✅ Update Qtag PV Only values
          const pvOnlyElements = document.querySelectorAll(`[id^="qtag-pv-val-"][id$="-${tag.id}"]`);
          pvOnlyElements.forEach(el => {
            const newVal = perTagStatus === 'disconnected' ? '0' : String(tag.value);
            if (el.textContent !== newVal) {
              pending.push({ valEl: el, newVal: newVal, tag });
            }
            // Update last updated time
            const card = el.closest('.qtag-single-sub-card');
            if (card) {
              const timeEl = card.querySelector('.qtag-single-update-time');
              if (timeEl) {
                timeEl.textContent = `Last updated: ${formatTime24h(nowForTag)}`;
              }
            }
          });

          // Check if new card types also need timer reset
          if (qtag6ValElements.length > 0 || single3PvElements.length > 0 || pvOnlyElements.length > 0 || single3SvElements.length > 0) {
            resetTagTimer(tag.id);
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
        const alarmTitle = `🚨Quad Alarm: ${alarmType} Threshold`;
        const alarmBody = `${cardTitle} - ${groupName}\nPV: ${tag1Val} | SV: ${tag2Val}\nCondition: ${operator} ${threshold}`;
        
        // Show browser notification when allowed; otherwise fallback to bell list only
        if (typeof Notification !== 'undefined' && window.isSecureContext && Notification.permission === 'granted') {
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
        
        // Bell notification is handled instantly by notifications.js via quad_alarm_event socket listener
        
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

        // Bell notification is handled instantly by notifications.js via quad_alarm_event socket listener

        console.log(`✅ Quad alarm cleared: ${outCardTitle} - ${outGroupName} (${alarmType})`);
      }
    });

    // Listen for per-tag alarm events (from existing alarm_rules system)
    // Applies alarm visuals to Qtag6, Single3, PV Only cards
    socket.on('alarm_event', function (data) {
      console.log('🔔 Tag alarm event received:', data);

      const tagId = data.tag_id;
      const status = data.status; // 'INCOMING' or 'OUTGOING'
      const level = data.level;   // 'Low', 'Medium', 'High', 'Critical'

      if (!tagId) return;

      if (status === 'INCOMING') {
        const alarmClass = getAlarmClassForLevel(level);
        applyTagAlarmVisual(tagId, alarmClass, {
          alarm_name: data.tag_name || data.title,
          level: level,
          value: data.value,
          operator: null,
          threshold: null
        });

        console.log(`✅ Tag alarm applied: tag ${tagId} - ${level}`);

      } else if (status === 'OUTGOING') {
        removeTagAlarmVisual(tagId);
        console.log(`✅ Tag alarm cleared: tag ${tagId}`);
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

      // Re-sync alarm colors from server after reconnection.
      // Alarm events may have been missed while socket was disconnected.
      setTimeout(fetchAndApplyQuadAlarms, 1500);
      setTimeout(fetchAndApplyTagAlarms, 1500);

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

      // Re-sync alarm colors from server after global reconnect
      setTimeout(fetchAndApplyQuadAlarms, 1500);
      setTimeout(fetchAndApplyTagAlarms, 1500);
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

  // ============================================================
  // ★ SV Type Toggle for Single3 modals (fixed vs tag)
  // ============================================================
  document.addEventListener('change', function (e) {
    if (!e.target.classList.contains('sv-type-select')) return;
    const target = e.target.dataset.target; // e.g. 'sv_high', 'sv_low', 'edit_sv_high', 'edit_sv_low'
    const val = e.target.value;
    const fixedGroup = document.querySelector(`.${target.replace(/_/g, '-')}-fixed-group`);
    const tagGroup = document.querySelector(`.${target.replace(/_/g, '-')}-tag-group`);
    if (fixedGroup && tagGroup) {
      fixedGroup.style.display = val === 'fixed' ? '' : 'none';
      tagGroup.style.display = val === 'tag' ? '' : 'none';
    }
  });

  // ============================================================
  // ★ Mutual exclusion: group_id <-> new_group_name in add forms
  // ============================================================
  document.querySelectorAll('[name="group_id"]').forEach(sel => {
    sel.addEventListener('change', function () {
      if (this.value) {
        const nameInput = this.closest('form').querySelector('[name="new_group_name"]');
        if (nameInput) nameInput.value = '';
      }
    });
  });
  document.querySelectorAll('[name="new_group_name"]').forEach(inp => {
    inp.addEventListener('input', function () {
      if (this.value.trim()) {
        const groupSel = this.closest('form').querySelector('[name="group_id"]');
        if (groupSel) groupSel.value = '';
      }
    });
  });

  // ============================================================
  // ★ CRUD: Add Qtag6 Card
  // ============================================================
  const addQtag6Form = document.getElementById('addQtag6Form');
  if (addQtag6Form) {
    addQtag6Form.addEventListener('submit', function (e) {
      e.preventDefault();
      const form = e.target;
      const errorEl = document.getElementById('qtag6-error-message');
      errorEl.style.display = 'none';

      // Validate all 6 tags
      for (let i = 1; i <= 6; i++) {
        if (!form.querySelector(`[name="tag${i}_id"]`).value) {
          errorEl.textContent = 'Please select all 6 tags';
          errorEl.style.display = 'block';
          return;
        }
      }

      const groupId = form.querySelector('[name="group_id"]').value;
      const newGroupName = form.querySelector('[name="new_group_name"]').value.trim();
      if (!groupId && !newGroupName) {
        errorEl.textContent = 'Please select a group or enter a new group name';
        errorEl.style.display = 'block';
        return;
      }

      Swal.fire({ title: 'Adding Qtag6 Card...', allowOutsideClick: false, didOpen: () => Swal.showLoading() });

      const formData = new FormData(form);
      fetch(`/subdash/${currentSubdashId}/add_qtag6`, {
        method: 'POST',
        body: formData
      })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          Swal.fire({ title: 'Success!', text: 'Qtag6 card added', icon: 'success', timer: 2000 }).then(() => location.reload());
        } else {
          Swal.fire({ title: 'Error', text: data.error || 'Failed to add Qtag6 card', icon: 'error' });
        }
      })
      .catch(err => {
        Swal.fire({ title: 'Error', text: err.message, icon: 'error' });
      });
    });
  }

  // ============================================================
  // ★ CRUD: Add Qtag Single3 Card
  // ============================================================
  const addSingle3Form = document.getElementById('addQtagSingle3Form');
  if (addSingle3Form) {
    addSingle3Form.addEventListener('submit', function (e) {
      e.preventDefault();
      const form = e.target;
      const errorEl = document.getElementById('single3-error-message');
      errorEl.style.display = 'none';

      if (!form.querySelector('[name="pv_tag_id"]').value) {
        errorEl.textContent = 'Please select a PV tag';
        errorEl.style.display = 'block';
        return;
      }

      const groupId = form.querySelector('[name="group_id"]').value;
      const newGroupName = form.querySelector('[name="new_group_name"]').value.trim();
      if (!groupId && !newGroupName) {
        errorEl.textContent = 'Please select a group or enter a new group name';
        errorEl.style.display = 'block';
        return;
      }

      Swal.fire({ title: 'Adding Single3 Card...', allowOutsideClick: false, didOpen: () => Swal.showLoading() });

      const formData = new FormData(form);
      fetch(`/subdash/${currentSubdashId}/add_qtag_single3`, {
        method: 'POST',
        body: formData
      })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          Swal.fire({ title: 'Success!', text: 'Single3 card added', icon: 'success', timer: 2000 }).then(() => location.reload());
        } else {
          Swal.fire({ title: 'Error', text: data.error || 'Failed to add Single3 card', icon: 'error' });
        }
      })
      .catch(err => {
        Swal.fire({ title: 'Error', text: err.message, icon: 'error' });
      });
    });
  }

  // ============================================================
  // ★ CRUD: Add Qtag PV Only Card
  // ============================================================
  const addPvForm = document.getElementById('addQtagPvForm');
  if (addPvForm) {
    addPvForm.addEventListener('submit', function (e) {
      e.preventDefault();
      const form = e.target;
      const errorEl = document.getElementById('pv-error-message');
      errorEl.style.display = 'none';

      if (!form.querySelector('[name="pv_tag_id"]').value) {
        errorEl.textContent = 'Please select a PV tag';
        errorEl.style.display = 'block';
        return;
      }

      const groupId = form.querySelector('[name="group_id"]').value;
      const newGroupName = form.querySelector('[name="new_group_name"]').value.trim();
      if (!groupId && !newGroupName) {
        errorEl.textContent = 'Please select a group or enter a new group name';
        errorEl.style.display = 'block';
        return;
      }

      Swal.fire({ title: 'Adding PV Only Card...', allowOutsideClick: false, didOpen: () => Swal.showLoading() });

      const formData = new FormData(form);
      fetch(`/subdash/${currentSubdashId}/add_qtag_pv`, {
        method: 'POST',
        body: formData
      })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          Swal.fire({ title: 'Success!', text: 'PV Only card added', icon: 'success', timer: 2000 }).then(() => location.reload());
        } else {
          Swal.fire({ title: 'Error', text: data.error || 'Failed to add PV Only card', icon: 'error' });
        }
      })
      .catch(err => {
        Swal.fire({ title: 'Error', text: err.message, icon: 'error' });
      });
    });
  }

  // ============================================================
  // ★ DELETE: Qtag6 Card
  // ============================================================
  document.addEventListener('click', function (e) {
    const btn = e.target.closest('.delete-qtag6-btn');
    if (!btn) return;
    e.preventDefault();
    const cardId = btn.dataset.cardId;

    Swal.fire({
      title: 'Delete Qtag6 Card?',
      text: 'This action cannot be undone.',
      icon: 'warning',
      showCancelButton: true,
      confirmButtonColor: '#dc3545',
      confirmButtonText: 'Yes, delete it!',
      cancelButtonText: 'Cancel'
    }).then(result => {
      if (!result.isConfirmed) return;
      Swal.fire({ title: 'Deleting...', allowOutsideClick: false, didOpen: () => Swal.showLoading() });

      fetch(`/subdash/${currentSubdashId}/delete_qtag6/${cardId}`, { method: 'DELETE' })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          Swal.fire({ title: 'Deleted!', icon: 'success', timer: 2000 }).then(() => location.reload());
        } else {
          Swal.fire({ title: 'Error', text: data.error || 'Failed to delete', icon: 'error' });
        }
      })
      .catch(err => Swal.fire({ title: 'Error', text: err.message, icon: 'error' }));
    });
  });

  // ============================================================
  // ★ DELETE: Qtag Single3 Card
  // ============================================================
  document.addEventListener('click', function (e) {
    const btn = e.target.closest('.delete-qtag-single3-btn');
    if (!btn) return;
    e.preventDefault();
    const cardId = btn.dataset.cardId;

    Swal.fire({
      title: 'Delete Single3 Card?',
      text: 'This action cannot be undone.',
      icon: 'warning',
      showCancelButton: true,
      confirmButtonColor: '#dc3545',
      confirmButtonText: 'Yes, delete it!',
      cancelButtonText: 'Cancel'
    }).then(result => {
      if (!result.isConfirmed) return;
      Swal.fire({ title: 'Deleting...', allowOutsideClick: false, didOpen: () => Swal.showLoading() });

      fetch(`/subdash/${currentSubdashId}/delete_qtag_single3/${cardId}`, { method: 'DELETE' })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          Swal.fire({ title: 'Deleted!', icon: 'success', timer: 2000 }).then(() => location.reload());
        } else {
          Swal.fire({ title: 'Error', text: data.error || 'Failed to delete', icon: 'error' });
        }
      })
      .catch(err => Swal.fire({ title: 'Error', text: err.message, icon: 'error' }));
    });
  });

  // ============================================================
  // ★ DELETE: Qtag PV Only Card
  // ============================================================
  document.addEventListener('click', function (e) {
    const btn = e.target.closest('.delete-qtag-pv-btn');
    if (!btn) return;
    e.preventDefault();
    const cardId = btn.dataset.cardId;

    Swal.fire({
      title: 'Delete PV Only Card?',
      text: 'This action cannot be undone.',
      icon: 'warning',
      showCancelButton: true,
      confirmButtonColor: '#dc3545',
      confirmButtonText: 'Yes, delete it!',
      cancelButtonText: 'Cancel'
    }).then(result => {
      if (!result.isConfirmed) return;
      Swal.fire({ title: 'Deleting...', allowOutsideClick: false, didOpen: () => Swal.showLoading() });

      fetch(`/subdash/${currentSubdashId}/delete_qtag_pv/${cardId}`, { method: 'DELETE' })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          Swal.fire({ title: 'Deleted!', icon: 'success', timer: 2000 }).then(() => location.reload());
        } else {
          Swal.fire({ title: 'Error', text: data.error || 'Failed to delete', icon: 'error' });
        }
      })
      .catch(err => Swal.fire({ title: 'Error', text: err.message, icon: 'error' }));
    });
  });

  // ============================================================
  // ★ RENAME: Qtag6 Card (card title, left title, right title)
  // ============================================================
  document.addEventListener('click', function (e) {
    const btn = e.target.closest('.rename-qtag6-btn');
    if (!btn) return;
    e.preventDefault();
    const cardId = btn.dataset.cardId;
    const target = btn.dataset.target || 'card'; // 'card', 'left', 'right'
    const card = btn.closest('.qtag6-card') || document.querySelector(`.qtag6-card[data-qtag6-id="${cardId}"]`);
    if (!card) return;

    let titleEl, currentTitle;
    if (target === 'left') {
      titleEl = card.querySelector('.qtag6-sub-card[data-column="left"] .qtag6-sub-title');
      currentTitle = titleEl ? titleEl.textContent.trim() : 'Group A';
    } else if (target === 'right') {
      titleEl = card.querySelector('.qtag6-sub-card[data-column="right"] .qtag6-sub-title');
      currentTitle = titleEl ? titleEl.textContent.trim() : 'Group B';
    } else {
      titleEl = card.querySelector('.fw-semibold');
      currentTitle = titleEl ? titleEl.textContent.trim() : 'Qtag6 Card';
    }

    Swal.fire({
      title: 'Đổi tên',
      input: 'text',
      inputLabel: 'Tiêu đề mới:',
      inputValue: currentTitle,
      showCancelButton: true,
      confirmButtonText: 'Cập nhật',
      cancelButtonText: 'Hủy',
      inputValidator: v => (!v || !v.trim()) ? 'Vui lòng nhập tiêu đề' : null
    }).then(result => {
      if (!result.isConfirmed) return;
      Swal.fire({ title: 'Đang cập nhật...', allowOutsideClick: false, didOpen: () => Swal.showLoading() });

      fetch(`/subdash/${currentSubdashId}/qtag6/${cardId}/rename`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ target: target, title: result.value.trim() })
      })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          if (titleEl) titleEl.textContent = result.value.trim();
          Swal.fire({ icon: 'success', title: 'Thành công', timer: 1500, showConfirmButton: false }).then(() => location.reload());
        } else {
          Swal.fire({ icon: 'error', title: 'Lỗi', text: data.error || 'Không thể cập nhật' });
        }
      })
      .catch(err => Swal.fire({ icon: 'error', title: 'Lỗi', text: err.message }));
    });
  });

  // ============================================================
  // ★ RENAME: Qtag Single3 Card
  // ============================================================
  document.addEventListener('click', function (e) {
    const btn = e.target.closest('.rename-qtag-single3-btn');
    if (!btn) return;
    e.preventDefault();
    const cardId = btn.dataset.cardId;
    const card = btn.closest('.qtag-single-sub-card') || document.querySelector(`.qtag-single-sub-card[data-qtag-single3-id="${cardId}"]`);
    if (!card) return;

    const titleEl = card.querySelector('.qtag-single-sub-title');
    const currentTitle = titleEl ? titleEl.textContent.trim() : 'Single3 Card';

    Swal.fire({
      title: 'Đổi tên',
      input: 'text',
      inputLabel: 'Tiêu đề mới:',
      inputValue: currentTitle,
      showCancelButton: true,
      confirmButtonText: 'Cập nhật',
      cancelButtonText: 'Hủy',
      inputValidator: v => (!v || !v.trim()) ? 'Vui lòng nhập tiêu đề' : null
    }).then(result => {
      if (!result.isConfirmed) return;
      Swal.fire({ title: 'Đang cập nhật...', allowOutsideClick: false, didOpen: () => Swal.showLoading() });

      fetch(`/subdash/${currentSubdashId}/qtag_single3/${cardId}/rename`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ title: result.value.trim() })
      })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          if (titleEl) titleEl.textContent = result.value.trim();
          Swal.fire({ icon: 'success', title: 'Thành công', timer: 1500, showConfirmButton: false }).then(() => location.reload());
        } else {
          Swal.fire({ icon: 'error', title: 'Lỗi', text: data.error || 'Không thể cập nhật' });
        }
      })
      .catch(err => Swal.fire({ icon: 'error', title: 'Lỗi', text: err.message }));
    });
  });

  // ============================================================
  // ★ RENAME: Qtag PV Only Card
  // ============================================================
  document.addEventListener('click', function (e) {
    const btn = e.target.closest('.rename-qtag-pv-btn');
    if (!btn) return;
    e.preventDefault();
    const cardId = btn.dataset.cardId;
    const card = btn.closest('.qtag-single-sub-card') || document.querySelector(`.qtag-single-sub-card[data-qtag-pv-id="${cardId}"]`);
    if (!card) return;

    const titleEl = card.querySelector('.qtag-single-sub-title');
    const currentTitle = titleEl ? titleEl.textContent.trim() : 'PV Only';

    Swal.fire({
      title: 'Đổi tên',
      input: 'text',
      inputLabel: 'Tiêu đề mới:',
      inputValue: currentTitle,
      showCancelButton: true,
      confirmButtonText: 'Cập nhật',
      cancelButtonText: 'Hủy',
      inputValidator: v => (!v || !v.trim()) ? 'Vui lòng nhập tiêu đề' : null
    }).then(result => {
      if (!result.isConfirmed) return;
      Swal.fire({ title: 'Đang cập nhật...', allowOutsideClick: false, didOpen: () => Swal.showLoading() });

      fetch(`/subdash/${currentSubdashId}/qtag_pv/${cardId}/rename`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ title: result.value.trim() })
      })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          if (titleEl) titleEl.textContent = result.value.trim();
          Swal.fire({ icon: 'success', title: 'Thành công', timer: 1500, showConfirmButton: false }).then(() => location.reload());
        } else {
          Swal.fire({ icon: 'error', title: 'Lỗi', text: data.error || 'Không thể cập nhật' });
        }
      })
      .catch(err => Swal.fire({ icon: 'error', title: 'Lỗi', text: err.message }));
    });
  });

  // ============================================================
  // ★ EDIT TAGS: Qtag6 - Open modal and populate
  // ============================================================
  document.addEventListener('click', function (e) {
    const btn = e.target.closest('.edit-qtag6-btn');
    if (!btn) return;
    e.preventDefault();

    document.getElementById('editQtag6Id').value = btn.dataset.cardId;
    // Populate left/right titles from card data attributes
    const card = btn.closest('.qtag6-card');
    if (card) {
      document.getElementById('editQtag6LeftTitle').value = card.dataset.leftTitle || '';
      document.getElementById('editQtag6RightTitle').value = card.dataset.rightTitle || '';
    }
    // Set current tag selections
    for (let i = 1; i <= 6; i++) {
      const selectEl = document.getElementById(`editQtag6Tag${i}`);
      if (selectEl) selectEl.value = btn.dataset[`tag${i}Id`] || '';
    }

    const modal = new bootstrap.Modal(document.getElementById('editQtag6Modal'));
    modal.show();
  });

  // Edit Qtag6 form submission
  const editQtag6Form = document.getElementById('editQtag6Form');
  if (editQtag6Form) {
    editQtag6Form.addEventListener('submit', function (e) {
      e.preventDefault();
      const cardId = document.getElementById('editQtag6Id').value;
      const errorEl = document.getElementById('edit-qtag6-error-message');
      errorEl.style.display = 'none';

      // Validate all 6 tags selected
      for (let i = 1; i <= 6; i++) {
        if (!document.getElementById(`editQtag6Tag${i}`).value) {
          errorEl.textContent = 'Please select all 6 tags';
          errorEl.style.display = 'block';
          return;
        }
      }

      Swal.fire({ title: 'Updating Qtag6...', allowOutsideClick: false, didOpen: () => Swal.showLoading() });

      const formData = new FormData(editQtag6Form);
      fetch(`/subdash/${currentSubdashId}/update_qtag6/${cardId}`, {
        method: 'POST',
        body: formData
      })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          Swal.fire({ title: 'Success!', text: 'Qtag6 updated', icon: 'success', timer: 2000 }).then(() => location.reload());
        } else {
          Swal.fire({ title: 'Error', text: data.error || 'Failed to update', icon: 'error' });
        }
      })
      .catch(err => Swal.fire({ title: 'Error', text: err.message, icon: 'error' }));
    });
  }

  // ============================================================
  // ★ EDIT TAGS: Qtag Single3 - Open modal and populate
  // ============================================================
  document.addEventListener('click', function (e) {
    const btn = e.target.closest('.edit-qtag-single3-btn');
    if (!btn) return;
    e.preventDefault();

    document.getElementById('editSingle3Id').value = btn.dataset.cardId;
    document.getElementById('editSingle3PvTag').value = btn.dataset.pvTagId || '';

    // SV HIGH
    const svHighType = btn.dataset.svHighType || 'fixed';
    document.getElementById('editSingle3SvHighType').value = svHighType;
    if (svHighType === 'tag') {
      document.querySelector('.edit-sv-high-fixed-group').style.display = 'none';
      document.querySelector('.edit-sv-high-tag-group').style.display = '';
      document.getElementById('editSingle3SvHighTag').value = btn.dataset.svHighTagId || '';
    } else {
      document.querySelector('.edit-sv-high-fixed-group').style.display = '';
      document.querySelector('.edit-sv-high-tag-group').style.display = 'none';
      document.getElementById('editSingle3SvHighFixed').value = btn.dataset.svHighFixed || '';
    }

    // SV LOW
    const svLowType = btn.dataset.svLowType || 'fixed';
    document.getElementById('editSingle3SvLowType').value = svLowType;
    if (svLowType === 'tag') {
      document.querySelector('.edit-sv-low-fixed-group').style.display = 'none';
      document.querySelector('.edit-sv-low-tag-group').style.display = '';
      document.getElementById('editSingle3SvLowTag').value = btn.dataset.svLowTagId || '';
    } else {
      document.querySelector('.edit-sv-low-fixed-group').style.display = '';
      document.querySelector('.edit-sv-low-tag-group').style.display = 'none';
      document.getElementById('editSingle3SvLowFixed').value = btn.dataset.svLowFixed || '';
    }

    const modal = new bootstrap.Modal(document.getElementById('editQtagSingle3Modal'));
    modal.show();
  });

  // Edit Single3 form submission
  const editSingle3Form = document.getElementById('editQtagSingle3Form');
  if (editSingle3Form) {
    editSingle3Form.addEventListener('submit', function (e) {
      e.preventDefault();
      const cardId = document.getElementById('editSingle3Id').value;
      const errorEl = document.getElementById('edit-single3-error-message');
      errorEl.style.display = 'none';

      if (!document.getElementById('editSingle3PvTag').value) {
        errorEl.textContent = 'Please select a PV tag';
        errorEl.style.display = 'block';
        return;
      }

      Swal.fire({ title: 'Updating Single3...', allowOutsideClick: false, didOpen: () => Swal.showLoading() });

      const formData = new FormData(editSingle3Form);
      fetch(`/subdash/${currentSubdashId}/update_qtag_single3/${cardId}`, {
        method: 'POST',
        body: formData
      })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          Swal.fire({ title: 'Success!', text: 'Single3 updated', icon: 'success', timer: 2000 }).then(() => location.reload());
        } else {
          Swal.fire({ title: 'Error', text: data.error || 'Failed to update', icon: 'error' });
        }
      })
      .catch(err => Swal.fire({ title: 'Error', text: err.message, icon: 'error' }));
    });
  }

  // ============================================================
  // ★ EDIT TAGS: Qtag PV Only - Open modal and populate
  // ============================================================
  document.addEventListener('click', function (e) {
    const btn = e.target.closest('.edit-qtag-pv-btn');
    if (!btn) return;
    e.preventDefault();

    document.getElementById('editPvId').value = btn.dataset.cardId;
    document.getElementById('editPvTag').value = btn.dataset.pvTagId || '';

    const modal = new bootstrap.Modal(document.getElementById('editQtagPvModal'));
    modal.show();
  });

  // Edit PV Only form submission
  const editPvForm = document.getElementById('editQtagPvForm');
  if (editPvForm) {
    editPvForm.addEventListener('submit', function (e) {
      e.preventDefault();
      const cardId = document.getElementById('editPvId').value;
      const errorEl = document.getElementById('edit-pv-error-message');
      errorEl.style.display = 'none';

      if (!document.getElementById('editPvTag').value) {
        errorEl.textContent = 'Please select a PV tag';
        errorEl.style.display = 'block';
        return;
      }

      Swal.fire({ title: 'Updating PV Card...', allowOutsideClick: false, didOpen: () => Swal.showLoading() });

      const formData = new FormData(editPvForm);
      fetch(`/subdash/${currentSubdashId}/update_qtag_pv/${cardId}`, {
        method: 'POST',
        body: formData
      })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          Swal.fire({ title: 'Success!', text: 'PV card updated', icon: 'success', timer: 2000 }).then(() => location.reload());
        } else {
          Swal.fire({ title: 'Error', text: data.error || 'Failed to update', icon: 'error' });
        }
      })
      .catch(err => Swal.fire({ title: 'Error', text: err.message, icon: 'error' }));
    });
  }

  // ============================================================
  // ★ CHANGE CARD COLOR (all card types: quad, quad6, single3, pvonly)
  // ============================================================
  // Helper: determine if a color is dark → return white text, else black
  function getContrastColor(hexColor) {
    if (!hexColor) return '';
    const hex = hexColor.replace('#', '');
    const r = parseInt(hex.substring(0, 2), 16);
    const g = parseInt(hex.substring(2, 4), 16);
    const b = parseInt(hex.substring(4, 6), 16);
    // YIQ formula for perceived brightness
    const yiq = (r * 299 + g * 587 + b * 114) / 1000;
    return yiq >= 140 ? '#1a1a1a' : '#ffffff';
  }

  // Apply color to a card element
  function applyCardColor(cardEl, color) {
    if (color) {
      cardEl.style.setProperty('--qtag-card-bg', color);
      cardEl.style.setProperty('--qtag-card-text', getContrastColor(color));
      cardEl.classList.add('qtag-custom-bg');
    } else {
      cardEl.style.removeProperty('--qtag-card-bg');
      cardEl.style.removeProperty('--qtag-card-text');
      cardEl.classList.remove('qtag-custom-bg');
    }
    cardEl.dataset.cardColor = color || '';
    // Update the dropdown button's data-current-color
    const colorBtn = cardEl.querySelector('.change-card-color-btn');
    if (colorBtn) colorBtn.dataset.currentColor = color || '';
  }

  document.addEventListener('click', function (e) {
    const btn = e.target.closest('.change-card-color-btn');
    if (!btn) return;
    e.preventDefault();

    const cardType = btn.dataset.cardType;
    const cardId = btn.dataset.cardId;
    const currentColor = btn.dataset.currentColor || '';

    // Color palette - organized by hue with light to dark shades
    const colorPalette = [
      // Row labels and colors: [label, ...shades from light to dark]
      ['Red',     '#ffcdd2','#ef9a9a','#e57373','#ef5350','#f44336','#e53935','#c62828','#b71c1c'],
      ['Pink',    '#f8bbd0','#f48fb1','#f06292','#ec407a','#e91e63','#d81b60','#ad1457','#880e4f'],
      ['Purple',  '#e1bee7','#ce93d8','#ba68c8','#ab47bc','#9c27b0','#8e24aa','#6a1b9a','#4a148c'],
      ['Indigo',  '#c5cae9','#9fa8da','#7986cb','#5c6bc0','#3f51b5','#3949ab','#283593','#1a237e'],
      ['Blue',    '#bbdefb','#90caf9','#64b5f6','#42a5f5','#2196f3','#1e88e5','#1565c0','#0d47a1'],
      ['Cyan',    '#b2ebf2','#80deea','#4dd0e1','#26c6da','#00bcd4','#00acc1','#00838f','#006064'],
      ['Teal',    '#b2dfdb','#80cbc4','#4db6ac','#26a69a','#009688','#00897b','#00695c','#004d40'],
      ['Green',   '#c8e6c9','#a5d6a7','#81c784','#66bb6a','#4caf50','#43a047','#2e7d32','#1b5e20'],
      ['Yellow',  '#fff9c4','#fff59d','#fff176','#ffee58','#ffeb3b','#fdd835','#f9a825','#f57f17'],
      ['Orange',  '#ffe0b2','#ffcc80','#ffb74d','#ffa726','#ff9800','#fb8c00','#e65100','#bf360c'],
      ['Brown',   '#d7ccc8','#bcaaa4','#a1887f','#8d6e63','#795548','#6d4c41','#4e342e','#3e2723'],
      ['Gray',    '#f5f5f5','#e0e0e0','#bdbdbd','#9e9e9e','#757575','#616161','#424242','#212121'],
    ];

    // Build palette grid HTML
    let paletteHtml = '<div style="display:grid;grid-template-columns:50px repeat(8,1fr);gap:3px;align-items:center;">';
    colorPalette.forEach(row => {
      const label = row[0];
      paletteHtml += `<span style="font-size:11px;color:#aaa;text-align:right;padding-right:4px;">${label}</span>`;
      for (let i = 1; i < row.length; i++) {
        const c = row[i];
        const isActive = c.toLowerCase() === currentColor.toLowerCase();
        paletteHtml += `<div class="swal-color-cell" data-color="${c}" title="${c}"
          style="width:100%;aspect-ratio:1;border-radius:4px;background:${c};cursor:pointer;
          border:2px solid ${isActive ? '#fff' : 'transparent'};
          box-shadow:${isActive ? '0 0 0 2px #6366f1' : 'none'};
          transition:transform .1s,border-color .15s;"></div>`;
      }
    });
    paletteHtml += '</div>';

    Swal.fire({
      title: 'Change Card Color',
      width: 480,
      html: `
        <div class="text-start">
          <div class="mb-2 d-flex align-items-center gap-2">
            <button type="button" id="swal-reset-default" class="btn btn-sm btn-outline-secondary">
              <i class="bi bi-arrow-counterclockwise me-1"></i>Reset Default
            </button>
            <div class="d-flex align-items-center gap-2 ms-auto">
              <label style="font-size:12px;color:#aaa;">Custom:</label>
              <input type="color" id="swal-color-picker" value="${currentColor || '#2196f3'}"
                style="width:36px;height:30px;border:1px solid #555;border-radius:4px;cursor:pointer;padding:1px;">
            </div>
          </div>
          <div class="mb-3" id="swal-palette">${paletteHtml}</div>
          <div class="p-3 rounded" id="swal-preview"
            style="background:${currentColor || '#2a2a2a'};color:${currentColor ? getContrastColor(currentColor) : '#fff'};text-align:center;font-weight:500;">
            Preview Text
          </div>
          <input type="hidden" id="swal-selected-color" value="${currentColor}">
        </div>
      `,
      showCancelButton: true,
      confirmButtonText: 'Save',
      cancelButtonText: 'Cancel',
      didOpen: () => {
        const popup = Swal.getPopup();
        const preview = popup.querySelector('#swal-preview');
        const hiddenInput = popup.querySelector('#swal-selected-color');
        const pickerInput = popup.querySelector('#swal-color-picker');
        const cells = popup.querySelectorAll('.swal-color-cell');
        const resetBtn = popup.querySelector('#swal-reset-default');

        function selectColor(color) {
          hiddenInput.value = color;
          preview.style.background = color || '#2a2a2a';
          preview.style.color = color ? getContrastColor(color) : '#fff';
          // Update cell borders
          cells.forEach(cell => {
            const match = cell.dataset.color.toLowerCase() === (color || '').toLowerCase();
            cell.style.border = match ? '2px solid #fff' : '2px solid transparent';
            cell.style.boxShadow = match ? '0 0 0 2px #6366f1' : 'none';
          });
          if (color) pickerInput.value = color;
        }

        // Palette cell click
        cells.forEach(cell => {
          cell.addEventListener('click', () => selectColor(cell.dataset.color));
          cell.addEventListener('mouseenter', () => { cell.style.transform = 'scale(1.25)'; cell.style.zIndex = '2'; });
          cell.addEventListener('mouseleave', () => { cell.style.transform = 'scale(1)'; cell.style.zIndex = ''; });
        });

        // Custom color picker - live update
        pickerInput.addEventListener('input', () => selectColor(pickerInput.value));

        // Reset default
        resetBtn.addEventListener('click', () => {
          selectColor('');
          pickerInput.value = '#2196f3';
        });
      },
      preConfirm: () => {
        return Swal.getPopup().querySelector('#swal-selected-color').value;
      }
    }).then(result => {
      if (!result.isConfirmed) return;
      const selectedColor = result.value;

      // Save to backend
      fetch(`/subdash/${currentSubdashId}/update_card_color`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ card_type: cardType, card_id: cardId, color: selectedColor })
      })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          // Find the card element and apply color immediately (no reload)
          const cardEl = btn.closest('.quad-tag-card, .qtag6-card, .qtag-single-sub-card');
          if (cardEl) {
            applyCardColor(cardEl, selectedColor);
          }
          Swal.fire({ icon: 'success', title: 'Color saved', timer: 1200, showConfirmButton: false });
        } else {
          Swal.fire({ icon: 'error', title: 'Error', text: data.message || 'Failed to save color' });
        }
      })
      .catch(err => Swal.fire({ icon: 'error', title: 'Error', text: err.message }));
    });
  });

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

    // Initialize --qtag-card-text for cards that already have custom color from server
    document.querySelectorAll('.qtag-custom-bg').forEach(cardEl => {
      const bg = getComputedStyle(cardEl).getPropertyValue('--qtag-card-bg').trim();
      if (bg) {
        cardEl.style.setProperty('--qtag-card-text', getContrastColor(bg));
      }
    });
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

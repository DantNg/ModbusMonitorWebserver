/* ==========================================================================
   Subdashboard Detail - Main JavaScript
   Extracted from subdashboards/detail.html inline <script>
   ========================================================================== */

// Read configuration passed from Jinja2 template
const currentSubdashId = window.SUBDASH_CONFIG.subdashId;
const currentGroup = window.SUBDASH_CONFIG.currentGroup;
let TAG_SCALE_MAP = {};
try {
  const scaleDataEl = document.getElementById('tag-scale-data');
  const scaleRaw = scaleDataEl ? scaleDataEl.getAttribute('data-scale-map') : null;
  TAG_SCALE_MAP = scaleRaw ? (JSON.parse(scaleRaw) || {}) : {};
} catch (e) {
  TAG_SCALE_MAP = {};
}

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
            // Force quad values for this tag to 0 at load if offline.
            // Exclude fixed-SV rows to prevent overwriting static values.
            document.querySelectorAll(`[id^="quad-tag-val-"][id$="-${tagId}"]:not([id^="quad-tag-val-fixed-"])`).forEach(el => {
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

      // Reset form and newGroupSection state when modal closes to prevent stale
      // new_group_name from being sent on the next open (which would create a new group
      // instead of using the selected existing group).
      const addTagModalEl = document.getElementById('addTagModal');
      if (addTagModalEl) {
        addTagModalEl.addEventListener('hidden.bs.modal', function () {
          addTagForm.reset();
          const newGroupSection = document.getElementById('newGroupSection');
          const targetGroupSelect = document.getElementById('target_group');
          const toggleBtn = document.getElementById('toggleNewGroup');
          const newGroupInput = document.getElementById('new_group_name');
          if (newGroupSection) newGroupSection.style.display = 'none';
          if (targetGroupSelect) targetGroupSelect.disabled = false;
          if (newGroupInput) { newGroupInput.required = false; newGroupInput.value = ''; }
          if (toggleBtn) {
            toggleBtn.innerHTML = '<i class="bi bi-plus-circle"></i> New';
            toggleBtn.className = 'btn btn-outline-secondary';
          }
        });
      }
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
        const groupId = document.getElementById('quad_target_group').value;
        const newGroupName = document.getElementById('new_quad_group_name').value.trim();
        
        // Validate PV tags
        if (!tag1Id || !tag2Id) {
          console.log('Validation failed: PV tags not selected');
          showQuadError('Please select both PV tags');
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
        
        // Use FormData from form (includes SV type/fixed and all other fields)
        const formData = new FormData(addQuadTagForm);
        // Map the title field names to what the route expects
        const cardTitle = document.getElementById('quad_card_title').value.trim();
        const leftTitle = document.getElementById('quad_left_title').value.trim();
        const rightTitle = document.getElementById('quad_right_title').value.trim();
        formData.set('card_title', cardTitle);
        formData.set('left_title', leftTitle);
        formData.set('right_title', rightTitle);
        
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
              const targetGroupId = data.group_id || groupId;
              if (targetGroupId) {
                const url = new URL(window.location.href);
                url.searchParams.set('group', String(targetGroupId));
                window.location.href = url.toString();
              } else {
                location.reload();
              }
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
        
        // Set quad ID
        document.getElementById('editQuadId').value = quadId;
        
        // Set PV tag selections
        document.getElementById('edit_tag1_select').value = btn.dataset.tag1Id;
        document.getElementById('edit_tag2_select').value = btn.dataset.tag2Id;
        
        // SV Left fix value
        const svLeftType = btn.dataset.svLeftType || 'tag';
        document.getElementById('edit_sv_left_type').value = svLeftType;
        const leftFixedGrp = document.querySelector('.edit-quad-sv-left-fixed-group');
        const leftTagGrp = document.querySelector('.edit-quad-sv-left-tag-group');
        if (leftFixedGrp && leftTagGrp) {
          leftFixedGrp.style.display = svLeftType === 'fixed' ? '' : 'none';
          leftTagGrp.style.display = svLeftType === 'tag' ? '' : 'none';
        }
        if (svLeftType === 'fixed') {
          document.getElementById('edit_sv_left_fixed').value = btn.dataset.svLeftFixed || '';
          document.getElementById('edit_sv_left_fixed_unit').value = btn.dataset.svLeftFixedUnit || '';
        } else {
          document.getElementById('edit_tag3_select').value = btn.dataset.tag3Id || '';
        }

        // SV Right fix value
        const svRightType = btn.dataset.svRightType || 'tag';
        document.getElementById('edit_sv_right_type').value = svRightType;
        const rightFixedGrp = document.querySelector('.edit-quad-sv-right-fixed-group');
        const rightTagGrp = document.querySelector('.edit-quad-sv-right-tag-group');
        if (rightFixedGrp && rightTagGrp) {
          rightFixedGrp.style.display = svRightType === 'fixed' ? '' : 'none';
          rightTagGrp.style.display = svRightType === 'tag' ? '' : 'none';
        }
        if (svRightType === 'fixed') {
          document.getElementById('edit_sv_right_fixed').value = btn.dataset.svRightFixed || '';
          document.getElementById('edit_sv_right_fixed_unit').value = btn.dataset.svRightFixedUnit || '';
        } else {
          document.getElementById('edit_tag4_select').value = btn.dataset.tag4Id || '';
        }
        
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
        
        // Validate PV tags
        if (!tag1Id || !tag2Id) {
          showEditQuadError('Please select both PV tags');
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
        
        // Use FormData from the form (includes SV type/fixed fields)
        const formData = new FormData(editQuadTagsForm);
        
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

    // Open card alarm conditions modal (all card types: Qtag6, Qtag4, Qtag3, Qtag2, Single3, PV Only, PV Dual)
    document.addEventListener('click', async function (e) {
      const btn = e.target.closest('.set-card-condition-btn');
      if (!btn) return;

      e.preventDefault();
      const cardType = btn.dataset.cardType || '';
      const cardId = btn.dataset.cardId || '';

      document.getElementById('cardCondCardType').value = cardType;
      document.getElementById('cardCondCardId').value = cardId;

      // Toggle right section visibility based on card type
      const rightSection = document.getElementById('cardCondRightSection');
      const isDual = DUAL_COLUMN_TYPES.includes(cardType);
      if (rightSection) {
        rightSection.style.display = isDual ? 'block' : 'none';
      }

      // Update column titles based on card type
      const leftTitle = document.getElementById('cardCondLeftTitle');
      const rightTitle = document.getElementById('cardCondRightTitle');
      if (cardType === 'qtag6') {
        if (leftTitle) leftTitle.textContent = 'Left Column (PV1)';
        if (rightTitle) rightTitle.textContent = 'Right Column (PV2)';
      } else if (cardType === 'pv_dual') {
        if (leftTitle) leftTitle.textContent = 'Left PV';
        if (rightTitle) rightTitle.textContent = 'Right PV';
      } else if (cardType === 'qtag4') {
        if (leftTitle) leftTitle.textContent = 'Left Column (PV1)';
        if (rightTitle) rightTitle.textContent = 'Right Column (PV2)';
      } else if (cardType === 'single3') {
        if (leftTitle) leftTitle.textContent = 'PV Value';
      } else if (cardType === 'pv_only') {
        if (leftTitle) leftTitle.textContent = 'PV Value';
      } else if (cardType === 'qtag2') {
        if (leftTitle) leftTitle.textContent = 'PV Value';
      } else if (cardType === 'qtag3') {
        if (leftTitle) leftTitle.textContent = 'PV Value';
      }

      // Load available tags trước (await), sau đó load conditions để selected value hoạt động đúng
      await loadCardAvailableTags();
      loadCardConditionsFromAPI(cardType, cardId);

      const modal = new bootstrap.Modal(document.getElementById('cardConditionsModal'));
      modal.show();
    });

    // Card condition compare type toggle
    document.addEventListener('change', function (e) {
      if (e.target.classList.contains('card-compare-type-selector')) {
        const target = e.target.dataset.target;
        if (target) {
          cardCondToggleCompareType(target, e.target.value);
        }
      }
    });

    // Card condition save button
    document.addEventListener('click', function (e) {
      if (e.target.id === 'saveCardConditionsBtn' || e.target.closest('#saveCardConditionsBtn')) {
        e.preventDefault();
        saveCardConditionsToAPI();
      }
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

      // Exclude PV tags of the current quad card to avoid self-compare selection.
      const quadId = document.getElementById('conditionQuadId')?.value || '';
      const excludedPvTagIds = new Set();
      if (quadId) {
        const quadCard = document.querySelector(`.quad-tag-card[data-quad-id="${quadId}"]`);
        if (quadCard) {
          quadCard.querySelectorAll('.quad-tag-item.pv-item[data-tag-id], .quad-tag-item[data-role="pv"][data-tag-id]').forEach(el => {
            const id = el.getAttribute('data-tag-id');
            if (id) excludedPvTagIds.add(String(id));
          });

          // Fallback for legacy markup: first tag-item in each sub-card is PV.
          if (excludedPvTagIds.size === 0) {
            quadCard.querySelectorAll('.quad-sub-card').forEach(sub => {
              const firstItem = sub.querySelector('.quad-tag-item[data-tag-id]');
              const id = firstItem?.getAttribute('data-tag-id');
              if (id) excludedPvTagIds.add(String(id));
            });
          }
        }
      }
      
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
          if (excludedPvTagIds.has(String(tag.id))) return;
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

  function updateLastUpdatedForElement(element, nowForTag) {
    if (!element) return;

    const card = element.closest(
      '.qtag6-sub-card, .qtag-single-sub-card, .qtag-pv-dual-sub-card, .quad-sub-card'
    );
    if (!card) return;

    const timeEl = card.querySelector(
      '.qtag6-update-time, .qtag-single-update-time, .qtag-pv-dual-update-time, .quad-update-time'
    );
    if (timeEl) {
      timeEl.textContent = `Last updated: ${formatTime24h(nowForTag)}`;
    }
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
      return num % 1 === 0 ? String(Math.round(num)) : num.toFixed(2);
    }
  }

  function scaleToDecimalPlaces(scaleValue) {
    const raw = Number(scaleValue);
    if (!Number.isFinite(raw)) return null;
    const scale = Math.abs(raw);
    if (scale === 0) return 0;
    const scaleText = scale.toFixed(12).replace(/0+$/, '').replace(/\.$/, '');
    if (!scaleText.includes('.')) return 0;
    return Math.min(6, scaleText.split('.')[1].length);
  }

  function resolveTagScale(tagOrTagId) {
    const normalizeScale = (entry) => {
      if (entry && typeof entry === 'object' && entry.scale !== undefined) {
        const objScale = Number(entry.scale);
        if (Number.isFinite(objScale)) return objScale;
      }
      const raw = Number(entry);
      return Number.isFinite(raw) ? raw : null;
    };

    if (tagOrTagId && typeof tagOrTagId === 'object') {
      const directScale = Number(tagOrTagId.scale);
      if (Number.isFinite(directScale)) return directScale;
      const mapScale = normalizeScale(TAG_SCALE_MAP[String(tagOrTagId.id)]);
      if (mapScale !== null) return mapScale;
      return 1;
    }
    const mapScale = normalizeScale(TAG_SCALE_MAP[String(tagOrTagId)]);
    return mapScale !== null ? mapScale : 1;
  }

  function resolveTagOffset(tagOrTagId) {
    const resolveFromEntry = (entry) => {
      if (entry && typeof entry === 'object' && entry.offset !== undefined) {
        const objOffset = Number(entry.offset);
        if (Number.isFinite(objOffset)) return objOffset;
      }
      return 0;
    };

    if (tagOrTagId && typeof tagOrTagId === 'object') {
      const directOffset = Number(tagOrTagId.offset);
      if (Number.isFinite(directOffset)) return directOffset;
      return resolveFromEntry(TAG_SCALE_MAP[String(tagOrTagId.id)]);
    }
    return resolveFromEntry(TAG_SCALE_MAP[String(tagOrTagId)]);
  }

  function transformTagValue(rawValue, tagOrTagId) {
    // Giá trị từ worker đã là engineering units (scale+offset đã áp tại nguồn).
    // Chỉ đảm bảo trả về số, không nhân scale+offset nữa để tránh double-apply.
    const num = Number(rawValue);
    if (!Number.isFinite(num)) return rawValue;
    return num;
  }

  function formatZeroByTagId(tagId) {
    return fmtVal(0, resolveTagScale(tagId));
  }

  function decimalsByDisplayRule(scaleValue) {
    const scale = Math.abs(Number(scaleValue));
    if (!Number.isFinite(scale)) return null;

    // Explicit display policy requested by user.
    if (Math.abs(scale - 1.0) < 1e-9) return null;
    if (Math.abs(scale - 0.1) < 1e-9) return 1;
    if (scale >= 0.2) return 2;

    // Fallback for other uncommon scales.
    return scaleToDecimalPlaces(scale);
  }


  /**
   * Format a raw tag value for display:
   * - integer (or whole float) -> no decimal     e.g. 123.0   -> "123"
   * - decimal -> up to 2 decimal places, no trailing zeros
   *              e.g. 123.5   -> "123.5"
   *              e.g. 123.50  -> "123.5"
   *              e.g. 123.456 -> "123.46"
   */
  function fmtVal(value, scale = 1) {
    if (value === null || value === undefined || value === '') return '0';
    const num = parseFloat(value);
    if (isNaN(num)) return String(value);

    const scaleNum = Math.abs(Number(scale));
    if (Number.isFinite(scaleNum) && Math.abs(scaleNum - 1.0) < 1e-9) {
      return Number.isInteger(num) ? String(Math.trunc(num)) : parseFloat(num.toFixed(2)).toString();
    }

    const decimalPlaces = decimalsByDisplayRule(scale);
    if (decimalPlaces === null) {
      // Fallback: max 2 decimals if scale is invalid/missing
      return parseFloat(num.toFixed(2)).toString();
    }
    return num.toFixed(decimalPlaces);
  }

  function extractTagIdFromValueElement(el) {
    if (!el) return null;
    const dataTagId = Number(el.getAttribute('data-tag-id'));
    if (Number.isFinite(dataTagId)) return dataTagId;

    const parentWithTag = el.closest('[data-tag-id]');
    if (parentWithTag) {
      const parentTagId = Number(parentWithTag.getAttribute('data-tag-id'));
      if (Number.isFinite(parentTagId)) return parentTagId;
    }

    const id = el.id || '';

    // Only fallback to parsing ID for value elements whose id format
    // is known to end with a real tag id (not card id).
    const tagIdTailPatterns = [
      /^quad-tag-val-\d+-\d+$/,
      /^qtag6-val-\d+-\d+$/,
      /^qtag4-val-\d+-\d+$/,
      /^qtag3-val-\d+-\d+$/,
      /^qtag2-val-\d+-\d+$/,
      /^qtag-single3-pv-\d+-\d+$/,
      /^qtag-pv-val-\d+-\d+$/,
      /^qtag-pv-dual-val-\d+-\d+$/
    ];

    if (!tagIdTailPatterns.some((pattern) => pattern.test(id))) {
      return null;
    }

    const match = id.match(/-(\d+)$/);
    if (!match) return null;
    const parsed = Number(match[1]);
    return Number.isFinite(parsed) ? parsed : null;
  }

  function formatValueByTagId(rawValue, tagId) {
    const num = Number(rawValue);
    if (!Number.isFinite(num)) return rawValue;
    return fmtVal(num, resolveTagScale(tagId));
  }

  function normalizeFixedDisplayText(rawValue) {
    // Giá trị fixed đã được template Jinja2 format đúng qua filter format_fixed_value.
    // Trả về nguyên văn để không làm mất trailing zeros người dùng nhập (vd: "12.00" → "12.00").
    return String(rawValue ?? '').trim();
  }

  function normalizeAllCardValuesByScale() {
    // No-op: giá trị đã được format sẵn trên BE qua shared/formatting.
    // FE không được tự format lại để tránh mất trailing zeros và sai quy tắc hiển thị.
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
  const QUAD_CACHE_PREFIX = `quad_layout_v3_${currentSubdashId}_`;

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
  const QUAD_VALUES_KEY = `quad_values_v2_${currentSubdashId}`;
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
          // Cache chứa formatted string từ BE — chỉ set trực tiếp, không format lại
          Object.keys(_quadValuesCache).forEach(tagId => {
            const el = grid.querySelector(`[id$="-${tagId}"]`);
            if (el) el.textContent = String(_quadValuesCache[tagId] ?? '0');
          });

          // Ensure fixed SV values from legacy cache are not shown with forced trailing zeros.
          grid.querySelectorAll('[id^="quad-tag-val-fixed-"]').forEach((el) => {
            el.textContent = normalizeFixedDisplayText(el.textContent);
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
        const originalBodyHTML = body.innerHTML;
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
        
        try {
          // Extract data from existing items - MUST BE IN ORDER
          const itemsData = [];
          for (let i = 0; i < 4; i++) {
            const item = items[i];
            if (!item) {
              console.warn(`Quad tag ${i} not found!`);
              continue;
            }

            // Get real tag ID only from data-tag-id.
            // If missing (fixed SV), keep it as null to avoid fake tag mapping.
            const rawTagId = item.getAttribute('data-tag-id');
            const hasRealTagId = rawTagId !== null && rawTagId !== '';
            const tagId = hasRealTagId ? String(rawTagId) : null;

            // Preserve device status so quad-only dashboards know current state
            const deviceStatus = item.getAttribute('data-device-status') || 'unknown';

            // Read value/unit directly from this item to support both tag-SV and fixed-SV markup.
            const valElement = item.querySelector('[id^="quad-tag-val-"]') || item.querySelector('.quad-tag-value span') || item.querySelector('.quad-tag-value .fw-bold') || item.querySelector('.fw-bold');
            const unitElement = item.querySelector('.quad-tag-unit, small.text-muted, small');
            const nameElement = item.querySelector('.quad-tag-name');

            // Extract current value and unit — ưu tiên value cache nếu có
            let value = valElement ? valElement.textContent.trim() : '0';
            if (hasRealTagId && _quadValuesCache[tagId] !== undefined) {
              // Cache chứa formatted string từ BE — hiển thị trực tiếp
              value = String(_quadValuesCache[tagId]);
            } else if (!hasRealTagId) {
              value = normalizeFixedDisplayText(value);
            }
            const unit = unitElement ? unitElement.textContent.trim() : '';
            const name = nameElement ? nameElement.textContent.trim() : `Tag ${i + 1}`;

            // ✅ Use unique ID; fixed SV rows must not end with numeric tag-id pattern.
            const id = hasRealTagId
              ? `quad-tag-val-${cardId}-${tagId}`
              : `quad-tag-val-fixed-${cardId}-${i}`;

            console.log(`Quad tag ${i}: ID=${id}, CardID=${cardId}, TagID=${tagId}, Value=${value}, Unit=${unit}, Status=${deviceStatus}`);

            itemsData.push({
              value: value, // Keep original value without K/M formatting
              unit: unit,
              id: id,
              tagId: tagId,
              hasRealTagId: hasRealTagId,
              name: name,
              deviceStatus: deviceStatus
            });
          }

          if (itemsData.length < 4) {
            console.warn(`Skipping quad rebuild for card ${cardId}: insufficient item data (${itemsData.length}/4)`);
            return;
          }

          // Clear existing content only after data extraction succeeded.
          grid.innerHTML = '';
        
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
            <div class="quad-tag-item pv-item" ${itemsData[0].hasRealTagId ? `data-tag-id="${itemsData[0].tagId}"` : ''} data-device-status="${itemsData[0].deviceStatus}">
              <div class="quad-tag-label">PV</div>
              <div class="quad-tag-value-row">
                <div class="quad-tag-value" id="${itemsData[0].id}">${itemsData[0].value}</div>
                <div class="quad-tag-unit">${itemsData[0].unit}</div>
              </div>
            </div>
            <div class="quad-tag-item sv-item" ${itemsData[2].hasRealTagId ? `data-tag-id="${itemsData[2].tagId}"` : ''} data-device-status="${itemsData[2].deviceStatus}">
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
            <div class="quad-tag-item pv-item" ${itemsData[1].hasRealTagId ? `data-tag-id="${itemsData[1].tagId}"` : ''} data-device-status="${itemsData[1].deviceStatus}">
              <div class="quad-tag-label">PV</div>
              <div class="quad-tag-value-row">
                <div class="quad-tag-value" id="${itemsData[1].id}">${itemsData[1].value}</div>
                <div class="quad-tag-unit">${itemsData[1].unit}</div>
              </div>
            </div>
            <div class="quad-tag-item sv-item" ${itemsData[3].hasRealTagId ? `data-tag-id="${itemsData[3].tagId}"` : ''} data-device-status="${itemsData[3].deviceStatus}">
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

          // Normalize header: keep only one top-level header to avoid duplicate gear/actions.
          const allCardHeaders = card.querySelectorAll('.card-header');
          let cardHeader = allCardHeaders[0] || null;
          if (allCardHeaders.length > 1) {
            allCardHeaders.forEach((hdr, idx) => {
              if (idx > 0) hdr.remove();
            });
          }

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

          // Keep only one gear dropdown/actions container in header.
          const dropdowns = cardHeader.querySelectorAll('.dropdown');
          if (dropdowns.length > 1) {
            dropdowns.forEach((dd, idx) => {
              if (idx > 0) dd.remove();
            });
          }

          console.log(`✅ Successfully restructured quad card ${cardIndex + 1}`);
        } catch (err) {
          console.error(`❌ Quad rebuild failed for card ${cardId}:`, err);
          // Fallback to server-rendered HTML instead of leaving blank body.
          const cardBody = card.querySelector('.card-body');
          if (cardBody) cardBody.innerHTML = originalBodyHTML;
        }
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

    // Re-apply display rule for all Qtag/Quad values in case old localStorage cache
    // still contains values formatted by previous rules.
    normalizeAllCardValuesByScale();
    setTimeout(normalizeAllCardValuesByScale, 250);
    setTimeout(normalizeAllCardValuesByScale, 1500);

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
   * Check if a card element has monitoring enabled.
   * Looks for the card-alarm-toggle inside the card (or its root ancestor).
   * Returns true if toggle doesn't exist (no conditions set yet).
   * Returns false if conditions exist but monitoring is disabled (data-monitoring-enabled="false").
   * Returns true if conditions exist and monitoring is enabled.
   */
  function isCardMonitoringEnabled(cardEl) {
    if (!cardEl) return true;
    // Walk up to find the root card element that contains the toggle
    const rootCard = cardEl.closest(
      '[data-qtag6-id], [data-qtag4-id], [data-qtag-single3-id], ' +
      '[data-qtag-pv-id], [data-qtag2-id], [data-qtag3-id], [data-qtag-pv-dual-id]'
    ) || cardEl;
    const toggleWrap = rootCard.querySelector('.card-alarm-toggle-wrap');
    // No toggle wrap = no conditions configured yet → allow alarms
    if (!toggleWrap) return true;
    // data-monitoring-enabled is set when conditions exist
    // 'false' = conditions exist but disabled; anything else (including absent) = allow
    if (toggleWrap.dataset.monitoringEnabled === 'false') return false;
    return true;
  }

  /**
   * Check whether a card has a saved alarm condition configured (enabled OR disabled).
   * loadCardConditionToggles() chỉ set data-monitoring-enabled ('true'/'false') cho
   * các card CÓ condition trong DB; card chưa cấu hình thì attribute này absent.
   * Dùng để biết khi nào backend (card_alarm_event, có stable time) là nguồn chân thực.
   */
  function cardHasAlarmCondition(cardEl) {
    if (!cardEl) return false;
    const rootCard = cardEl.closest(
      '[data-qtag6-id], [data-qtag4-id], [data-qtag-single3-id], ' +
      '[data-qtag-pv-id], [data-qtag2-id], [data-qtag3-id], [data-qtag-pv-dual-id]'
    ) || cardEl;
    const toggleWrap = rootCard.querySelector('.card-alarm-toggle-wrap');
    if (!toggleWrap) return false;
    const me = toggleWrap.dataset.monitoringEnabled;
    return me === 'true' || me === 'false';
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
      // Skip if parent card has monitoring disabled
      const parentCard = tagEl.closest('.qtag6-card, .qtag4-card, .qtag-single-sub-card, .qtag3-card');
      if (!isCardMonitoringEnabled(parentCard)) return;

      // Card CÓ condition: toàn bộ visual do card_alarm_event điều khiển (tôn trọng on_stable).
      // Legacy alarm không được gây bất kỳ thay đổi visual nào — kể cả tag items —
      // để tránh nháy layout và trạng thái không nhất quán giữa 2 stable time khác nhau.
      if (cardHasAlarmCondition(parentCard)) return;

      // Remove opposite class first to prevent stale DOM state causing wrong card color
      tagEl.classList.remove('tag-alarm-high', 'tag-alarm-low');
      tagEl.classList.add('tag-alarm-active', `tag-alarm-${alarmClass}`);

      // Add tooltip with alarm info
      if (alarmInfo) {
        const tooltipText = `⚠️ ${alarmInfo.alarm_name || 'Alarm'}\nLevel: ${alarmInfo.level || 'High'}\nValue: ${alarmInfo.value ?? 'N/A'}\nCondition: ${alarmInfo.operator || '>'} ${alarmInfo.threshold ?? 'N/A'}`;
        tagEl.setAttribute('title', tooltipText);
      }

      const card = tagEl.closest('.qtag6-card, .qtag4-card, .qtag-single-sub-card, .qtag3-card');
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
      // Card CÓ condition: không can thiệp visual (đối xứng với applyTagAlarmVisual).
      const card = tagEl.closest('.qtag6-card, .qtag4-card, .qtag-single-sub-card, .qtag3-card');
      if (cardHasAlarmCondition(card)) return;

      tagEl.classList.remove('tag-alarm-active', 'tag-alarm-high', 'tag-alarm-low');
      tagEl.removeAttribute('title');

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
    const isQtag4 = card.classList.contains('qtag4-card'); // dual-column, uses qtag6-sub-card
    const isSingle = card.classList.contains('qtag-single-sub-card'); // single3, pv_only, qtag2
    const isQtag3 = card.classList.contains('qtag3-card'); // single-column card-level

    if (isQtag6 || isQtag4) {
      // Qtag6/Qtag4: each sub-column has independent alarm state.
      // Xác định card type và card ID để kiểm tra card alarm state (nguồn chân thực).
      const cardType = isQtag6 ? 'qtag6' : 'qtag4';
      const cardId = parseInt(card.getAttribute('data-qtag6-id') || card.getAttribute('data-qtag4-id') || '0');

      card.querySelectorAll('.qtag6-sub-card').forEach((sub, subIdx) => {
        const hasHigh = sub.querySelector('.tag-alarm-high');
        const hasLow = sub.querySelector('.tag-alarm-low');

        // Lấy column name từ data attribute hoặc index (0 = left, 1 = right)
        const columnAttr = sub.getAttribute('data-column');
        const column = columnAttr || (subIdx === 0 ? 'left' : 'right');

        sub.classList.remove('qtag6-alarm-high', 'qtag6-alarm-low');

        if (hasHigh || hasLow) {
          // Chỉ tô màu sub-card nếu card alarm state server-side cũng xác nhận
          // cột này đang alarm. Điều này tránh stale tag alarm (từ alarm_events cũ)
          // tô màu sai cột khi cột đó thực sự không còn alarm.
          const cardAlarmKey = cardId > 0 ? buildCardAlarmKey(cardType, cardId, column) : null;
          const serverConfirmed = cardAlarmKey ? _lastSyncedCardAlarms.has(cardAlarmKey) : false;

          if (serverConfirmed) {
            const confirmedType = _lastSyncedCardAlarms.get(cardAlarmKey);
            // Trust server exclusively – stale hasHigh/hasLow DOM must not override confirmed type
            if (confirmedType === 'High') {
              sub.classList.add('qtag6-alarm-high');
            } else if (confirmedType === 'Low') {
              sub.classList.add('qtag6-alarm-low');
            }
          } else {
            // Card has no condition (applyTagAlarmVisual returns early for cards with conditions),
            // so server will never have an entry for this card — always use DOM fallback.
            if (hasHigh) sub.classList.add('qtag6-alarm-high');
            else if (hasLow) sub.classList.add('qtag6-alarm-low');
          }
        }
      });
    } else if (isSingle) {
      // For Single3/PV Only/Qtag2: alarm goes on the card itself.
      card.classList.remove('qtag-single-alarm-high', 'qtag-single-alarm-low');
      // tag-alarm-high reflects rule SEVERITY (Critical/High), NOT threshold direction.
      // card_alarm_event carries alarm_type (High/Low = threshold direction) which is authoritative.
      // When server has confirmed a card alarm, use its type instead of DOM-derived tag severity.
      let singleCardType = null, singleCardId = 0;
      if (card.hasAttribute('data-qtag-single3-id')) {
        singleCardType = 'single3'; singleCardId = parseInt(card.getAttribute('data-qtag-single3-id') || '0');
      } else if (card.hasAttribute('data-qtag-pv-id')) {
        singleCardType = 'pv_only'; singleCardId = parseInt(card.getAttribute('data-qtag-pv-id') || '0');
      } else if (card.hasAttribute('data-qtag2-id')) {
        singleCardType = 'qtag2'; singleCardId = parseInt(card.getAttribute('data-qtag2-id') || '0');
      }
      const singleCardKey = (singleCardType && singleCardId > 0) ? buildCardAlarmKey(singleCardType, singleCardId, null) : null;
      if (singleCardKey && _lastSyncedCardAlarms.has(singleCardKey)) {
        const confirmedType = _lastSyncedCardAlarms.get(singleCardKey);
        if (confirmedType === 'High') card.classList.add('qtag-single-alarm-high');
        else if (confirmedType === 'Low') card.classList.add('qtag-single-alarm-low');
      } else {
        const hasHigh = card.querySelector('.tag-alarm-high');
        const hasLow = card.querySelector('.tag-alarm-low');
        if (hasHigh) card.classList.add('qtag-single-alarm-high');
        else if (hasLow) card.classList.add('qtag-single-alarm-low');
      }
    } else if (isQtag3) {
      // For Qtag3: card-level alarm.
      card.classList.remove('card-alarm-high', 'card-alarm-low');
      const qtag3Id = parseInt(card.getAttribute('data-qtag3-id') || '0');
      const qtag3Key = qtag3Id > 0 ? buildCardAlarmKey('qtag3', qtag3Id, null) : null;
      if (qtag3Key && _lastSyncedCardAlarms.has(qtag3Key)) {
        const confirmedType = _lastSyncedCardAlarms.get(qtag3Key);
        if (confirmedType === 'High') card.classList.add('card-alarm-high');
        else if (confirmedType === 'Low') card.classList.add('card-alarm-low');
      } else {
        const hasHigh = card.querySelector('.tag-alarm-high');
        const hasLow = card.querySelector('.tag-alarm-low');
        if (hasHigh) card.classList.add('card-alarm-high');
        else if (hasLow) card.classList.add('card-alarm-low');
      }
    }
  }

  /**
   * Recalculate alarm state for a card after a tag alarm is removed.
   * Checks remaining alarming tags and applies highest severity.
   */
  function recalcCardAlarmState(card) {
    const isQtag6 = card.classList.contains('qtag6-card');
    const isQtag4 = card.classList.contains('qtag4-card');
    const isSingle = card.classList.contains('qtag-single-sub-card');
    const isQtag3 = card.classList.contains('qtag3-card');

    if (isQtag6 || isQtag4) {
      // Khi alarm bị xóa: chỉ giữ lại màu cho các cột vẫn còn được xác nhận bởi server.
      const cardType = isQtag6 ? 'qtag6' : 'qtag4';
      const cardId = parseInt(card.getAttribute('data-qtag6-id') || card.getAttribute('data-qtag4-id') || '0');

      card.querySelectorAll('.qtag6-sub-card').forEach((sub, subIdx) => {
        const hasHigh = sub.querySelector('.tag-alarm-high');
        const hasLow = sub.querySelector('.tag-alarm-low');
        const columnAttr = sub.getAttribute('data-column');
        const column = columnAttr || (subIdx === 0 ? 'left' : 'right');
        sub.classList.remove('qtag6-alarm-high', 'qtag6-alarm-low');

        if (hasHigh || hasLow) {
          const cardAlarmKey = cardId > 0 ? buildCardAlarmKey(cardType, cardId, column) : null;
          const serverConfirmed = cardAlarmKey ? _lastSyncedCardAlarms.has(cardAlarmKey) : false;
          if (serverConfirmed) {
            const confirmedType = _lastSyncedCardAlarms.get(cardAlarmKey);
            // Trust server exclusively – stale hasHigh/hasLow must not override confirmed type
            if (confirmedType === 'High') sub.classList.add('qtag6-alarm-high');
            else if (confirmedType === 'Low') sub.classList.add('qtag6-alarm-low');
          } else {
            // Card has no condition — server never has an entry for it, always use DOM fallback.
            if (hasHigh) sub.classList.add('qtag6-alarm-high');
            else if (hasLow) sub.classList.add('qtag6-alarm-low');
          }
        }
      });
    } else if (isSingle) {
      card.classList.remove('qtag-single-alarm-high', 'qtag-single-alarm-low');
      let singleCardType = null, singleCardId = 0;
      if (card.hasAttribute('data-qtag-single3-id')) {
        singleCardType = 'single3'; singleCardId = parseInt(card.getAttribute('data-qtag-single3-id') || '0');
      } else if (card.hasAttribute('data-qtag-pv-id')) {
        singleCardType = 'pv_only'; singleCardId = parseInt(card.getAttribute('data-qtag-pv-id') || '0');
      } else if (card.hasAttribute('data-qtag2-id')) {
        singleCardType = 'qtag2'; singleCardId = parseInt(card.getAttribute('data-qtag2-id') || '0');
      }
      const singleCardKey = (singleCardType && singleCardId > 0) ? buildCardAlarmKey(singleCardType, singleCardId, null) : null;
      if (singleCardKey && _lastSyncedCardAlarms.has(singleCardKey)) {
        const confirmedType = _lastSyncedCardAlarms.get(singleCardKey);
        if (confirmedType === 'High') card.classList.add('qtag-single-alarm-high');
        else if (confirmedType === 'Low') card.classList.add('qtag-single-alarm-low');
      } else {
        const hasHigh = card.querySelector('.tag-alarm-high');
        const hasLow = card.querySelector('.tag-alarm-low');
        if (hasHigh) card.classList.add('qtag-single-alarm-high');
        else if (hasLow) card.classList.add('qtag-single-alarm-low');
      }
    } else if (isQtag3) {
      card.classList.remove('card-alarm-high', 'card-alarm-low');
      const qtag3Id = parseInt(card.getAttribute('data-qtag3-id') || '0');
      const qtag3Key = qtag3Id > 0 ? buildCardAlarmKey('qtag3', qtag3Id, null) : null;
      if (qtag3Key && _lastSyncedCardAlarms.has(qtag3Key)) {
        const confirmedType = _lastSyncedCardAlarms.get(qtag3Key);
        if (confirmedType === 'High') card.classList.add('card-alarm-high');
        else if (confirmedType === 'Low') card.classList.add('card-alarm-low');
      } else {
        const hasHigh = card.querySelector('.tag-alarm-high');
        const hasLow = card.querySelector('.tag-alarm-low');
        if (hasHigh) card.classList.add('card-alarm-high');
        else if (hasLow) card.classList.add('card-alarm-low');
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

        // Delta sync: remove alarms that are no longer active
        _lastSyncedTagAlarms.forEach((_snapshot, tagId) => {
          if (!serverAlarmMap.has(tagId)) {
            removeTagAlarmVisual(tagId);
          }
        });

        // Delta sync: apply only new/changed alarms
        const nextSnapshot = new Map();
        data.alarms.forEach(alarm => {
          const snapshot = `${alarm.level || ''}|${alarm.value ?? ''}|${alarm.operator || ''}|${alarm.threshold ?? ''}`;
          nextSnapshot.set(alarm.tag_id, snapshot);
          if (_lastSyncedTagAlarms.get(alarm.tag_id) === snapshot) {
            return;
          }
          const alarmClass = getAlarmClassForLevel(alarm.level);
          applyTagAlarmVisual(alarm.tag_id, alarmClass, alarm);
        });

        _lastSyncedTagAlarms = nextSnapshot;

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

      // Skip if monitoring is disabled for this card
      if (!isCardMonitoringEnabled(card)) return;

      // Nếu card đã có alarm condition cấu hình → backend card-alarm state machine
      // điều khiển màu theo đúng on_stable/off_stable (qua card_alarm_event).
      // KHÔNG chạy so sánh PV-vs-SV tức thời ở đây, nếu không card sẽ đỏ ngay khi
      // PV vượt SV, bỏ qua stable time (vd set on=30s nhưng 3s đã đỏ).
      // Fallback chỉ dành cho card CHƯA cấu hình condition (hiển thị nhanh PV vs SV).
      if (cardHasAlarmCondition(card)) return;

      // Skip if PV tag already has a system alarm active
      if (systemAlarmMap && systemAlarmMap.has(parseInt(pvTagId))) return;

      // Skip if card_alarm_event has already confirmed an alarm type for this card –
      // card_alarm_event uses threshold direction (High/Low) which is authoritative.
      const single3Id = parseInt(card.getAttribute('data-qtag-single3-id') || '0');
      if (single3Id > 0) {
        const s3Key = buildCardAlarmKey('single3', single3Id, null);
        if (_lastSyncedCardAlarms.has(s3Key)) {
          const confirmedType = _lastSyncedCardAlarms.get(s3Key);
          card.classList.remove('qtag-single-alarm-high', 'qtag-single-alarm-low');
          if (confirmedType === 'High') card.classList.add('qtag-single-alarm-high');
          else if (confirmedType === 'Low') card.classList.add('qtag-single-alarm-low');
          return;
        }
      }

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
        // Remove opposite tag class first so stale class doesn't corrupt card color logic
        pvTagItem.classList.remove('tag-alarm-high', 'tag-alarm-low');
        pvTagItem.classList.add('tag-alarm-active', `tag-alarm-${fallbackAlarm}`);
        const condText = fallbackAlarm === 'high'
          ? `PV (${pvVal}) > SV HIGH (${svHighVal})`
          : `PV (${pvVal}) < SV LOW (${svLowVal})`;
        pvTagItem.setAttribute('title', `⚠️ PV vs SV: ${condText}`);

        // Apply to card – always remove both classes first so LOW doesn't stay red
        card.classList.remove('qtag-single-alarm-high', 'qtag-single-alarm-low');
        if (fallbackAlarm === 'high') {
          card.classList.add('qtag-single-alarm-high');
        } else {
          card.classList.add('qtag-single-alarm-low');
        }
      } else if (pvTagItem) {
        // PV within normal range – clear fallback alarm classes from tag item, then
        // delegate card class cleanup to recalcCardAlarmState so that any still-active
        // server-confirmed card alarm (in _lastSyncedCardAlarms) is preserved correctly.
        pvTagItem.classList.remove('tag-alarm-active', 'tag-alarm-high', 'tag-alarm-low');
        pvTagItem.removeAttribute('title');
        recalcCardAlarmState(card);
      }
    });
  }

  let _lastSyncedTagAlarms = new Map();

  // Expose for external use
  window.fetchAndApplyTagAlarms = fetchAndApplyTagAlarms;

  // Periodic tag alarm sync alongside quad alarm sync
  const _tagAlarmSyncInterval = setInterval(fetchAndApplyTagAlarms, 10000);

  // ========== Card Alarm System (Qtag6, Qtag4, Qtag3, Qtag2, Single3, PV Only, PV Dual) ==========

  // Apply active card alarm states on page load
  function applyActiveCardAlarms() {
    const dataEl = document.getElementById('card-alarm-data');
    let activeAlarms = [];
    if (dataEl) {
      try {
        const dataStr = dataEl.getAttribute('data-active-alarms');
        activeAlarms = dataStr ? JSON.parse(dataStr) : [];
      } catch (e) {
        console.error('Failed to parse active card alarms:', e);
        return;
      }
    }
    if (!activeAlarms || activeAlarms.length === 0) return;

    activeAlarms.forEach(alarm => {
      // Pre-populate _lastSyncedCardAlarms từ page data để applyCardAlarmState
      // có thể tham chiếu ngay khi applyActiveTagAlarms chạy (DOMContentLoaded).
      const key = buildCardAlarmKey(alarm.card_type, alarm.card_id, alarm.column);
      _lastSyncedCardAlarms.set(key, alarm.alarm_type);
      applyCardAlarmVisual(alarm.card_type, alarm.card_id, alarm.column, alarm.alarm_type);
    });
    console.log(`[CardAlarm] Applied ${activeAlarms.length} active card alarms`);
  }

  function normalizeDualAlarmColumn(column) {
    if (column === null || column === undefined) return null;
    const value = String(column).trim().toLowerCase();
    if (!value) return null;

    if (['left', 'l', '1', '0', 'col1', 'column1', 'a'].includes(value)) {
      return 'left';
    }
    if (['right', 'r', '2', 'col2', 'column2', 'b'].includes(value)) {
      return 'right';
    }
    return null;
  }

  function applyCardAlarmVisual(cardType, cardId, column, alarmType) {
    // Find the card element based on card type using correct HTML data attributes
    let cardEl = null;
    let alarmClass = alarmType === 'High' ? 'card-alarm-high' : 'card-alarm-low';
    let removeClass = alarmType === 'High' ? 'card-alarm-low' : 'card-alarm-high';

    if (cardType === 'qtag6') {
      const card = document.querySelector(`.qtag6-card[data-qtag6-id="${cardId}"]`);
      if (card) {
        const normalizedColumn = normalizeDualAlarmColumn(column);
        if (!normalizedColumn) {
          console.warn(`[CardAlarm] Skip qtag6 alarm due to invalid column:`, { cardId, column, alarmType });
          return;
        }
        const subCards = card.querySelectorAll('.qtag6-sub-card');
        cardEl = normalizedColumn === 'left' ? subCards[0] : subCards[1];
        alarmClass = alarmType === 'High' ? 'qtag6-alarm-high' : 'qtag6-alarm-low';
        removeClass = alarmType === 'High' ? 'qtag6-alarm-low' : 'qtag6-alarm-high';
      }
    } else if (cardType === 'qtag4') {
      const card = document.querySelector(`.qtag4-card[data-qtag4-id="${cardId}"]`);
      if (card) {
        const normalizedColumn = normalizeDualAlarmColumn(column);
        if (!normalizedColumn) {
          console.warn(`[CardAlarm] Skip qtag4 alarm due to invalid column:`, { cardId, column, alarmType });
          return;
        }
        const subCards = card.querySelectorAll('.qtag6-sub-card');
        cardEl = normalizedColumn === 'left' ? subCards[0] : subCards[1];
        alarmClass = alarmType === 'High' ? 'qtag6-alarm-high' : 'qtag6-alarm-low';
        removeClass = alarmType === 'High' ? 'qtag6-alarm-low' : 'qtag6-alarm-high';
      }
    } else if (cardType === 'pv_dual') {
      // Dual-column: find sub-card by column index
      const card = document.querySelector(`.qtag-pv-dual-card[data-qtag-pv-dual-id="${cardId}"]`);
      if (card) {
        const subCards = card.querySelectorAll('.qtag-pv-dual-sub-card');
        cardEl = column === 'left' ? subCards[0] : subCards[1];
        alarmClass = alarmType === 'High' ? 'qtag-pv-dual-alarm-high' : 'qtag-pv-dual-alarm-low';
        removeClass = alarmType === 'High' ? 'qtag-pv-dual-alarm-low' : 'qtag-pv-dual-alarm-high';
      }
    } else if (cardType === 'single3') {
      cardEl = document.querySelector(`[data-qtag-single3-id="${cardId}"]`);
      alarmClass = alarmType === 'High' ? 'qtag-single-alarm-high' : 'qtag-single-alarm-low';
      removeClass = alarmType === 'High' ? 'qtag-single-alarm-low' : 'qtag-single-alarm-high';
    } else if (cardType === 'pv_only') {
      cardEl = document.querySelector(`[data-qtag-pv-id="${cardId}"]`);
      alarmClass = alarmType === 'High' ? 'qtag-single-alarm-high' : 'qtag-single-alarm-low';
      removeClass = alarmType === 'High' ? 'qtag-single-alarm-low' : 'qtag-single-alarm-high';
    } else if (cardType === 'qtag2') {
      cardEl = document.querySelector(`[data-qtag2-id="${cardId}"]`);
      alarmClass = alarmType === 'High' ? 'qtag-single-alarm-high' : 'qtag-single-alarm-low';
      removeClass = alarmType === 'High' ? 'qtag-single-alarm-low' : 'qtag-single-alarm-high';
    } else if (cardType === 'qtag3') {
      const card = document.querySelector(`.qtag3-card[data-qtag3-id="${cardId}"]`);
      if (card) {
        cardEl = card.querySelector('.qtag6-sub-card') || card;
      }
      alarmClass = alarmType === 'High' ? 'qtag6-alarm-high' : 'qtag6-alarm-low';
      removeClass = alarmType === 'High' ? 'qtag6-alarm-low' : 'qtag6-alarm-high';
    }

    if (cardEl) {
      // Check if monitoring is enabled for this card
      if (!isCardMonitoringEnabled(cardEl)) return;
      cardEl.classList.remove(removeClass);
      cardEl.classList.add(alarmClass);
    }
  }

  function removeCardAlarmVisual(cardType, cardId, column) {
    let cardEl = null;
    let classesToRemove = ['card-alarm-high', 'card-alarm-low'];

    if (cardType === 'qtag6') {
      const card = document.querySelector(`.qtag6-card[data-qtag6-id="${cardId}"]`);
      if (card) {
        const normalizedColumn = normalizeDualAlarmColumn(column);
        if (!normalizedColumn) {
          console.warn(`[CardAlarm] Skip qtag6 clear due to invalid column:`, { cardId, column });
          return;
        }
        const subCards = card.querySelectorAll('.qtag6-sub-card');
        cardEl = normalizedColumn === 'left' ? subCards[0] : subCards[1];
      }
      classesToRemove = ['qtag6-alarm-high', 'qtag6-alarm-low'];
    } else if (cardType === 'qtag4') {
      const card = document.querySelector(`.qtag4-card[data-qtag4-id="${cardId}"]`);
      if (card) {
        const normalizedColumn = normalizeDualAlarmColumn(column);
        if (!normalizedColumn) {
          console.warn(`[CardAlarm] Skip qtag4 clear due to invalid column:`, { cardId, column });
          return;
        }
        const subCards = card.querySelectorAll('.qtag6-sub-card');
        cardEl = normalizedColumn === 'left' ? subCards[0] : subCards[1];
      }
      classesToRemove = ['qtag6-alarm-high', 'qtag6-alarm-low'];
    } else if (cardType === 'pv_dual') {
      const card = document.querySelector(`.qtag-pv-dual-card[data-qtag-pv-dual-id="${cardId}"]`);
      if (card) {
        const subCards = card.querySelectorAll('.qtag-pv-dual-sub-card');
        cardEl = column === 'left' ? subCards[0] : subCards[1];
      }
      classesToRemove = ['qtag-pv-dual-alarm-high', 'qtag-pv-dual-alarm-low'];
    } else if (cardType === 'single3') {
      cardEl = document.querySelector(`[data-qtag-single3-id="${cardId}"]`);
      classesToRemove = ['qtag-single-alarm-high', 'qtag-single-alarm-low'];
    } else if (cardType === 'pv_only') {
      cardEl = document.querySelector(`[data-qtag-pv-id="${cardId}"]`);
      classesToRemove = ['qtag-single-alarm-high', 'qtag-single-alarm-low'];
    } else if (cardType === 'qtag2') {
      cardEl = document.querySelector(`[data-qtag2-id="${cardId}"]`);
      classesToRemove = ['qtag-single-alarm-high', 'qtag-single-alarm-low'];
    } else if (cardType === 'qtag3') {
      const card = document.querySelector(`.qtag3-card[data-qtag3-id="${cardId}"]`);
      if (card) {
        cardEl = card.querySelector('.qtag6-sub-card') || card;
      }
      classesToRemove = ['qtag6-alarm-high', 'qtag6-alarm-low'];
    }

    if (cardEl) {
      cardEl.classList.remove(...classesToRemove);
    }
  }

  function buildCardAlarmKey(cardType, cardId, column) {
    let normalizedColumn = column;
    if (cardType === 'qtag6' || cardType === 'qtag4') {
      normalizedColumn = normalizeDualAlarmColumn(column) || '';
    } else if (cardType === 'pv_dual') {
      normalizedColumn = (column === 'left' || column === 'right') ? column : '';
    } else {
      normalizedColumn = '';
    }
    return `${cardType}|${cardId}|${normalizedColumn}`;
  }

  function parseCardAlarmKey(key) {
    const [cardType, cardIdRaw, columnRaw] = key.split('|');
    return {
      cardType,
      cardId: parseInt(cardIdRaw, 10),
      column: columnRaw || null
    };
  }

  function fetchAndApplyCardAlarms() {
    const subdashId = currentSubdashId;
    if (!subdashId) return;

    fetch(`/subdash/${subdashId}/api/active_card_alarms`)
      .then(res => res.json())
      .then(data => {
        if (!data.success || !Array.isArray(data.alarms)) return;

        const serverAlarmMap = new Map();

        // Build current server snapshot
        data.alarms.forEach(alarm => {
          const key = buildCardAlarmKey(alarm.card_type, alarm.card_id, alarm.column);
          serverAlarmMap.set(key, alarm.alarm_type);
        });

        // Delta sync: remove alarms no longer active
        _lastSyncedCardAlarms.forEach((_alarmType, key) => {
          if (!serverAlarmMap.has(key)) {
            const parsed = parseCardAlarmKey(key);
            if (!Number.isNaN(parsed.cardId)) {
              removeCardAlarmVisual(parsed.cardType, parsed.cardId, parsed.column);
            }
          }
        });

        // Delta sync: apply only new/changed alarms
        data.alarms.forEach(alarm => {
          const key = buildCardAlarmKey(alarm.card_type, alarm.card_id, alarm.column);
          const previousType = _lastSyncedCardAlarms.get(key);
          if (previousType === alarm.alarm_type) {
            return;
          }
          applyCardAlarmVisual(alarm.card_type, alarm.card_id, alarm.column, alarm.alarm_type);
        });

        _lastSyncedCardAlarms = serverAlarmMap;

        console.log(`[CardAlarmSync] Synced ${data.alarms.length} active card alarms`);
      })
      .catch(err => {
        console.warn('[CardAlarmSync] Failed to fetch active card alarms:', err);
      });
  }

  let _lastSyncedCardAlarms = new Map();

  window.fetchAndApplyCardAlarms = fetchAndApplyCardAlarms;
  const _cardAlarmSyncInterval = setInterval(fetchAndApplyCardAlarms, 10000);

  // Apply card alarms on page load
  applyActiveCardAlarms();

  // ========== Card Alarm Toggle Switch ==========

  /**
   * Load trạng thái enabled của tất cả card conditions trong subdashboard.
   * Hiện toggle wrap nếu card đã có conditions, và set giá trị checked theo DB.
   */
  function loadCardConditionToggles() {
    if (!currentSubdashId) return;
    fetch(`/subdash/${currentSubdashId}/api/card_conditions_states`)
      .then(res => res.json())
      .then(data => {
        if (!data.success) return;
        const states = data.states || {};
        Object.entries(states).forEach(([key, enabled]) => {
          // key format: "qtag2_5", "qtag3_2", etc.
          const lastUnderscore = key.lastIndexOf('_');
          const cardType = key.substring(0, lastUnderscore);
          const cardId = key.substring(lastUnderscore + 1);

          // Luôn gắn data-monitoring-enabled, KHÔNG hiện toggle trên card (chỉ dùng settings modal)
          const wrap = document.querySelector(
            `.card-alarm-toggle-wrap[data-card-type="${cardType}"][data-card-id="${cardId}"]`
          );
          if (wrap) {
            wrap.dataset.monitoringEnabled = enabled ? 'true' : 'false';
            // Luôn giữ ẩn
            wrap.classList.add('d-none');
            if (!enabled) {
              // Xóa alarm visuals ngay sau khi biết trạng thái disabled
              // (fix race condition: alarms có thể đã apply trước khi states load xong)
              removeCardAlarmVisual(cardType, parseInt(cardId), 'left');
              removeCardAlarmVisual(cardType, parseInt(cardId), 'right');
              // Xóa tag-alarm visuals bên trong card
              const rootCard = wrap.closest(
                '[data-qtag6-id], [data-qtag4-id], [data-qtag-single3-id], ' +
                '[data-qtag-pv-id], [data-qtag2-id], [data-qtag3-id], [data-qtag-pv-dual-id]'
              );
              if (rootCard) {
                rootCard.querySelectorAll('.tag-alarm-active').forEach(el => {
                  el.classList.remove('tag-alarm-active', 'tag-alarm-high', 'tag-alarm-low');
                  el.removeAttribute('title');
                });
                // Xóa sub-card alarm classes (qtag6/qtag4)
                rootCard.querySelectorAll('.qtag6-sub-card').forEach(sub => {
                  sub.classList.remove('qtag6-alarm-high', 'qtag6-alarm-low');
                });
                rootCard.querySelectorAll('.qtag-single-sub-card').forEach(sub => {
                  sub.classList.remove('qtag-single-alarm-high', 'qtag-single-alarm-low');
                });
              }
            }
          }

          // Set trạng thái checked
          const toggle = document.querySelector(
            `.card-alarm-toggle[data-card-type="${cardType}"][data-card-id="${cardId}"]`
          );
          if (toggle) toggle.checked = enabled;
        });
      })
      .catch(err => console.warn('[CardToggle] Could not load states:', err));
  }

  // Load toggle states khi page load
  loadCardConditionToggles();

  // Handle toggle click: gọi API toggle_enabled
  document.addEventListener('change', function (e) {
    if (!e.target.classList.contains('card-alarm-toggle')) return;
    const cardType = e.target.dataset.cardType;
    const cardId = e.target.dataset.cardId;
    const enabled = e.target.checked;

    fetch(`/subdash/${currentSubdashId}/card_condition/${cardType}/${cardId}/toggle_enabled`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ enabled })
    })
      .then(res => res.json())
      .then(result => {
        if (!result.success) {
          // Revert toggle nếu lỗi
          e.target.checked = !enabled;
          Swal.fire({
            icon: 'warning',
            title: 'Chưa có Alarm Conditions',
            text: result.message || 'Vui lòng Set Alarm Conditions trước khi bật.',
            timer: 3000,
            showConfirmButton: false
          });
        } else {
          // Nếu disable → xóa màu alarm trên card và ẩn toggle
          if (!enabled) {
            removeCardAlarmVisual(cardType, parseInt(cardId), 'left');
            removeCardAlarmVisual(cardType, parseInt(cardId), 'right');
            const wrap = document.querySelector(
              `.card-alarm-toggle-wrap[data-card-type="${cardType}"][data-card-id="${cardId}"]`
            );
            if (wrap) {
              wrap.dataset.monitoringEnabled = 'false';
              wrap.classList.add('d-none');
            }
          } else {
            // Enable: cập nhật data attribute
            const wrap = document.querySelector(
              `.card-alarm-toggle-wrap[data-card-type="${cardType}"][data-card-id="${cardId}"]`
            );
            if (wrap) wrap.dataset.monitoringEnabled = 'true';
          }
          console.log(`[CardToggle] ${cardType}/${cardId} alarm = ${enabled}`);
        }
      })
      .catch(err => {
        e.target.checked = !enabled;
        console.error('[CardToggle] Error:', err);
      });
  });

  // Khi save conditions thành công trong modal → hiện toggle và set state
  // (patch vào saveCardConditionsToAPI trong card_conditions.js)
  window._onCardConditionSaved = function (cardType, cardId, enabled) {
    const wrap = document.querySelector(
      `.card-alarm-toggle-wrap[data-card-type="${cardType}"][data-card-id="${cardId}"]`
    );
    if (wrap) {
      wrap.dataset.monitoringEnabled = enabled ? 'true' : 'false';
      // Luôn giữ ẩn - monitoring toggle chỉ dùng trong settings modal
      wrap.classList.add('d-none');
    }
    const toggle = document.querySelector(
      `.card-alarm-toggle[data-card-type="${cardType}"][data-card-id="${cardId}"]`
    );
    if (toggle) toggle.checked = enabled;
  };

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
        // Force all quad tag values tied to this device to 0 when offline.
        // Exclude fixed-SV rows to prevent overwriting static values.
        document.querySelectorAll(`[id^="quad-tag-val-"][id$="-${tagId}"]:not([id^="quad-tag-val-fixed-"])`).forEach(el => {
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

    // Fallback cho id kết thúc bằng -tagId (exclude fixed-SV rows).
    document.querySelectorAll(`[id^="quad-tag-val-"][id$="-${tagId}"]:not([id^="quad-tag-val-fixed-"])`).forEach(span => {
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
          // Exclude fixed-SV rows (id^="quad-tag-val-fixed-") to prevent overwriting static values.
          const quadValElements = document.querySelectorAll(`[id^="quad-tag-val-"][id$="-${tag.id}"]:not([id^="quad-tag-val-fixed-"])`);

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
          
          // Backend workers (tcp_worker/rtu_worker) đã format tag.value qua shared/formatting.
          // Frontend chỉ render string được BE gửi về, không tính toán lại định dạng.
          const _tagDisplayVal = (perTagStatus === 'disconnected')
            ? '0'
            : ((tag.value !== null && tag.value !== undefined) ? String(tag.value) : '0');

          if (valEl) {
            const oldVal = valEl.textContent;
            const newVal = _tagDisplayVal;

            if (oldVal !== newVal) {
              pending.push({ valEl, newVal, tag });
            }
          }
          
          // ✅ Loop qua TẤT CẢ quad tag elements với tag.id này
          quadValElements.forEach(quadValEl => {
            const oldQuadVal = quadValEl.textContent;
            // Khi offline thì ép về 0, tránh giữ giá trị cũ
            const newQuadVal = _tagDisplayVal;
            
            if (oldQuadVal !== newQuadVal) {
              pending.push({ valEl: quadValEl, newVal: newQuadVal, tag });
              updateQuadLastUpdated();
            }
          });

          // Ngay cả khi giá trị không đổi, vẫn cập nhật thời gian nhận bản tin
          if (quadValElements.length > 0) {
            updateQuadLastUpdated();
          }

          // ✅ Update Quad SV elements (tag-based only, fixed values are static)
          const quadSvElements = document.querySelectorAll(
            `[id^="quad-tag-val-sv-"][data-tag-id="${tag.id}"]`
          );
          quadSvElements.forEach(el => {
            const newVal = _tagDisplayVal;
            if (el.textContent !== newVal) {
              pending.push({ valEl: el, newVal: newVal, tag });
            }
          });

          // ✅ Update Qtag6 values - PV elements with pattern qtag6-val-{cardId}-{tagId}
          const qtag6ValElements = document.querySelectorAll(`[id^="qtag6-val-"][id$="-${tag.id}"]`);
          qtag6ValElements.forEach(el => {
            const newVal = _tagDisplayVal;
            if (el.textContent !== newVal) {
              pending.push({ valEl: el, newVal: newVal, tag });
            }
            updateLastUpdatedForElement(el, nowForTag);
          });

          // ✅ Update Qtag6 SV elements (tag-based only, fixed values are static)
          const qtag6SvElements = document.querySelectorAll(
            `.qtag6-sv-low[data-tag-id="${tag.id}"], .qtag6-sv-high[data-tag-id="${tag.id}"]`
          );
          qtag6SvElements.forEach(el => {
            const newVal = _tagDisplayVal;
            if (el.textContent !== newVal) {
              pending.push({ valEl: el, newVal: newVal, tag });
            }
          });

          // ✅ Update Qtag4 values - PV elements with pattern qtag4-val-{cardId}-{tagId}
          const qtag4ValElements = document.querySelectorAll(`[id^="qtag4-val-"][id$="-${tag.id}"]`);
          qtag4ValElements.forEach(el => {
            const newVal = _tagDisplayVal;
            if (el.textContent !== newVal) {
              pending.push({ valEl: el, newVal: newVal, tag });
            }
            updateLastUpdatedForElement(el, nowForTag);
          });

          // ✅ Update Qtag4 SV elements (tag-based only, fixed values are static)
          const qtag4SvElements = document.querySelectorAll(
            `.qtag4-sv-value[data-tag-id="${tag.id}"]`
          );
          qtag4SvElements.forEach(el => {
            const newVal = _tagDisplayVal;
            if (el.textContent !== newVal) {
              pending.push({ valEl: el, newVal: newVal, tag });
            }
          });

          // ✅ Update Qtag3 PV values - pattern qtag3-val-{cardId}-{tagId}
          const qtag3ValElements = document.querySelectorAll(`[id^="qtag3-val-"][id$="-${tag.id}"]`);
          qtag3ValElements.forEach(el => {
            const newVal = _tagDisplayVal;
            if (el.textContent !== newVal) {
              pending.push({ valEl: el, newVal: newVal, tag });
            }
            updateLastUpdatedForElement(el, nowForTag);
          });

          // ✅ Update Qtag3 SV elements (tag-based only)
          const qtag3SvElements = document.querySelectorAll(
            `.qtag3-sv-low[data-tag-id="${tag.id}"], .qtag3-sv-high[data-tag-id="${tag.id}"]`
          );
          qtag3SvElements.forEach(el => {
            const newVal = _tagDisplayVal;
            if (el.textContent !== newVal) {
              pending.push({ valEl: el, newVal: newVal, tag });
            }
          });

          // ✅ Update Qtag2 PV values - pattern qtag2-val-{cardId}-{tagId}
          const qtag2ValElements = document.querySelectorAll(`[id^="qtag2-val-"][id$="-${tag.id}"]`);
          qtag2ValElements.forEach(el => {
            const newVal = _tagDisplayVal;
            if (el.textContent !== newVal) {
              pending.push({ valEl: el, newVal: newVal, tag });
            }
            updateLastUpdatedForElement(el, nowForTag);
          });

          // ✅ Update Qtag2 SV elements (tag-based only)
          const qtag2SvElements = document.querySelectorAll(`.qtag2-sv-value[data-tag-id="${tag.id}"]`);
          qtag2SvElements.forEach(el => {
            const newVal = _tagDisplayVal;
            if (el.textContent !== newVal) {
              pending.push({ valEl: el, newVal: newVal, tag });
            }
          });

          // ✅ Update Qtag Single3 PV values
          const single3PvElements = document.querySelectorAll(`[id^="qtag-single3-pv-"][id$="-${tag.id}"]`);
          single3PvElements.forEach(el => {
            const newVal = _tagDisplayVal;
            if (el.textContent !== newVal) {
              pending.push({ valEl: el, newVal: newVal, tag });
            }
            updateLastUpdatedForElement(el, nowForTag);
          });

          // ✅ Update Qtag Single3 SV HIGH/LOW (matched by data-tag-id attribute)
          const single3SvElements = document.querySelectorAll(`.qtag-single-sv-low[data-tag-id="${tag.id}"], .qtag-single-sv-high[data-tag-id="${tag.id}"]`);
          single3SvElements.forEach(el => {
            const newVal = _tagDisplayVal;
            if (el.textContent !== newVal) {
              pending.push({ valEl: el, newVal: newVal, tag });
            }
          });

          // ✅ Update Qtag PV Only values
          const pvOnlyElements = document.querySelectorAll(`[id^="qtag-pv-val-"][id$="-${tag.id}"]`);
          pvOnlyElements.forEach(el => {
            const newVal = _tagDisplayVal;
            if (el.textContent !== newVal) {
              pending.push({ valEl: el, newVal: newVal, tag });
            }
            updateLastUpdatedForElement(el, nowForTag);
          });

          // ✅ Update Qtag PV Dual values
          const pvDualElements = document.querySelectorAll(`[id^="qtag-pv-dual-val-"][id$="-${tag.id}"]`);
          pvDualElements.forEach(el => {
            const newVal = _tagDisplayVal;
            if (el.textContent !== newVal) {
              pending.push({ valEl: el, newVal: newVal, tag });
            }
            updateLastUpdatedForElement(el, nowForTag);
          });

          // Check if new card types also need timer reset
          if (qtag6ValElements.length > 0 || single3PvElements.length > 0 || pvOnlyElements.length > 0 || single3SvElements.length > 0 || pvDualElements.length > 0) {
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
        
        // Dùng display fields từ BE (alarm_strategies.py build_event)
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
        const outTag1Val = data.tag1_display !== undefined && data.tag1_display !== null
          ? (data.tag1_display || 'N/A')
          : (data.tag1_value !== null && data.tag1_value !== undefined ? String(data.tag1_value) : 'N/A');
        const outTag2Val = data.tag2_display !== undefined && data.tag2_display !== null
          ? (data.tag2_display || 'N/A')
          : (data.tag2_value !== null && data.tag2_value !== undefined ? String(data.tag2_value) : 'N/A');
        const outOperator = data.operator || '>';
        const outThreshold = data.threshold_display !== undefined && data.threshold_display !== null
          ? (data.threshold_display || 'N/A')
          : (data.threshold !== null && data.threshold !== undefined ? String(data.threshold) : 'N/A');

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
        // Cập nhật snapshot để fetchAndApplyTagAlarms có thể detect và xóa khi alarm clear
        const tagSnapshot = `${level || ''}|${data.value ?? ''}||`;
        _lastSyncedTagAlarms.set(tagId, tagSnapshot);
        console.log(`✅ Tag alarm applied: tag ${tagId} - ${level}`);

      } else if (status === 'OUTGOING') {
        removeTagAlarmVisual(tagId);
        // Xóa khỏi snapshot để periodic sync không re-apply
        _lastSyncedTagAlarms.delete(tagId);
        console.log(`✅ Tag alarm cleared: tag ${tagId}`);
      }
    });

    // Listen for card alarm events (Qtag6, Single3, PV Only, PV Dual)
    socket.on('card_alarm_event', function (data) {
      console.log('🚨 Card alarm event received:', data);

      const cardType = data.card_type;
      const cardId = data.card_id;
      const column = data.column;
      const alarmType = data.alarm_type;
      const status = data.status;

      if (status === 'INCOMING') {
        applyCardAlarmVisual(cardType, cardId, column, alarmType);
        // Cập nhật _lastSyncedCardAlarms để fetchAndApplyCardAlarms có thể
        // detect và xóa alarm color khi alarm đã clear trên server
        const cardKey = buildCardAlarmKey(cardType, cardId, column);
        _lastSyncedCardAlarms.set(cardKey, alarmType);
        console.log(`✅ Card alarm applied: ${cardType}/${cardId} ${column} (${alarmType})`);
      } else if (status === 'OUTGOING') {
        removeCardAlarmVisual(cardType, cardId, column);
        // Xóa khỏi snapshot để periodic sync không re-apply
        const cardKey = buildCardAlarmKey(cardType, cardId, column);
        _lastSyncedCardAlarms.delete(cardKey);
        console.log(`✅ Card alarm cleared: ${cardType}/${cardId} ${column}`);
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
      setTimeout(fetchAndApplyCardAlarms, 1500);

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
      setTimeout(fetchAndApplyCardAlarms, 1500);
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
  // ★ SV Type Toggle — Fixed Value ↔ From Tag
  // Scoped to the closest <form> to avoid cross-modal interference
  // when multiple modals are present in the DOM simultaneously.
  // ============================================================
  document.addEventListener('change', function (e) {
    if (!e.target.classList.contains('sv-type-select')) return;
    const target = e.target.dataset.target;
    const val    = e.target.value;
    const cssBase = target.replace(/_/g, '-');
    // Scope lookup to the containing form so selectors are unique per modal
    const scope  = e.target.closest('form') || document;
    const fixedGroup = scope.querySelector(`.${cssBase}-fixed-group`);
    const tagGroup   = scope.querySelector(`.${cssBase}-tag-group`);
    if (fixedGroup) fixedGroup.style.display = val === 'fixed' ? '' : 'none';
    if (tagGroup)   tagGroup.style.display   = val === 'tag'   ? '' : 'none';
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

      // Validate PV tags (tag1, tag2 required)
      for (let i = 1; i <= 2; i++) {
        if (!form.querySelector(`[name="tag${i}_id"]`).value) {
          errorEl.textContent = 'Please select both PV tags (Tag 1 and Tag 2)';
          errorEl.style.display = 'block';
          errorEl.scrollIntoView({ behavior: 'smooth', block: 'center' });
          return;
        }
      }

      const groupId = form.querySelector('[name="group_id"]').value;
      const newGroupName = form.querySelector('[name="new_group_name"]').value.trim();
      if (!groupId && !newGroupName) {
        errorEl.textContent = 'Please select a group or enter a new group name';
        errorEl.style.display = 'block';
        errorEl.scrollIntoView({ behavior: 'smooth', block: 'center' });
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
          Swal.fire({ title: 'Error', text: data.message || data.error || 'Failed to add Qtag6 card', icon: 'error' });
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
  // ★ ADD: Qtag4 Card Form
  // ============================================================
  const addQtag4Form = document.getElementById('addQtag4Form');
  if (addQtag4Form) {
    addQtag4Form.addEventListener('submit', function (e) {
      e.preventDefault();
      const form = e.target;
      const errorEl = document.getElementById('qtag4-error-message');
      errorEl.style.display = 'none';

      for (let i = 1; i <= 2; i++) {
        if (!form.querySelector(`[name="tag${i}_id"]`).value) {
          errorEl.textContent = 'Please select both PV tags (Tag 1 and Tag 2)';
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

      Swal.fire({ title: 'Adding Qtag4 Card...', allowOutsideClick: false, didOpen: () => Swal.showLoading() });

      const formData = new FormData(form);
      fetch(`/subdash/${currentSubdashId}/add_qtag4`, {
        method: 'POST',
        body: formData
      })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          Swal.fire({ title: 'Success!', text: 'Qtag4 card added', icon: 'success', timer: 2000 }).then(() => location.reload());
        } else {
          Swal.fire({ title: 'Error', text: data.message || 'Failed to add Qtag4 card', icon: 'error' });
        }
      })
      .catch(err => {
        Swal.fire({ title: 'Error', text: err.message, icon: 'error' });
      });
    });
  }

  // ============================================================
  // ★ DELETE: Qtag4 Card
  // ============================================================
  document.addEventListener('click', function (e) {
    const btn = e.target.closest('.delete-qtag4-btn');
    if (!btn) return;
    e.preventDefault();
    const cardId = btn.dataset.cardId;

    Swal.fire({
      title: 'Delete Qtag4 Card?',
      text: 'This action cannot be undone.',
      icon: 'warning',
      showCancelButton: true,
      confirmButtonColor: '#dc3545',
      confirmButtonText: 'Yes, delete it!',
      cancelButtonText: 'Cancel'
    }).then(result => {
      if (!result.isConfirmed) return;
      Swal.fire({ title: 'Deleting...', allowOutsideClick: false, didOpen: () => Swal.showLoading() });

      fetch(`/subdash/${currentSubdashId}/delete_qtag4/${cardId}`, { method: 'DELETE' })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          Swal.fire({ title: 'Deleted!', icon: 'success', timer: 2000 }).then(() => location.reload());
        } else {
          Swal.fire({ title: 'Error', text: data.message || 'Failed to delete', icon: 'error' });
        }
      })
      .catch(err => Swal.fire({ title: 'Error', text: err.message, icon: 'error' }));
    });
  });

  // ============================================================
  // ★ RENAME: Qtag4 Card
  // ============================================================
  document.addEventListener('click', function (e) {
    const btn = e.target.closest('.rename-qtag4-btn');
    if (!btn) return;
    e.preventDefault();
    const cardId = btn.dataset.cardId;
    const target = btn.dataset.target || 'card';
    const card = btn.closest('.qtag4-card') || document.querySelector(`.qtag4-card[data-qtag4-id="${cardId}"]`);
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
      currentTitle = titleEl ? titleEl.textContent.trim() : 'Qtag4 Card';
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

      fetch(`/subdash/${currentSubdashId}/qtag4/${cardId}/rename`, {
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
          Swal.fire({ icon: 'error', title: 'Lỗi', text: data.message || 'Không thể cập nhật' });
        }
      })
      .catch(err => Swal.fire({ icon: 'error', title: 'Lỗi', text: err.message }));
    });
  });

  // ============================================================
  // ★ EDIT: Qtag4 Card - Open modal and populate
  // ============================================================
  document.addEventListener('click', function (e) {
    const btn = e.target.closest('.edit-qtag4-btn');
    if (!btn) return;
    e.preventDefault();

    document.getElementById('editQtag4Id').value = btn.dataset.cardId;
    const card = btn.closest('.qtag4-card');
    if (card) {
      document.getElementById('editQtag4LeftTitle').value = card.dataset.leftTitle || '';
      document.getElementById('editQtag4RightTitle').value = card.dataset.rightTitle || '';
    }
    // PV tags
    for (let i = 1; i <= 2; i++) {
      const selectEl = document.getElementById(`editQtag4Tag${i}`);
      if (selectEl) selectEl.value = btn.dataset[`tag${i}Id`] || '';
    }

    // SV fields
    const svFields = [
      { prefix: 'left_sv', typeEl: 'editQtag4LeftSvType', fixedEl: 'editQtag4LeftSvFixed', fixedUnitEl: 'editQtag4LeftSvFixedUnit', tagEl: 'editQtag4Tag3', dataType: 'leftSvType', dataFixed: 'leftSvFixed', dataFixedUnit: 'leftSvFixedUnit', dataTag: 'tag3Id' },
      { prefix: 'right_sv', typeEl: 'editQtag4RightSvType', fixedEl: 'editQtag4RightSvFixed', fixedUnitEl: 'editQtag4RightSvFixedUnit', tagEl: 'editQtag4Tag4', dataType: 'rightSvType', dataFixed: 'rightSvFixed', dataFixedUnit: 'rightSvFixedUnit', dataTag: 'tag4Id' },
    ];
    svFields.forEach(f => {
      const svType = btn.dataset[f.dataType] || 'tag';
      document.getElementById(f.typeEl).value = svType;
      const target = `edit_qtag4_${f.prefix}`;
      const fixedGrp = document.querySelector(`.${target.replace(/_/g, '-')}-fixed-group`);
      const tagGrp = document.querySelector(`.${target.replace(/_/g, '-')}-tag-group`);
      if (fixedGrp && tagGrp) {
        fixedGrp.style.display = svType === 'fixed' ? '' : 'none';
        tagGrp.style.display = svType === 'tag' ? '' : 'none';
      }
      if (svType === 'fixed') {
        document.getElementById(f.fixedEl).value = btn.dataset[f.dataFixed] || '';
        const fixedUnitEl = document.getElementById(f.fixedUnitEl);
        if (fixedUnitEl) fixedUnitEl.value = btn.dataset[f.dataFixedUnit] || '';
      } else {
        document.getElementById(f.tagEl).value = btn.dataset[f.dataTag] || '';
      }
    });

    const modal = new bootstrap.Modal(document.getElementById('editQtag4Modal'));
    modal.show();
  });

  // Edit Qtag4 form submission
  const editQtag4Form = document.getElementById('editQtag4Form');
  if (editQtag4Form) {
    editQtag4Form.addEventListener('submit', function (e) {
      e.preventDefault();
      const cardId = document.getElementById('editQtag4Id').value;
      const errorEl = document.getElementById('edit-qtag4-error-message');
      errorEl.style.display = 'none';

      for (let i = 1; i <= 2; i++) {
        if (!document.getElementById(`editQtag4Tag${i}`).value) {
          errorEl.textContent = 'Please select both PV tags';
          errorEl.style.display = 'block';
          return;
        }
      }

      Swal.fire({ title: 'Updating Qtag4...', allowOutsideClick: false, didOpen: () => Swal.showLoading() });

      const formData = new FormData(editQtag4Form);
      fetch(`/subdash/${currentSubdashId}/update_qtag4/${cardId}`, {
        method: 'POST',
        body: formData
      })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          Swal.fire({ title: 'Success!', text: 'Qtag4 updated', icon: 'success', timer: 2000 }).then(() => location.reload());
        } else {
          Swal.fire({ title: 'Error', text: data.message || 'Failed to update', icon: 'error' });
        }
      })
      .catch(err => Swal.fire({ title: 'Error', text: err.message, icon: 'error' }));
    });
  }

  // ============================================================
  // ★ ADD: Qtag3 Card Form
  // ============================================================
  const addQtag3Form = document.getElementById('addQtag3Form');
  if (addQtag3Form) {
    addQtag3Form.addEventListener('submit', function (e) {
      e.preventDefault();
      const form = e.target;
      const errorEl = document.getElementById('qtag3-error-message');
      errorEl.style.display = 'none';

      if (!form.querySelector('[name="tag1_id"]').value) {
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

      Swal.fire({ title: 'Adding Qtag3 Card...', allowOutsideClick: false, didOpen: () => Swal.showLoading() });

      const formData = new FormData(form);
      fetch(`/subdash/${currentSubdashId}/add_qtag3`, {
        method: 'POST',
        body: formData
      })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          Swal.fire({ title: 'Success!', text: 'Qtag3 card added', icon: 'success', timer: 2000 }).then(() => location.reload());
        } else {
          Swal.fire({ title: 'Error', text: data.message || 'Failed to add Qtag3 card', icon: 'error' });
        }
      })
      .catch(err => {
        Swal.fire({ title: 'Error', text: err.message, icon: 'error' });
      });
    });
  }

  // ============================================================
  // ★ DELETE: Qtag3 Card
  // ============================================================
  document.addEventListener('click', function (e) {
    const btn = e.target.closest('.delete-qtag3-btn');
    if (!btn) return;
    e.preventDefault();
    const cardId = btn.dataset.cardId;

    Swal.fire({
      title: 'Delete Qtag3 Card?',
      text: 'This action cannot be undone.',
      icon: 'warning',
      showCancelButton: true,
      confirmButtonColor: '#dc3545',
      confirmButtonText: 'Yes, delete it!',
      cancelButtonText: 'Cancel'
    }).then(result => {
      if (!result.isConfirmed) return;
      Swal.fire({ title: 'Deleting...', allowOutsideClick: false, didOpen: () => Swal.showLoading() });

      fetch(`/subdash/${currentSubdashId}/delete_qtag3/${cardId}`, { method: 'DELETE' })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          Swal.fire({ title: 'Deleted!', icon: 'success', timer: 2000 }).then(() => location.reload());
        } else {
          Swal.fire({ title: 'Error', text: data.message || 'Failed to delete', icon: 'error' });
        }
      })
      .catch(err => Swal.fire({ title: 'Error', text: err.message, icon: 'error' }));
    });
  });

  // ============================================================
  // ★ RENAME: Qtag3 Card
  // ============================================================
  document.addEventListener('click', function (e) {
    const btn = e.target.closest('.rename-qtag3-btn');
    if (!btn) return;
    e.preventDefault();
    const cardId = btn.dataset.cardId;
    const target = btn.dataset.target || 'card';
    const card = btn.closest('.qtag3-card') || document.querySelector(`.qtag3-card[data-qtag3-id="${cardId}"]`);
    if (!card) return;

    let currentTitle;
    if (target === 'column') {
      const titleEl = card.querySelector('.qtag6-sub-title');
      currentTitle = titleEl ? titleEl.textContent.trim() : '';
    } else {
      const titleEl = card.querySelector('.fw-semibold');
      currentTitle = titleEl ? titleEl.textContent.trim() : 'Qtag3 Card';
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

      fetch(`/subdash/${currentSubdashId}/qtag3/${cardId}/rename`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ target: target, title: result.value.trim() })
      })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          Swal.fire({ icon: 'success', title: 'Thành công', timer: 1500, showConfirmButton: false }).then(() => location.reload());
        } else {
          Swal.fire({ icon: 'error', title: 'Lỗi', text: data.message || 'Không thể cập nhật' });
        }
      })
      .catch(err => Swal.fire({ icon: 'error', title: 'Lỗi', text: err.message }));
    });
  });

  // ============================================================
  // ★ EDIT: Qtag3 Card - Open modal and populate
  // ============================================================
  document.addEventListener('click', function (e) {
    const btn = e.target.closest('.edit-qtag3-btn');
    if (!btn) return;
    e.preventDefault();

    document.getElementById('editQtag3Id').value = btn.dataset.cardId;
    // PV tag
    document.getElementById('editQtag3Tag1').value = btn.dataset.tag1Id || '';

    // SV HIGH
    const svHighType = btn.dataset.svHighType || 'tag';
    document.getElementById('editQtag3SvHighType').value = svHighType;
    const highFixedGrp = document.querySelector('.edit-qtag3-sv-high-fixed-group');
    const highTagGrp = document.querySelector('.edit-qtag3-sv-high-tag-group');
    if (highFixedGrp && highTagGrp) {
      highFixedGrp.style.display = svHighType === 'fixed' ? '' : 'none';
      highTagGrp.style.display = svHighType === 'tag' ? '' : 'none';
    }
    if (svHighType === 'fixed') {
      document.getElementById('editQtag3SvHighFixed').value = btn.dataset.svHighFixed || '';
      document.getElementById('editQtag3SvHighFixedUnit').value = btn.dataset.svHighFixedUnit || '';
    } else {
      document.getElementById('editQtag3Tag2').value = btn.dataset.tag2Id || '';
    }

    // SV LOW
    const svLowType = btn.dataset.svLowType || 'tag';
    document.getElementById('editQtag3SvLowType').value = svLowType;
    const lowFixedGrp = document.querySelector('.edit-qtag3-sv-low-fixed-group');
    const lowTagGrp = document.querySelector('.edit-qtag3-sv-low-tag-group');
    if (lowFixedGrp && lowTagGrp) {
      lowFixedGrp.style.display = svLowType === 'fixed' ? '' : 'none';
      lowTagGrp.style.display = svLowType === 'tag' ? '' : 'none';
    }
    if (svLowType === 'fixed') {
      document.getElementById('editQtag3SvLowFixed').value = btn.dataset.svLowFixed || '';
      document.getElementById('editQtag3SvLowFixedUnit').value = btn.dataset.svLowFixedUnit || '';
    } else {
      document.getElementById('editQtag3Tag3').value = btn.dataset.tag3Id || '';
    }

    const modal = new bootstrap.Modal(document.getElementById('editQtag3Modal'));
    modal.show();
  });

  // Edit Qtag3 form submission
  const editQtag3Form = document.getElementById('editQtag3Form');
  if (editQtag3Form) {
    editQtag3Form.addEventListener('submit', function (e) {
      e.preventDefault();
      const cardId = document.getElementById('editQtag3Id').value;
      const errorEl = document.getElementById('edit-qtag3-error-message');
      errorEl.style.display = 'none';

      if (!document.getElementById('editQtag3Tag1').value) {
        errorEl.textContent = 'Please select a PV tag';
        errorEl.style.display = 'block';
        return;
      }

      Swal.fire({ title: 'Updating Qtag3...', allowOutsideClick: false, didOpen: () => Swal.showLoading() });

      const formData = new FormData(editQtag3Form);
      fetch(`/subdash/${currentSubdashId}/update_qtag3/${cardId}`, {
        method: 'POST',
        body: formData
      })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          Swal.fire({ title: 'Success!', text: 'Qtag3 updated', icon: 'success', timer: 2000 }).then(() => location.reload());
        } else {
          Swal.fire({ title: 'Error', text: data.message || 'Failed to update', icon: 'error' });
        }
      })
      .catch(err => Swal.fire({ title: 'Error', text: err.message, icon: 'error' }));
    });
  }

  // ============================================================
  // ★ ADD: Qtag2 Card Form
  // ============================================================
  const addQtag2Form = document.getElementById('addQtag2Form');
  if (addQtag2Form) {
    addQtag2Form.addEventListener('submit', function (e) {
      e.preventDefault();
      const form = e.target;
      const errorEl = document.getElementById('qtag2-error-message');
      errorEl.style.display = 'none';

      if (!form.querySelector('[name="tag1_id"]').value) {
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

      Swal.fire({ title: 'Adding Qtag2 Card...', allowOutsideClick: false, didOpen: () => Swal.showLoading() });

      const formData = new FormData(form);
      fetch(`/subdash/${currentSubdashId}/add_qtag2`, {
        method: 'POST',
        body: formData
      })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          Swal.fire({ title: 'Success!', text: 'Qtag2 card added', icon: 'success', timer: 2000 }).then(() => location.reload());
        } else {
          Swal.fire({ title: 'Error', text: data.message || 'Failed to add Qtag2 card', icon: 'error' });
        }
      })
      .catch(err => {
        Swal.fire({ title: 'Error', text: err.message, icon: 'error' });
      });
    });
  }

  // ============================================================
  // ★ DELETE: Qtag2 Card
  // ============================================================
  document.addEventListener('click', function (e) {
    const btn = e.target.closest('.delete-qtag2-btn');
    if (!btn) return;
    e.preventDefault();
    const cardId = btn.dataset.cardId;

    Swal.fire({
      title: 'Delete Qtag2 Card?',
      text: 'This action cannot be undone.',
      icon: 'warning',
      showCancelButton: true,
      confirmButtonColor: '#dc3545',
      confirmButtonText: 'Yes, delete it!',
      cancelButtonText: 'Cancel'
    }).then(result => {
      if (!result.isConfirmed) return;
      Swal.fire({ title: 'Deleting...', allowOutsideClick: false, didOpen: () => Swal.showLoading() });

      fetch(`/subdash/${currentSubdashId}/delete_qtag2/${cardId}`, { method: 'DELETE' })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          Swal.fire({ title: 'Deleted!', icon: 'success', timer: 2000 }).then(() => location.reload());
        } else {
          Swal.fire({ title: 'Error', text: data.message || 'Failed to delete', icon: 'error' });
        }
      })
      .catch(err => Swal.fire({ title: 'Error', text: err.message, icon: 'error' }));
    });
  });

  // ============================================================
  // ★ RENAME: Qtag2 Card
  // ============================================================
  document.addEventListener('click', function (e) {
    const btn = e.target.closest('.rename-qtag2-btn');
    if (!btn) return;
    e.preventDefault();
    const cardId = btn.dataset.cardId;
    const target = btn.dataset.target || 'card';
    const card = btn.closest('.qtag2-card') || document.querySelector(`.qtag2-card[data-qtag2-id="${cardId}"]`);
    if (!card) return;

    let currentTitle;
    if (target === 'column') {
      const titleEl = card.querySelector('.qtag6-sub-title');
      currentTitle = titleEl ? titleEl.textContent.trim() : '';
    } else {
      const titleEl = card.querySelector('.fw-semibold');
      currentTitle = titleEl ? titleEl.textContent.trim() : 'Qtag2 Card';
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

      fetch(`/subdash/${currentSubdashId}/qtag2/${cardId}/rename`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ target: target, title: result.value.trim() })
      })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          Swal.fire({ icon: 'success', title: 'Thành công', timer: 1500, showConfirmButton: false }).then(() => location.reload());
        } else {
          Swal.fire({ icon: 'error', title: 'Lỗi', text: data.message || 'Không thể cập nhật' });
        }
      })
      .catch(err => Swal.fire({ icon: 'error', title: 'Lỗi', text: err.message }));
    });
  });

  // ============================================================
  // ★ EDIT: Qtag2 Card - Open modal and populate
  // ============================================================
  document.addEventListener('click', function (e) {
    const btn = e.target.closest('.edit-qtag2-btn');
    if (!btn) return;
    e.preventDefault();

    document.getElementById('editQtag2Id').value = btn.dataset.cardId;
    // PV tag
    document.getElementById('editQtag2Tag1').value = btn.dataset.tag1Id || '';

    // SV
    const svType = btn.dataset.svType || 'tag';
    document.getElementById('editQtag2SvType').value = svType;
    const fixedGrp = document.querySelector('.edit-qtag2-sv-fixed-group');
    const tagGrp = document.querySelector('.edit-qtag2-sv-tag-group');
    if (fixedGrp && tagGrp) {
      fixedGrp.style.display = svType === 'fixed' ? '' : 'none';
      tagGrp.style.display = svType === 'tag' ? '' : 'none';
    }
    if (svType === 'fixed') {
      document.getElementById('editQtag2SvFixed').value = btn.dataset.svFixed || '';
      document.getElementById('editQtag2SvFixedUnit').value = btn.dataset.svFixedUnit || '';
    } else {
      document.getElementById('editQtag2Tag2').value = btn.dataset.tag2Id || '';
    }

    const modal = new bootstrap.Modal(document.getElementById('editQtag2Modal'));
    modal.show();
  });

  // Edit Qtag2 form submission
  const editQtag2Form = document.getElementById('editQtag2Form');
  if (editQtag2Form) {
    editQtag2Form.addEventListener('submit', function (e) {
      e.preventDefault();
      const cardId = document.getElementById('editQtag2Id').value;
      const errorEl = document.getElementById('edit-qtag2-error-message');
      errorEl.style.display = 'none';

      if (!document.getElementById('editQtag2Tag1').value) {
        errorEl.textContent = 'Please select a PV tag';
        errorEl.style.display = 'block';
        return;
      }

      Swal.fire({ title: 'Updating Qtag2...', allowOutsideClick: false, didOpen: () => Swal.showLoading() });

      const formData = new FormData(editQtag2Form);
      fetch(`/subdash/${currentSubdashId}/update_qtag2/${cardId}`, {
        method: 'POST',
        body: formData
      })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          Swal.fire({ title: 'Success!', text: 'Qtag2 updated', icon: 'success', timer: 2000 }).then(() => location.reload());
        } else {
          Swal.fire({ title: 'Error', text: data.message || 'Failed to update', icon: 'error' });
        }
      })
      .catch(err => Swal.fire({ title: 'Error', text: err.message, icon: 'error' }));
    });
  }

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
    // Set PV tag selections
    for (let i = 1; i <= 2; i++) {
      const selectEl = document.getElementById(`editQtag6Tag${i}`);
      if (selectEl) selectEl.value = btn.dataset[`tag${i}Id`] || '';
    }

    // SV type/fixed/tag population helper
    const svFields = [
      { prefix: 'left_sv_high', typeEl: 'editQtag6LeftSvHighType', fixedEl: 'editQtag6LeftSvHighFixed', fixedUnitEl: 'editQtag6LeftSvHighFixedUnit', tagEl: 'editQtag6Tag3', dataType: 'leftSvHighType', dataFixed: 'leftSvHighFixed', dataFixedUnit: 'leftSvHighFixedUnit', dataTag: 'tag3Id' },
      { prefix: 'right_sv_high', typeEl: 'editQtag6RightSvHighType', fixedEl: 'editQtag6RightSvHighFixed', fixedUnitEl: 'editQtag6RightSvHighFixedUnit', tagEl: 'editQtag6Tag4', dataType: 'rightSvHighType', dataFixed: 'rightSvHighFixed', dataFixedUnit: 'rightSvHighFixedUnit', dataTag: 'tag4Id' },
      { prefix: 'left_sv_low', typeEl: 'editQtag6LeftSvLowType', fixedEl: 'editQtag6LeftSvLowFixed', fixedUnitEl: 'editQtag6LeftSvLowFixedUnit', tagEl: 'editQtag6Tag5', dataType: 'leftSvLowType', dataFixed: 'leftSvLowFixed', dataFixedUnit: 'leftSvLowFixedUnit', dataTag: 'tag5Id' },
      { prefix: 'right_sv_low', typeEl: 'editQtag6RightSvLowType', fixedEl: 'editQtag6RightSvLowFixed', fixedUnitEl: 'editQtag6RightSvLowFixedUnit', tagEl: 'editQtag6Tag6', dataType: 'rightSvLowType', dataFixed: 'rightSvLowFixed', dataFixedUnit: 'rightSvLowFixedUnit', dataTag: 'tag6Id' },
    ];
    svFields.forEach(f => {
      const svType = btn.dataset[f.dataType] || 'tag';
      document.getElementById(f.typeEl).value = svType;
      const target = `edit_qtag6_${f.prefix}`;
      const fixedGrp = document.querySelector(`.${target.replace(/_/g, '-')}-fixed-group`);
      const tagGrp = document.querySelector(`.${target.replace(/_/g, '-')}-tag-group`);
      if (fixedGrp && tagGrp) {
        fixedGrp.style.display = svType === 'fixed' ? '' : 'none';
        tagGrp.style.display = svType === 'tag' ? '' : 'none';
      }
      if (svType === 'fixed') {
        document.getElementById(f.fixedEl).value = btn.dataset[f.dataFixed] || '';
        const fixedUnitEl = document.getElementById(f.fixedUnitEl);
        if (fixedUnitEl) fixedUnitEl.value = btn.dataset[f.dataFixedUnit] || '';
      } else {
        document.getElementById(f.tagEl).value = btn.dataset[f.dataTag] || '';
      }
    });

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

      // Validate PV tags selected
      for (let i = 1; i <= 2; i++) {
        if (!document.getElementById(`editQtag6Tag${i}`).value) {
          errorEl.textContent = 'Please select both PV tags';
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
      document.getElementById('editSingle3SvHighFixedUnit').value = btn.dataset.svHighFixedUnit || '';
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
      document.getElementById('editSingle3SvLowFixedUnit').value = btn.dataset.svLowFixedUnit || '';
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
    document.getElementById('editPvDescription').value = btn.dataset.description || '';

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
  // ★ ADD: Qtag PV Dual Card
  // ============================================================
  const addPvDualForm = document.getElementById('addQtagPvDualForm');
  if (addPvDualForm) {
    addPvDualForm.addEventListener('submit', function (e) {
      e.preventDefault();
      const form = e.target;
      const errorEl = document.getElementById('pv-dual-error-message');
      errorEl.style.display = 'none';

      if (!form.querySelector('[name="left_tag_id"]').value || !form.querySelector('[name="right_tag_id"]').value) {
        errorEl.textContent = 'Please select both left and right PV tags';
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

      Swal.fire({ title: 'Adding PV Dual Card...', allowOutsideClick: false, didOpen: () => Swal.showLoading() });

      const formData = new FormData(form);
      fetch(`/subdash/${currentSubdashId}/add_qtag_pv_dual`, {
        method: 'POST',
        body: formData
      })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          Swal.fire({ title: 'Success!', text: 'PV Dual card added', icon: 'success', timer: 2000 }).then(() => location.reload());
        } else {
          Swal.fire({ title: 'Error', text: data.error || 'Failed to add PV Dual card', icon: 'error' });
        }
      })
      .catch(err => {
        Swal.fire({ title: 'Error', text: err.message, icon: 'error' });
      });
    });
  }

  // ============================================================
  // ★ DELETE: Qtag PV Dual Card
  // ============================================================
  document.addEventListener('click', function (e) {
    const btn = e.target.closest('.delete-qtag-pv-dual-btn');
    if (!btn) return;
    e.preventDefault();
    const cardId = btn.dataset.cardId;

    Swal.fire({
      title: 'Delete PV Dual Card?',
      text: 'This action cannot be undone.',
      icon: 'warning',
      showCancelButton: true,
      confirmButtonColor: '#dc3545',
      confirmButtonText: 'Yes, delete it!',
      cancelButtonText: 'Cancel'
    }).then(result => {
      if (!result.isConfirmed) return;
      Swal.fire({ title: 'Deleting...', allowOutsideClick: false, didOpen: () => Swal.showLoading() });

      fetch(`/subdash/${currentSubdashId}/delete_qtag_pv_dual/${cardId}`, { method: 'DELETE' })
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
  // ★ RENAME: Qtag PV Dual Card (card title, left title, right title)
  // ============================================================
  document.addEventListener('click', function (e) {
    const btn = e.target.closest('.rename-qtag-pv-dual-btn');
    if (!btn) return;
    e.preventDefault();
    const cardId = btn.dataset.cardId;
    const target = btn.dataset.target || 'card';
    const card = btn.closest('.qtag-pv-dual-card') || document.querySelector(`.qtag-pv-dual-card[data-qtag-pv-dual-id="${cardId}"]`);
    if (!card) return;

    let titleEl, currentTitle;
    if (target === 'left') {
      titleEl = card.querySelector('.qtag-pv-dual-sub-card[data-column="left"] .qtag-pv-dual-sub-title');
      currentTitle = titleEl ? titleEl.textContent.trim() : 'Left';
    } else if (target === 'right') {
      titleEl = card.querySelector('.qtag-pv-dual-sub-card[data-column="right"] .qtag-pv-dual-sub-title');
      currentTitle = titleEl ? titleEl.textContent.trim() : 'Right';
    } else {
      titleEl = card.querySelector('.fw-semibold');
      currentTitle = titleEl ? titleEl.textContent.trim() : 'PV Dual Card';
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

      fetch(`/subdash/${currentSubdashId}/qtag_pv_dual/${cardId}/rename`, {
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
  // ★ EDIT TAGS: Qtag PV Dual - Open modal and populate
  // ============================================================
  document.addEventListener('click', function (e) {
    const btn = e.target.closest('.edit-qtag-pv-dual-btn');
    if (!btn) return;
    e.preventDefault();

    document.getElementById('editPvDualId').value = btn.dataset.cardId;
    document.getElementById('editPvDualLeftTag').value = btn.dataset.leftTagId || '';
    document.getElementById('editPvDualRightTag').value = btn.dataset.rightTagId || '';

    // Populate left/right titles from card data attributes
    const card = btn.closest('.qtag-pv-dual-card');
    if (card) {
      document.getElementById('editPvDualLeftTitle').value = card.dataset.leftTitle || '';
      document.getElementById('editPvDualRightTitle').value = card.dataset.rightTitle || '';
      document.getElementById('editPvDualLeftDescription').value = card.dataset.leftDescription || '';
      document.getElementById('editPvDualRightDescription').value = card.dataset.rightDescription || '';
    }

    const modal = new bootstrap.Modal(document.getElementById('editQtagPvDualModal'));
    modal.show();
  });

  // Edit PV Dual form submission
  const editPvDualForm = document.getElementById('editQtagPvDualForm');
  if (editPvDualForm) {
    editPvDualForm.addEventListener('submit', function (e) {
      e.preventDefault();
      const cardId = document.getElementById('editPvDualId').value;
      const errorEl = document.getElementById('edit-pv-dual-error-message');
      errorEl.style.display = 'none';

      if (!document.getElementById('editPvDualLeftTag').value || !document.getElementById('editPvDualRightTag').value) {
        errorEl.textContent = 'Please select both left and right PV tags';
        errorEl.style.display = 'block';
        return;
      }

      Swal.fire({ title: 'Updating PV Dual...', allowOutsideClick: false, didOpen: () => Swal.showLoading() });

      const formData = new FormData(editPvDualForm);
      fetch(`/subdash/${currentSubdashId}/update_qtag_pv_dual/${cardId}`, {
        method: 'POST',
        body: formData
      })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          Swal.fire({ title: 'Success!', text: 'PV Dual card updated', icon: 'success', timer: 2000 }).then(() => location.reload());
        } else {
          Swal.fire({ title: 'Error', text: data.error || 'Failed to update', icon: 'error' });
        }
      })
      .catch(err => Swal.fire({ title: 'Error', text: err.message, icon: 'error' }));
    });
  }

  // ============================================================
  // ★ CHANGE CARD COLOR (all card types: quad, quad6, single3, pvonly, pvdual)
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

  // Apply independent sub-header color to a card element
  // Apply independent sub-header color (side: 'left' for single/left, 'right' for right column)
  function applySubColor(cardEl, color, side) {
    if (side === 'right') {
      if (color) {
        cardEl.style.setProperty('--qtag-sub-bg-right', color);
        cardEl.style.setProperty('--qtag-sub-text-right', getContrastColor(color));
        cardEl.classList.add('qtag-sub-right-custom');
      } else {
        cardEl.style.removeProperty('--qtag-sub-bg-right');
        cardEl.style.removeProperty('--qtag-sub-text-right');
        cardEl.classList.remove('qtag-sub-right-custom');
      }
      cardEl.dataset.subColorRight = color || '';
      const btn = cardEl.querySelector('.change-sub-color-btn[data-sub-side="right"]');
      if (btn) btn.dataset.currentColor = color || '';
    } else {
      // left / single column
      if (color) {
        cardEl.style.setProperty('--qtag-sub-bg', color);
        cardEl.style.setProperty('--qtag-sub-text', getContrastColor(color));
        cardEl.classList.add('qtag-sub-custom-bg');
      } else {
        cardEl.style.removeProperty('--qtag-sub-bg');
        cardEl.style.removeProperty('--qtag-sub-text');
        cardEl.classList.remove('qtag-sub-custom-bg');
      }
      cardEl.dataset.subColor = color || '';
      const btn = cardEl.querySelector('.change-sub-color-btn[data-sub-side="left"]');
      if (btn) btn.dataset.currentColor = color || '';
    }
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
          const cardEl = btn.closest('.quad-tag-card, .qtag6-card, .qtag4-card, .qtag3-card, .qtag2-card, .qtag-single-sub-card, .qtag-pv-dual-card');
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

  // Handler for changing independent sub-header color
  document.addEventListener('click', function (e) {
    const btn = e.target.closest('.change-sub-color-btn');
    if (!btn) return;
    e.preventDefault();

    const cardType = btn.dataset.cardType;
    const cardId = btn.dataset.cardId;
    const currentColor = btn.dataset.currentColor || '';
    const subSide = btn.dataset.subSide || 'left'; // 'left' or 'right'
    const colorField = subSide === 'right' ? 'sub_color_right' : 'sub_color';
    const dialogTitle = subSide === 'right' ? 'Change Right Sub-Header Color' : 'Change Left Sub-Header Color';

    // Color palette (same as card color palette)
    const colorPalette = [
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
      title: dialogTitle,
      width: 480,
      html: `
        <div class="text-start">
          <div class="mb-2 d-flex align-items-center gap-2">
            <button type="button" id="swal-reset-default" class="btn btn-sm btn-outline-secondary">
              <i class="bi bi-arrow-counterclockwise me-1"></i>Reset Default
            </button>
            <div class="d-flex align-items-center gap-2 ms-auto">
              <label style="font-size:12px;color:#aaa;">Custom:</label>
              <input type="color" id="swal-color-picker" value="${currentColor || '#1565c0'}"
                style="width:36px;height:30px;border:1px solid #555;border-radius:4px;cursor:pointer;padding:1px;">
            </div>
          </div>
          <div class="mb-3" id="swal-palette">${paletteHtml}</div>
          <div class="p-3 rounded" id="swal-preview"
            style="background:${currentColor || '#2a2a2a'};color:${currentColor ? getContrastColor(currentColor) : '#fff'};text-align:center;font-weight:500;">
            Sub-Header Preview
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
          cells.forEach(cell => {
            const match = cell.dataset.color.toLowerCase() === (color || '').toLowerCase();
            cell.style.border = match ? '2px solid #fff' : '2px solid transparent';
            cell.style.boxShadow = match ? '0 0 0 2px #6366f1' : 'none';
          });
          if (color) pickerInput.value = color;
        }

        cells.forEach(cell => {
          cell.addEventListener('click', () => selectColor(cell.dataset.color));
          cell.addEventListener('mouseenter', () => { cell.style.transform = 'scale(1.25)'; cell.style.zIndex = '2'; });
          cell.addEventListener('mouseleave', () => { cell.style.transform = 'scale(1)'; cell.style.zIndex = ''; });
        });

        pickerInput.addEventListener('input', () => selectColor(pickerInput.value));
        resetBtn.addEventListener('click', () => {
          selectColor('');
          pickerInput.value = '#1565c0';
        });
      },
      preConfirm: () => {
        return Swal.getPopup().querySelector('#swal-selected-color').value;
      }
    }).then(result => {
      if (!result.isConfirmed) return;
      const selectedColor = result.value;

      fetch(`/subdash/${currentSubdashId}/update_card_color`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ card_type: cardType, card_id: cardId, color: selectedColor, color_field: colorField })
      })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          const cardEl = btn.closest('.quad-tag-card, .qtag6-card, .qtag4-card, .qtag3-card, .qtag-pv-dual-card');
          if (cardEl) {
            applySubColor(cardEl, selectedColor, subSide);
          }
          Swal.fire({ icon: 'success', title: 'Sub-header color saved', timer: 1200, showConfirmButton: false });
        } else {
          Swal.fire({ icon: 'error', title: 'Error', text: data.message || 'Failed to save sub-header color' });
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
      // Use data-card-color attribute (most reliable); fallback to inline CSS var
      const bg = cardEl.dataset.cardColor ||
                 cardEl.style.getPropertyValue('--qtag-card-bg').trim() ||
                 getComputedStyle(cardEl).getPropertyValue('--qtag-card-bg').trim();
      if (bg) {
        cardEl.style.setProperty('--qtag-card-text', getContrastColor(bg));
      }
    });

    // Initialize --qtag-sub-text for cards that already have custom sub-header color from server
    document.querySelectorAll('.qtag-sub-custom-bg').forEach(cardEl => {
      const subBg = cardEl.dataset.subColor ||
                    cardEl.style.getPropertyValue('--qtag-sub-bg').trim();
      if (subBg) {
        cardEl.style.setProperty('--qtag-sub-text', getContrastColor(subBg));
      }
    });

    // Initialize --qtag-sub-text-right for cards with independent right sub-header color
    document.querySelectorAll('.qtag-sub-right-custom').forEach(cardEl => {
      const subBgRight = cardEl.dataset.subColorRight ||
                         cardEl.style.getPropertyValue('--qtag-sub-bg-right').trim();
      if (subBgRight) {
        cardEl.style.setProperty('--qtag-sub-text-right', getContrastColor(subBgRight));
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

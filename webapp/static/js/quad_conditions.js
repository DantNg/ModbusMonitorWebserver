/**
 * Quad Tag Conditions Manager
 * Quản lý điều kiện alarm cho Quad Tag Cards - sử dụng database thay vì localStorage
 */

// Load conditions from database via API
async function loadConditionsFromAPI(quadId) {
  try {
    const currentSubdashId = document.querySelector('[data-subdash-id]')?.dataset.subdashId || window.currentSubdashId;
    const response = await fetch(`/subdash/${currentSubdashId}/quad_condition/${quadId}`);
    const result = await response.json();
    
    if (!result.success) {
      console.error('Failed to load conditions:', result.message);
      setDefaultConditions();
      return;
    }
    
    const condition = result.condition;
    
    // If no condition exists, use defaults
    if (!condition) {
      setDefaultConditions();
      return;
    }
    
    // Populate form with condition data
    populateConditionForm(condition);
    
  } catch (error) {
    console.error('Error loading conditions:', error);
    setDefaultConditions();
  }
}

// Populate form with condition data from database
function populateConditionForm(condition) {
  // Global enabled state
  document.getElementById('conditionsEnabled').checked = condition.enabled !== false;
  
  // Left column - High threshold
  setValue('leftHighOperator', condition.left_high_operator, '>');
  setValue('leftHighCompareType', condition.left_high_compare_type, 'static');
  setValue('leftHighValue', condition.left_high_value, '');
  setValue('leftHighCompareTag', condition.left_high_compare_tag_id, '');
  setValue('leftHighOnStable', condition.left_high_on_stable, 10);
  setValue('leftHighOffStable', condition.left_high_off_stable, 30);
  toggleCompareType('leftHigh', condition.left_high_compare_type || 'static');
  
  // Left column - Low threshold
  setValue('leftLowOperator', condition.left_low_operator, '<');
  setValue('leftLowCompareType', condition.left_low_compare_type, 'static');
  setValue('leftLowValue', condition.left_low_value, '');
  setValue('leftLowCompareTag', condition.left_low_compare_tag_id, '');
  setValue('leftLowOnStable', condition.left_low_on_stable, 10);
  setValue('leftLowOffStable', condition.left_low_off_stable, 30);
  toggleCompareType('leftLow', condition.left_low_compare_type || 'static');
  
  // Left column - Notifications
  setValue('leftEmail', condition.left_email, '');
  setValue('leftSMS', condition.left_sms, '');
  setValue('leftDescription', condition.left_description, '');
  
  // Right column - High threshold
  setValue('rightHighOperator', condition.right_high_operator, '>');
  setValue('rightHighCompareType', condition.right_high_compare_type, 'static');
  setValue('rightHighValue', condition.right_high_value, '');
  setValue('rightHighCompareTag', condition.right_high_compare_tag_id, '');
  setValue('rightHighOnStable', condition.right_high_on_stable, 10);
  setValue('rightHighOffStable', condition.right_high_off_stable, 30);
  toggleCompareType('rightHigh', condition.right_high_compare_type || 'static');
  
  // Right column - Low threshold
  setValue('rightLowOperator', condition.right_low_operator, '<');
  setValue('rightLowCompareType', condition.right_low_compare_type, 'static');
  setValue('rightLowValue', condition.right_low_value, '');
  setValue('rightLowCompareTag', condition.right_low_compare_tag_id, '');
  setValue('rightLowOnStable', condition.right_low_on_stable, 10);
  setValue('rightLowOffStable', condition.right_low_off_stable, 30);
  toggleCompareType('rightLow', condition.right_low_compare_type || 'static');
  
  // Right column - Notifications
  setValue('rightEmail', condition.right_email, '');
  setValue('rightSMS', condition.right_sms, '');
  setValue('rightDescription', condition.right_description, '');
}

// Helper function to set value with default fallback
function setValue(elementId, value, defaultValue) {
  const element = document.getElementById(elementId);
  if (element) {
    element.value = value !== null && value !== undefined ? value : defaultValue;
  }
}

// Toggle compare type visibility (static value vs tag)
function toggleCompareType(target, compareType) {
  const valueContainer = document.getElementById(`${target}ValueContainer`);
  const tagContainer = document.getElementById(`${target}TagContainer`);
  
  if (valueContainer && tagContainer) {
    if (compareType === 'static') {
      valueContainer.style.display = 'block';
      tagContainer.style.display = 'none';
    } else {
      valueContainer.style.display = 'none';
      tagContainer.style.display = 'block';
    }
  }
}

// Set default values for all form fields
function setDefaultConditions() {
  document.getElementById('conditionsEnabled').checked = true;
  
  // Left column defaults
  setValue('leftHighOperator', '>', '>');
  setValue('leftHighCompareType', 'static', 'static');
  setValue('leftHighValue', '', '');
  setValue('leftHighCompareTag', '', '');
  setValue('leftHighOnStable', 10, 10);
  setValue('leftHighOffStable', 30, 30);
  toggleCompareType('leftHigh', 'static');
  
  setValue('leftLowOperator', '<', '<');
  setValue('leftLowCompareType', 'static', 'static');
  setValue('leftLowValue', '', '');
  setValue('leftLowCompareTag', '', '');
  setValue('leftLowOnStable', 10, 10);
  setValue('leftLowOffStable', 30, 30);
  toggleCompareType('leftLow', 'static');
  
  setValue('leftEmail', '', '');
  setValue('leftSMS', '', '');
  setValue('leftDescription', '', '');
  
  // Right column defaults
  setValue('rightHighOperator', '>', '>');
  setValue('rightHighCompareType', 'static', 'static');
  setValue('rightHighValue', '', '');
  setValue('rightHighCompareTag', '', '');
  setValue('rightHighOnStable', 10, 10);
  setValue('rightHighOffStable', 30, 30);
  toggleCompareType('rightHigh', 'static');
  
  setValue('rightLowOperator', '<', '<');
  setValue('rightLowCompareType', 'static', 'static');
  setValue('rightLowValue', '', '');
  setValue('rightLowCompareTag', '', '');
  setValue('rightLowOnStable', 10, 10);
  setValue('rightLowOffStable', 30, 30);
  toggleCompareType('rightLow', 'static');
  
  setValue('rightEmail', '', '');
  setValue('rightSMS', '', '');
  setValue('rightDescription', '', '');
}

// Save conditions to database via API
async function saveConditionsToAPI() {
  const quadId = document.getElementById('conditionQuadId').value;
  
  if (!quadId) {
    Swal.fire({
      icon: 'error',
      title: 'Lỗi',
      text: 'Không tìm thấy Quad Card ID'
    });
    return;
  }
  
  // Gather all form data
  const conditionData = {
    enabled: document.getElementById('conditionsEnabled').checked,
    
    // Left column
    left_high_operator: document.getElementById('leftHighOperator').value,
    left_high_compare_type: document.getElementById('leftHighCompareType').value,
    left_high_value: parseFloat(document.getElementById('leftHighValue').value) || null,
    left_high_compare_tag_id: parseInt(document.getElementById('leftHighCompareTag').value) || null,
    left_high_on_stable: parseInt(document.getElementById('leftHighOnStable').value) || 10,
    left_high_off_stable: parseInt(document.getElementById('leftHighOffStable').value) || 30,
    
    left_low_operator: document.getElementById('leftLowOperator').value,
    left_low_compare_type: document.getElementById('leftLowCompareType').value,
    left_low_value: parseFloat(document.getElementById('leftLowValue').value) || null,
    left_low_compare_tag_id: parseInt(document.getElementById('leftLowCompareTag').value) || null,
    left_low_on_stable: parseInt(document.getElementById('leftLowOnStable').value) || 10,
    left_low_off_stable: parseInt(document.getElementById('leftLowOffStable').value) || 30,
    
    left_email: document.getElementById('leftEmail').value.trim(),
    left_sms: document.getElementById('leftSMS').value.trim(),
    left_description: document.getElementById('leftDescription').value.trim(),
    
    // Right column
    right_high_operator: document.getElementById('rightHighOperator').value,
    right_high_compare_type: document.getElementById('rightHighCompareType').value,
    right_high_value: parseFloat(document.getElementById('rightHighValue').value) || null,
    right_high_compare_tag_id: parseInt(document.getElementById('rightHighCompareTag').value) || null,
    right_high_on_stable: parseInt(document.getElementById('rightHighOnStable').value) || 10,
    right_high_off_stable: parseInt(document.getElementById('rightHighOffStable').value) || 30,
    
    right_low_operator: document.getElementById('rightLowOperator').value,
    right_low_compare_type: document.getElementById('rightLowCompareType').value,
    right_low_value: parseFloat(document.getElementById('rightLowValue').value) || null,
    right_low_compare_tag_id: parseInt(document.getElementById('rightLowCompareTag').value) || null,
    right_low_on_stable: parseInt(document.getElementById('rightLowOnStable').value) || 10,
    right_low_off_stable: parseInt(document.getElementById('rightLowOffStable').value) || 30,
    
    right_email: document.getElementById('rightEmail').value.trim(),
    right_sms: document.getElementById('rightSMS').value.trim(),
    right_description: document.getElementById('rightDescription').value.trim(),
  };
  
  try {
    const currentSubdashId = document.querySelector('[data-subdash-id]')?.dataset.subdashId || window.currentSubdashId;
    const response = await fetch(`/subdash/${currentSubdashId}/quad_condition/${quadId}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(conditionData)
    });
    
    const result = await response.json();
    
    if (result.success) {
      // Close modal FIRST before showing Swal to avoid Bootstrap backdrop conflict
      const modalEl = document.getElementById('compareConditionsModal');
      const modal = bootstrap.Modal.getInstance(modalEl);

      // Mark that this quad has conditions for UI updates
      const quadCard = document.querySelector(`[data-quad-id="${quadId}"]`);
      if (quadCard && conditionData.enabled) {
        quadCard.setAttribute('data-has-conditions', 'true');
      }

      if (modal) {
        // Show success Swal only after modal is fully hidden (avoids backdrop conflict)
        modalEl.addEventListener('hidden.bs.modal', function onModalHidden() {
          modalEl.removeEventListener('hidden.bs.modal', onModalHidden);
          Swal.fire({
            icon: 'success',
            title: 'Thành công!',
            text: 'Điều kiện đã được lưu',
            timer: 2000,
            showConfirmButton: false
          });
        });
        modal.hide();
      } else {
        Swal.fire({
          icon: 'success',
          title: 'Thành công!',
          text: 'Điều kiện đã được lưu',
          timer: 2000,
          showConfirmButton: false
        });
      }
      
    } else {
      Swal.fire({
        icon: 'error',
        title: 'Lỗi',
        text: result.message || 'Không thể lưu điều kiện'
      });
    }
    
  } catch (error) {
    console.error('Error saving conditions:', error);
    Swal.fire({
      icon: 'error',
      title: 'Lỗi kết nối',
      text: 'Không thể kết nối đến server'
    });
  }
}

// Export functions to global scope
window.loadConditionsFromAPI = loadConditionsFromAPI;
window.saveConditionsToAPI = saveConditionsToAPI;
window.setDefaultConditions = setDefaultConditions;
window.toggleCompareType = toggleCompareType;

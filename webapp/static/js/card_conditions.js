/**
 * Card Alarm Conditions Manager
 * Manages alarm conditions for Qtag6, Single3, PV Only, PV Dual, Qtag2, Qtag3, Qtag4 cards
 * Uses unified card_alarm_conditions table via API
 */

// Card types that use dual columns (left + right)
const DUAL_COLUMN_TYPES = ['qtag6', 'pv_dual', 'qtag4'];

function cardCondSetValue(elementId, value, defaultValue) {
  const element = document.getElementById(elementId);
  if (element) {
    element.value = value !== null && value !== undefined ? value : defaultValue;
  }
}

function cardCondToggleCompareType(target, compareType) {
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

function cardCondSetDefaults() {
  document.getElementById('cardCondEnabled').checked = true;
  // Left defaults
  cardCondSetValue('cardLeftHighOperator', '>', '>');
  cardCondSetValue('cardLeftHighCompareType', 'static', 'static');
  cardCondSetValue('cardLeftHighValue', '', '');
  cardCondSetValue('cardLeftHighCompareTag', '', '');
  cardCondSetValue('cardLeftHighOnStable', 10, 10);
  cardCondSetValue('cardLeftHighOffStable', 30, 30);
  cardCondToggleCompareType('cardLeftHigh', 'static');
  cardCondSetValue('cardLeftLowOperator', '<', '<');
  cardCondSetValue('cardLeftLowCompareType', 'static', 'static');
  cardCondSetValue('cardLeftLowValue', '', '');
  cardCondSetValue('cardLeftLowCompareTag', '', '');
  cardCondSetValue('cardLeftLowOnStable', 10, 10);
  cardCondSetValue('cardLeftLowOffStable', 30, 30);
  cardCondToggleCompareType('cardLeftLow', 'static');
  cardCondSetValue('cardLeftEmail', '', '');
  cardCondSetValue('cardLeftSMS', '', '');
  cardCondSetValue('cardLeftDescription', '', '');
  // Right defaults
  cardCondSetValue('cardRightHighOperator', '>', '>');
  cardCondSetValue('cardRightHighCompareType', 'static', 'static');
  cardCondSetValue('cardRightHighValue', '', '');
  cardCondSetValue('cardRightHighCompareTag', '', '');
  cardCondSetValue('cardRightHighOnStable', 10, 10);
  cardCondSetValue('cardRightHighOffStable', 30, 30);
  cardCondToggleCompareType('cardRightHigh', 'static');
  cardCondSetValue('cardRightLowOperator', '<', '<');
  cardCondSetValue('cardRightLowCompareType', 'static', 'static');
  cardCondSetValue('cardRightLowValue', '', '');
  cardCondSetValue('cardRightLowCompareTag', '', '');
  cardCondSetValue('cardRightLowOnStable', 10, 10);
  cardCondSetValue('cardRightLowOffStable', 30, 30);
  cardCondToggleCompareType('cardRightLow', 'static');
  cardCondSetValue('cardRightEmail', '', '');
  cardCondSetValue('cardRightSMS', '', '');
  cardCondSetValue('cardRightDescription', '', '');
}

function cardCondPopulateForm(condition) {
  // SQLite stores Boolean as 0/1 (integer), use !! to coerce correctly
  document.getElementById('cardCondEnabled').checked = !!condition.enabled;

  // Left column
  cardCondSetValue('cardLeftHighOperator', condition.left_high_operator, '>');
  cardCondSetValue('cardLeftHighCompareType', condition.left_high_compare_type, 'static');
  cardCondSetValue('cardLeftHighValue', condition.left_high_value, '');
  cardCondSetValue('cardLeftHighCompareTag', condition.left_high_compare_tag_id, '');
  cardCondSetValue('cardLeftHighOnStable', condition.left_high_on_stable, 10);
  cardCondSetValue('cardLeftHighOffStable', condition.left_high_off_stable, 30);
  cardCondToggleCompareType('cardLeftHigh', condition.left_high_compare_type || 'static');

  cardCondSetValue('cardLeftLowOperator', condition.left_low_operator, '<');
  cardCondSetValue('cardLeftLowCompareType', condition.left_low_compare_type, 'static');
  cardCondSetValue('cardLeftLowValue', condition.left_low_value, '');
  cardCondSetValue('cardLeftLowCompareTag', condition.left_low_compare_tag_id, '');
  cardCondSetValue('cardLeftLowOnStable', condition.left_low_on_stable, 10);
  cardCondSetValue('cardLeftLowOffStable', condition.left_low_off_stable, 30);
  cardCondToggleCompareType('cardLeftLow', condition.left_low_compare_type || 'static');

  cardCondSetValue('cardLeftEmail', condition.left_email, '');
  cardCondSetValue('cardLeftSMS', condition.left_sms, '');
  cardCondSetValue('cardLeftDescription', condition.left_description, '');

  // Right column
  cardCondSetValue('cardRightHighOperator', condition.right_high_operator, '>');
  cardCondSetValue('cardRightHighCompareType', condition.right_high_compare_type, 'static');
  cardCondSetValue('cardRightHighValue', condition.right_high_value, '');
  cardCondSetValue('cardRightHighCompareTag', condition.right_high_compare_tag_id, '');
  cardCondSetValue('cardRightHighOnStable', condition.right_high_on_stable, 10);
  cardCondSetValue('cardRightHighOffStable', condition.right_high_off_stable, 30);
  cardCondToggleCompareType('cardRightHigh', condition.right_high_compare_type || 'static');

  cardCondSetValue('cardRightLowOperator', condition.right_low_operator, '<');
  cardCondSetValue('cardRightLowCompareType', condition.right_low_compare_type, 'static');
  cardCondSetValue('cardRightLowValue', condition.right_low_value, '');
  cardCondSetValue('cardRightLowCompareTag', condition.right_low_compare_tag_id, '');
  cardCondSetValue('cardRightLowOnStable', condition.right_low_on_stable, 10);
  cardCondSetValue('cardRightLowOffStable', condition.right_low_off_stable, 30);
  cardCondToggleCompareType('cardRightLow', condition.right_low_compare_type || 'static');

  cardCondSetValue('cardRightEmail', condition.right_email, '');
  cardCondSetValue('cardRightSMS', condition.right_sms, '');
  cardCondSetValue('cardRightDescription', condition.right_description, '');
}

async function loadCardAvailableTags() {
  const tagSelects = [
    document.getElementById('cardLeftHighCompareTag'),
    document.getElementById('cardLeftLowCompareTag'),
    document.getElementById('cardRightHighCompareTag'),
    document.getElementById('cardRightLowCompareTag')
  ];

  const subdashId = window.currentSubdashId
    || document.body.dataset.subdashId
    || document.querySelector('[data-subdash-id]')?.dataset.subdashId;

  if (!subdashId) {
    console.warn('[CardConditions] Cannot load tags: subdashId not found');
    return;
  }

  try {
    const res = await fetch(`/subdash/${subdashId}/api/tags_for_conditions`);
    const data = await res.json();

    if (!data.success || !Array.isArray(data.tags) || data.tags.length === 0) {
      console.warn('[CardConditions] No tags returned from API');
      return;
    }

    // Exclude PV tags that belong to the current card to prevent self-comparison.
    const cardType = document.getElementById('cardCondCardType')?.value || '';
    const cardId = document.getElementById('cardCondCardId')?.value || '';
    const excludedPvTagIds = getCardPvTagIds(cardType, cardId);

    // Giữ value đang chọn (nếu đang edit condition cũ)
    const savedValues = tagSelects.map(sel => sel ? sel.value : '');

    tagSelects.forEach((sel, idx) => {
      if (!sel) return;
      while (sel.options.length > 1) sel.remove(1);
      data.tags.forEach(tag => {
        if (excludedPvTagIds.has(String(tag.id))) return;
        const opt = document.createElement('option');
        opt.value = tag.id;
        opt.textContent = tag.name;
        sel.appendChild(opt);
      });
      // Khôi phục lại value đã chọn trước
      if (savedValues[idx]) sel.value = savedValues[idx];
    });
  } catch (err) {
    console.error('[CardConditions] Failed to load tags from API:', err);
  }
}

function getCardElementByType(cardType, cardId) {
  const map = {
    qtag6: `.qtag6-card[data-qtag6-id="${cardId}"]`,
    qtag4: `.qtag4-card[data-qtag4-id="${cardId}"]`,
    qtag3: `.qtag3-card[data-qtag3-id="${cardId}"]`,
    qtag2: `.qtag2-card[data-qtag2-id="${cardId}"]`,
    single3: `[data-qtag-single3-id="${cardId}"]`,
    pv_only: `[data-qtag-pv-id="${cardId}"]`,
    pv_dual: `.qtag-pv-dual-card[data-qtag-pv-dual-id="${cardId}"]`
  };

  const selector = map[cardType];
  return selector ? document.querySelector(selector) : null;
}

function getCardPvTagIds(cardType, cardId) {
  const excluded = new Set();
  if (!cardType || !cardId) return excluded;

  const cardEl = getCardElementByType(cardType, cardId);
  if (!cardEl) return excluded;

  cardEl.querySelectorAll('[data-role="pv"][data-tag-id]').forEach(el => {
    const id = el.getAttribute('data-tag-id');
    if (id) excluded.add(String(id));
  });

  // Some cards expose PV tag id on status indicator.
  cardEl.querySelectorAll('.quad-status-indicator[data-pv-tag-id]').forEach(el => {
    const id = el.getAttribute('data-pv-tag-id');
    if (id) excluded.add(String(id));
  });

  return excluded;
}

async function loadCardConditionsFromAPI(cardType, cardId) {
  try {
    const subdashId = document.querySelector('[data-subdash-id]')?.dataset.subdashId || window.currentSubdashId;
    const response = await fetch(`/subdash/${subdashId}/card_condition/${cardType}/${cardId}`);
    const result = await response.json();

    if (!result.success) {
      console.error('Failed to load card conditions:', result.message);
      cardCondSetDefaults();
      return;
    }

    const condition = result.condition;
    if (!condition) {
      cardCondSetDefaults();
      return;
    }

    cardCondPopulateForm(condition);
  } catch (error) {
    console.error('Error loading card conditions:', error);
    cardCondSetDefaults();
  }
}

async function saveCardConditionsToAPI() {
  const cardType = document.getElementById('cardCondCardType').value;
  const cardId = document.getElementById('cardCondCardId').value;

  if (!cardType || !cardId) {
    Swal.fire({ icon: 'error', title: 'Error', text: 'Card type or ID not found' });
    return;
  }

  const conditionData = {
    enabled: document.getElementById('cardCondEnabled').checked,
    // Left column
    left_high_operator: document.getElementById('cardLeftHighOperator').value,
    left_high_compare_type: document.getElementById('cardLeftHighCompareType').value,
    left_high_value: parseFloat(document.getElementById('cardLeftHighValue').value) || null,
    left_high_compare_tag_id: parseInt(document.getElementById('cardLeftHighCompareTag').value) || null,
    left_high_on_stable: parseInt(document.getElementById('cardLeftHighOnStable').value) || 10,
    left_high_off_stable: parseInt(document.getElementById('cardLeftHighOffStable').value) || 30,
    left_low_operator: document.getElementById('cardLeftLowOperator').value,
    left_low_compare_type: document.getElementById('cardLeftLowCompareType').value,
    left_low_value: parseFloat(document.getElementById('cardLeftLowValue').value) || null,
    left_low_compare_tag_id: parseInt(document.getElementById('cardLeftLowCompareTag').value) || null,
    left_low_on_stable: parseInt(document.getElementById('cardLeftLowOnStable').value) || 10,
    left_low_off_stable: parseInt(document.getElementById('cardLeftLowOffStable').value) || 30,
    left_email: document.getElementById('cardLeftEmail').value.trim(),
    left_sms: document.getElementById('cardLeftSMS').value.trim(),
    left_description: document.getElementById('cardLeftDescription').value.trim(),
    // Right column
    right_high_operator: document.getElementById('cardRightHighOperator').value,
    right_high_compare_type: document.getElementById('cardRightHighCompareType').value,
    right_high_value: parseFloat(document.getElementById('cardRightHighValue').value) || null,
    right_high_compare_tag_id: parseInt(document.getElementById('cardRightHighCompareTag').value) || null,
    right_high_on_stable: parseInt(document.getElementById('cardRightHighOnStable').value) || 10,
    right_high_off_stable: parseInt(document.getElementById('cardRightHighOffStable').value) || 30,
    right_low_operator: document.getElementById('cardRightLowOperator').value,
    right_low_compare_type: document.getElementById('cardRightLowCompareType').value,
    right_low_value: parseFloat(document.getElementById('cardRightLowValue').value) || null,
    right_low_compare_tag_id: parseInt(document.getElementById('cardRightLowCompareTag').value) || null,
    right_low_on_stable: parseInt(document.getElementById('cardRightLowOnStable').value) || 10,
    right_low_off_stable: parseInt(document.getElementById('cardRightLowOffStable').value) || 30,
    right_email: document.getElementById('cardRightEmail').value.trim(),
    right_sms: document.getElementById('cardRightSMS').value.trim(),
    right_description: document.getElementById('cardRightDescription').value.trim(),
  };

  try {
    const subdashId = document.querySelector('[data-subdash-id]')?.dataset.subdashId || window.currentSubdashId;
    const response = await fetch(`/subdash/${subdashId}/card_condition/${cardType}/${cardId}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(conditionData)
    });

    const result = await response.json();

    if (result.success) {
      Swal.fire({ icon: 'success', title: 'Success!', text: 'Alarm conditions saved', timer: 2000, showConfirmButton: false });
      const modal = bootstrap.Modal.getInstance(document.getElementById('cardConditionsModal'));
      if (modal) modal.hide();
      // Cập nhật toggle switch trên card (nếu có)
      if (typeof window._onCardConditionSaved === 'function') {
        window._onCardConditionSaved(cardType, cardId, !!conditionData.enabled);
      }
    } else {
      Swal.fire({ icon: 'error', title: 'Error', text: result.message || 'Could not save conditions' });
    }
  } catch (error) {
    console.error('Error saving card conditions:', error);
    Swal.fire({ icon: 'error', title: 'Connection Error', text: 'Could not connect to server' });
  }
}

// Expose functions globally
window.loadCardConditionsFromAPI = loadCardConditionsFromAPI;
window.saveCardConditionsToAPI = saveCardConditionsToAPI;
window.cardCondSetDefaults = cardCondSetDefaults;
window.cardCondToggleCompareType = cardCondToggleCompareType;
window.loadCardAvailableTags = loadCardAvailableTags;
window.DUAL_COLUMN_TYPES = DUAL_COLUMN_TYPES;

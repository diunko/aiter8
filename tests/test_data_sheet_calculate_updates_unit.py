import pytest
import pandas as pd
import numpy as np
from unittest.mock import MagicMock # Not usually needed for unit tests of calculate, but keep just in case

# Assuming DataSheet class and _UpdateContext are in src.iter8.data_sheet
from iter8.data_sheet import DataSheet, _UpdateContext

# Fixtures (like data_sheet_instance) are automatically discovered from tests/conftest.py

# =======================================
# Unit Tests for _calculate_updates
# =======================================

def test_calculate_updates_single_cell(data_sheet_instance):
    """
    Unit test for _calculate_updates focusing on a single cell change.
    """
    original_df = data_sheet_instance
    copy_df = original_df.copy()
    
    # Simulate the change made within the context manager
    change_idx = 1
    change_col = 'col_b'
    new_value = 'Updated B3'
    copy_df.loc[change_idx, change_col] = new_value

    # Expected payload for gspread batch_update
    # Index 1 -> Sheet Row 3, col_b -> Column B
    expected_payload = [
        {'range': 'B3', 'values': [[new_value]]}
    ]

    # Instantiate the context and manually set the dfs
    # _UpdateContext needs the original_df which holds the mock worksheet
    context = _UpdateContext(original_df)
    context.copy_df = copy_df # Set the modified copy

    # Calculate updates
    actual_payload = context._calculate_updates()

    # Assert the calculated payload is correct
    assert len(actual_payload) == 1
    assert actual_payload == expected_payload

def test_calculate_updates_multiple_cells(data_sheet_instance):
    """
    Unit test for _calculate_updates focusing on multiple cell changes.
    """
    original_df = data_sheet_instance
    copy_df = original_df.copy()
    
    # Simulate changes
    changes = [
        {'idx': 0, 'col': 'col_c', 'val': 99, 'range': 'C2'},
        {'idx': 2, 'col': 'col_d', 'val': 'New D3', 'range': 'D4'}
    ]
    copy_df.loc[changes[0]['idx'], changes[0]['col']] = changes[0]['val']
    copy_df.loc[changes[1]['idx'], changes[1]['col']] = changes[1]['val']

    # Expected payload
    expected_payload = [
        {'range': changes[0]['range'], 'values': [[changes[0]['val']]]},
        {'range': changes[1]['range'], 'values': [[changes[1]['val']]]},
    ]

    # Instantiate context and calculate updates
    context = _UpdateContext(original_df)
    context.copy_df = copy_df
    actual_payload = context._calculate_updates()

    # Assert the calculated payload is correct (order might vary, so compare sets or sort)
    # Sort based on range for consistent comparison
    sorted_actual = sorted(actual_payload, key=lambda x: x['range'])
    sorted_expected = sorted(expected_payload, key=lambda x: x['range'])
    assert sorted_actual == sorted_expected

def test_calculate_updates_nan_to_value(data_sheet_instance):
    """
    Unit test for _calculate_updates: Changing NaN to a value.
    """
    original_df = data_sheet_instance # original_df.loc[1, 'col_d'] is NaN
    copy_df = original_df.copy()
    
    change_idx = 1
    change_col = 'col_d'
    new_value = 'Now Has Value'
    copy_df.loc[change_idx, change_col] = new_value

    # Expect update payload with the new value (D3)
    expected_payload = [
        {'range': 'D3', 'values': [[new_value]]}
    ]

    context = _UpdateContext(original_df)
    context.copy_df = copy_df
    actual_payload = context._calculate_updates()

    assert actual_payload == expected_payload

def test_calculate_updates_value_to_empty(data_sheet_instance):
    """
    Unit test for _calculate_updates: Changing a value to empty string.
    """
    original_df = data_sheet_instance # original_df.loc[2, 'col_d'] is 'C3'
    copy_df = original_df.copy()
    
    change_idx = 2
    change_col = 'col_d'
    new_value = ''
    copy_df.loc[change_idx, change_col] = new_value

    # Expect update payload with empty string (D4)
    expected_payload = [
        {'range': 'D4', 'values': [[new_value]]}
    ]

    context = _UpdateContext(original_df)
    context.copy_df = copy_df
    actual_payload = context._calculate_updates()

    assert actual_payload == expected_payload

def test_calculate_updates_value_to_nan(data_sheet_instance):
    """
    Unit test for _calculate_updates: Changing a value to NaN.
    """
    original_df = data_sheet_instance # original_df.loc[0, 'col_c'] is 10
    copy_df = original_df.copy()
    
    change_idx = 0
    change_col = 'col_c'
    new_value = np.nan # Using numpy NaN
    copy_df.loc[change_idx, change_col] = new_value

    # Expect update payload with empty string (NaN is converted)
    # C2
    expected_payload = [
        {'range': 'C2', 'values': [['']]}
    ]

    context = _UpdateContext(original_df)
    context.copy_df = copy_df
    actual_payload = context._calculate_updates()

    assert actual_payload == expected_payload

# --- Unit Tests for New Fields ---

def test_calculate_updates_add_single_new_field(data_sheet_instance):
    """
    Unit test for _calculate_updates: Adding a single new column with one value.
    """
    original_df = data_sheet_instance
    copy_df = original_df.copy()
    
    new_col_name = 'new_col_E'
    change_idx = 0
    new_value = 'E1'
    
    # Simulate adding the column and value
    copy_df[new_col_name] = pd.NA # Initialize column
    copy_df.loc[change_idx, new_col_name] = new_value

    # Expected payload: Header for new column (E1) and the single value (E2)
    expected_payload = [
        {'range': 'E1', 'values': [[new_col_name]]}, # Header
        {'range': 'E2', 'values': [[new_value]]}      # Value at index 0
    ]

    context = _UpdateContext(original_df)
    context.copy_df = copy_df
    actual_payload = context._calculate_updates()

    # Sort by range for consistent comparison
    sorted_actual = sorted(actual_payload, key=lambda x: x['range'])
    sorted_expected = sorted(expected_payload, key=lambda x: x['range'])
    assert sorted_actual == sorted_expected

def test_calculate_updates_add_multiple_new_fields_and_modify_existing(data_sheet_instance):
    """
    Unit test for _calculate_updates: Adding multiple new columns and modifying an existing one.
    """
    original_df = data_sheet_instance
    copy_df = original_df.copy()
    
    new_fields = ['description', 'category', 'in_stock']
    
    # Simulate changes
    copy_df[new_fields[0]] = pd.NA # description
    copy_df[new_fields[1]] = pd.NA # category
    copy_df[new_fields[2]] = pd.NA # in_stock
    
    copy_df.loc[0, new_fields[0]] = 'First item description'
    copy_df.loc[1, new_fields[1]] = 'Electronics'
    copy_df.loc[2, new_fields[2]] = True
    copy_df.loc[0, 'col_c'] = 15.0 # Modify existing

    # Expected payload (columns sorted alphabetically: category, description, in_stock)
    # Existing: A=id, B=col_b, C=col_c, D=col_d
    # New: E=category, F=description, G=in_stock
    expected_payload = [
        # Headers
        {'range': 'E1', 'values': [['category']]},
        {'range': 'F1', 'values': [['description']]},
        {'range': 'G1', 'values': [['in_stock']]},
        # Values
        {'range': 'F2', 'values': [['First item description']]}, # idx 0, description
        {'range': 'E3', 'values': [['Electronics']]},           # idx 1, category
        {'range': 'G4', 'values': [[True]]},                    # idx 2, in_stock
        {'range': 'C2', 'values': [[15.0]]}                     # idx 0, col_c (modified existing)
    ]

    context = _UpdateContext(original_df)
    context.copy_df = copy_df
    actual_payload = context._calculate_updates()

    # Sort by range for consistent comparison
    sorted_actual = sorted(actual_payload, key=lambda x: x['range'])
    sorted_expected = sorted(expected_payload, key=lambda x: x['range'])
    assert sorted_actual == sorted_expected

def test_calculate_updates_explicitly_setting_nan_in_new_field(data_sheet_instance):
    """
    Unit test for _calculate_updates: Explicitly setting NaN in a new field.
    """
    original_df = data_sheet_instance
    copy_df = original_df.copy()
    
    new_field = 'notes'
    val_note = 'Important note'

    # Simulate changes
    copy_df[new_field] = pd.NA # Initialize
    copy_df.loc[0, new_field] = val_note
    copy_df.loc[1, new_field] = np.nan # Explicit NaN

    # Expected: Header (E1), Value for index 0 (E2). NaN at index 1 is skipped.
    expected_payload = [
        {'range': 'E1', 'values': [[new_field]]},
        {'range': 'E2', 'values': [[val_note]]},
    ]

    context = _UpdateContext(original_df)
    context.copy_df = copy_df
    actual_payload = context._calculate_updates()

    # Sort by range for consistent comparison
    sorted_actual = sorted(actual_payload, key=lambda x: x['range'])
    sorted_expected = sorted(expected_payload, key=lambda x: x['range'])
    assert sorted_actual == sorted_expected

def test_calculate_updates_add_field_with_dict_assignment(data_sheet_instance):
    """
    Unit test for _calculate_updates: Adding fields via dict assignment.
    """
    original_df = data_sheet_instance
    copy_df = original_df.copy()
    
    new_fields_dict = {
        'description': 'New description',
        'category': 'Household',
        'tags': 'discount,featured'
    }
    target_idx = 1

    # Simulate changes
    for field in new_fields_dict:
        copy_df[field] = pd.NA # Initialize columns
    copy_df.loc[target_idx, list(new_fields_dict.keys())] = list(new_fields_dict.values())

    # Expected: Headers (E1, F1, G1), Values for index 1 (E3, F3, G3)
    # Sorted alphabetically: category, description, tags
    expected_payload = [
        {'range': 'E1', 'values': [['category']]},
        {'range': 'F1', 'values': [['description']]},
        {'range': 'G1', 'values': [['tags']]},
        {'range': 'E3', 'values': [['Household']]},         # idx 1 -> row 3
        {'range': 'F3', 'values': [['New description']]},
        {'range': 'G3', 'values': [['discount,featured']]},
    ]

    context = _UpdateContext(original_df)
    context.copy_df = copy_df
    actual_payload = context._calculate_updates()

    # Sort by range for consistent comparison
    sorted_actual = sorted(actual_payload, key=lambda x: x['range'])
    sorted_expected = sorted(expected_payload, key=lambda x: x['range'])
    assert sorted_actual == sorted_expected

def test_calculate_updates_add_new_field_with_formula(data_sheet_instance):
    """
    Unit test for _calculate_updates: Adding a formula in a new field and changing an existing value.
    """
    original_df = data_sheet_instance
    copy_df = original_df.copy()
    
    new_field = 'image_formula'
    formula_value = '=IMAGE("http://example.com/img.jpg")'
    existing_col = 'col_c'
    existing_col_new_val = 12.50
    target_idx_formula = 0
    target_idx_existing = 1

    # Simulate changes
    copy_df[new_field] = pd.NA # Initialize
    copy_df.loc[target_idx_formula, new_field] = formula_value
    copy_df.loc[target_idx_existing, existing_col] = existing_col_new_val

    # Expected: Header (E1), Formula value (E2), Existing value (C3)
    expected_payload = [
        {'range': 'E1', 'values': [[new_field]]},
        {'range': 'E2', 'values': [[formula_value]]},
        {'range': 'C3', 'values': [[existing_col_new_val]]},
    ]

    context = _UpdateContext(original_df)
    context.copy_df = copy_df
    actual_payload = context._calculate_updates()

    # Sort by range for consistent comparison
    sorted_actual = sorted(actual_payload, key=lambda x: x['range'])
    sorted_expected = sorted(expected_payload, key=lambda x: x['range'])
    assert sorted_actual == sorted_expected

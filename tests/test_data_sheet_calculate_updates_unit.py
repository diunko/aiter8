import pytest
import pandas as pd
import numpy as np
from unittest.mock import MagicMock # Not usually needed for unit tests of calculate, but keep just in case

# Assuming DataSheet class and _UpdateContext are in src.iter8.data_sheet
from iter8.data_sheet import DataSheet, _UpdateContext

# Fixtures (like data_sheet_instance) are automatically discovered from tests/conftest.py

# --- Helper Function for Unit Tests ---
def _run_calc_updates(data_sheet_instance, modify_copy_func):
    """Runs the common setup, modification, and calculation for _calculate_updates tests."""
    original_df = data_sheet_instance
    copy_df = original_df.copy()
    context = _UpdateContext(original_df)
    context.copy_df = copy_df
    
    # Apply the test-specific modifications
    if modify_copy_func:
        modify_copy_func(copy_df)
        
    # Calculate and return the payload
    return context._calculate_updates()

# =======================================
# Unit Tests for _calculate_updates
# =======================================

def test_calculate_updates_single_cell(data_sheet_instance):
    """
    Unit test for _calculate_updates focusing on a single cell change.
    """
    # Define modification and expected result
    def modify(df):
        df.loc[1, 'col_b'] = 'Updated B3' # Simulate changing B3
        
    expected_payload = [
        {'range': 'B3', 'values': [['Updated B3']]}
    ]

    # Run calculation using helper
    actual_payload = _run_calc_updates(data_sheet_instance, modify)

    # Assert the calculated payload is correct
    # assert len(actual_payload) == 1 # Length check implied by direct comparison
    assert actual_payload == expected_payload

def test_calculate_updates_multiple_cells(data_sheet_instance):
    """
    Unit test for _calculate_updates focusing on multiple cell changes.
    """
    def modify(df):
        # Simulate changes: C2 (10 -> 99), D4 ('D4_val' -> 'New D4 val')
        df.loc[0, 'col_c'] = 99
        df.loc[2, 'col_d'] = 'New D4 val'

    expected_payload = [
        {'range': 'C2', 'values': [[99]]},
        {'range': 'D4', 'values': [['New D4 val']]},
    ]

    actual_payload = _run_calc_updates(data_sheet_instance, modify)

    # Sort based on range for consistent comparison
    sorted_actual = sorted(actual_payload, key=lambda x: x['range'])
    sorted_expected = sorted(expected_payload, key=lambda x: x['range'])
    assert sorted_actual == sorted_expected

def test_calculate_updates_nan_to_value(data_sheet_instance):
    """
    Unit test for _calculate_updates: Changing NaN to a value.
    """
    def modify(df):
        # Simulate changing D3 (NaN -> 'Now Has Value')
        df.loc[1, 'col_d'] = 'Now Has Value'
        
    expected_payload = [
        {'range': 'D3', 'values': [['Now Has Value']]}
    ]

    actual_payload = _run_calc_updates(data_sheet_instance, modify)
    assert actual_payload == expected_payload

def test_calculate_updates_value_to_empty(data_sheet_instance):
    """
    Unit test for _calculate_updates: Changing a value to empty string.
    """
    def modify(df):
        # Simulate changing D4 ('D4_val' -> '')
        df.loc[2, 'col_d'] = ''

    expected_payload = [
        {'range': 'D4', 'values': [['']]}
    ]

    actual_payload = _run_calc_updates(data_sheet_instance, modify)
    assert actual_payload == expected_payload

def test_calculate_updates_value_to_nan(data_sheet_instance):
    """
    Unit test for _calculate_updates: Changing a value to NaN.
    """
    def modify(df):
        # Simulate changing C2 (10 -> NaN)
        df.loc[0, 'col_c'] = np.nan

    expected_payload = [
        {'range': 'C2', 'values': [['']]}
    ]

    actual_payload = _run_calc_updates(data_sheet_instance, modify)
    assert actual_payload == expected_payload

# --- Unit Tests for New Fields ---

def test_calculate_updates_add_single_new_field(data_sheet_instance):
    """
    Unit test for _calculate_updates: Adding a single new column with one value.
    """
    def modify(df):
        # Simulate adding column 'new_col_E' and setting E2='E2_val'
        df['new_col_E'] = pd.NA 
        df.loc[0, 'new_col_E'] = 'E2_val'

    expected_payload = [
        {'range': 'E1', 'values': [['new_col_E']]},
        {'range': 'E2', 'values': [['E2_val']]}
    ]

    actual_payload = _run_calc_updates(data_sheet_instance, modify)

    # Sort by range for consistent comparison
    sorted_actual = sorted(actual_payload, key=lambda x: x['range'])
    sorted_expected = sorted(expected_payload, key=lambda x: x['range'])
    assert sorted_actual == sorted_expected

def test_calculate_updates_add_multiple_new_fields_and_modify_existing(data_sheet_instance):
    """
    Unit test for _calculate_updates: Adding multiple new columns and modifying an existing one.
    """
    def modify(df):
        # New columns (will become E, F, G - alphabetically sorted)
        df['description'] = pd.NA
        df['category'] = pd.NA
        df['in_stock'] = pd.NA 
        # Set values
        df.loc[0, 'description'] = 'Desc for F2' # F2
        df.loc[1, 'category'] = 'Cat for E3'     # E3
        df.loc[2, 'in_stock'] = True            # G4
        df.loc[0, 'col_c'] = 15.0                # Modify existing C2

    expected_payload = [
        # Headers
        {'range': 'E1', 'values': [['category']]},
        {'range': 'F1', 'values': [['description']]},
        {'range': 'G1', 'values': [['in_stock']]},
        # Values
        {'range': 'E3', 'values': [['Cat for E3']]},           # idx 1, category
        {'range': 'F2', 'values': [['Desc for F2']]},         # idx 0, description
        {'range': 'G4', 'values': [[True]]},                    # idx 2, in_stock
        {'range': 'C2', 'values': [[15.0]]}                     # idx 0, col_c (modified existing)
    ]

    actual_payload = _run_calc_updates(data_sheet_instance, modify)

    # Sort by range for consistent comparison
    sorted_actual = sorted(actual_payload, key=lambda x: x['range'])
    sorted_expected = sorted(expected_payload, key=lambda x: x['range'])
    assert sorted_actual == sorted_expected

def test_calculate_updates_explicitly_setting_nan_in_new_field(data_sheet_instance):
    """
    Unit test for _calculate_updates: Explicitly setting NaN in a new field.
    """
    def modify(df):
        # Simulate adding 'notes' column (becomes E) and setting E2='Note for E2', E3=NaN
        df['notes'] = pd.NA
        df.loc[0, 'notes'] = 'Note for E2'
        df.loc[1, 'notes'] = np.nan

    expected_payload = [
        {'range': 'E1', 'values': [['notes']]},
        {'range': 'E2', 'values': [['Note for E2']]},
    ]

    actual_payload = _run_calc_updates(data_sheet_instance, modify)

    # Sort by range for consistent comparison
    sorted_actual = sorted(actual_payload, key=lambda x: x['range'])
    sorted_expected = sorted(expected_payload, key=lambda x: x['range'])
    assert sorted_actual == sorted_expected

def test_calculate_updates_add_field_with_dict_assignment(data_sheet_instance):
    """
    Unit test for _calculate_updates: Adding fields via dict assignment.
    """
    def modify(df):
        new_fields_dict = {
            'description': 'Dict Desc for F3',
            'category': 'Dict Cat for E3',
            'tags': 'Dict Tags for G3'
        }
        # Simulate changes: Add columns, assign values to index 1 (Sheet Row 3)
        for field in new_fields_dict:
            df[field] = pd.NA 
        df.loc[1, list(new_fields_dict.keys())] = list(new_fields_dict.values())

    expected_payload = [
        {'range': 'E1', 'values': [['category']]},
        {'range': 'F1', 'values': [['description']]},
        {'range': 'G1', 'values': [['tags']]},
        {'range': 'E3', 'values': [['Dict Cat for E3']]}, 
        {'range': 'F3', 'values': [['Dict Desc for F3']]},
        {'range': 'G3', 'values': [['Dict Tags for G3']]},
    ]

    actual_payload = _run_calc_updates(data_sheet_instance, modify)

    # Sort by range for consistent comparison
    sorted_actual = sorted(actual_payload, key=lambda x: x['range'])
    sorted_expected = sorted(expected_payload, key=lambda x: x['range'])
    assert sorted_actual == sorted_expected

def test_calculate_updates_add_new_field_with_formula(data_sheet_instance):
    """
    Unit test for _calculate_updates: Adding a formula in a new field and changing an existing value.
    """
    def modify(df):
        # Simulate changes: Add 'image_formula' col (becomes E), set E2=formula, change C3 (idx 1) = 12.50
        df['image_formula'] = pd.NA
        df.loc[0, 'image_formula'] = '=IMAGE("formula_for_E2")'
        df.loc[1, 'col_c'] = 12.50

    expected_payload = [
        {'range': 'E1', 'values': [['image_formula']]},
        {'range': 'E2', 'values': [['=IMAGE("formula_for_E2")']]},
        {'range': 'C3', 'values': [[12.50]]},
    ]

    actual_payload = _run_calc_updates(data_sheet_instance, modify)

    # Sort by range for consistent comparison
    sorted_actual = sorted(actual_payload, key=lambda x: x['range'])
    sorted_expected = sorted(expected_payload, key=lambda x: x['range'])
    assert sorted_actual == sorted_expected

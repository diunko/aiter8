import pytest
import pandas as pd
import numpy as np
from unittest.mock import MagicMock, patch, call

from iter8.data_sheet import DataSheet, _UpdateContext
# Fixtures are now automatically discovered from tests/conftest.py

# ================================================
# Integration Tests for start_update (New Fields)
# ================================================

def test_add_single_new_field_to_one_row(data_sheet_instance, mock_worksheet):
    """
    Test adding a single new field to one specific row.
    Integration test: Verifies context manager updates sheet and DataFrame.
    """
    new_field = 'description'
    new_value = 'This is item at index 1' # Clarify value
    
    with data_sheet_instance.start_update() as change:
        # Add a new field to only row index 1 (Sheet Row 3)
        change.loc[1, new_field] = new_value 

    # Assertions
    # 1. Check sheet update attempt
    mock_worksheet.batch_update.assert_called_once()
    call_args, call_kwargs = mock_worksheet.batch_update.call_args
    # Check that header + value update were generated
    assert len(call_args[0]) == 2 
    assert call_kwargs.get('value_input_option') == 'USER_ENTERED'
    # (Payload content verified by unit test)
    
    # 2. Verify the original DataFrame
    assert new_field in data_sheet_instance.columns
    assert data_sheet_instance.loc[1, new_field] == new_value
    # Verify other rows have NaN/NA for the new field
    assert pd.isna(data_sheet_instance.loc[0, new_field]) 
    assert pd.isna(data_sheet_instance.loc[2, new_field]) 

def test_add_multiple_new_fields_to_different_rows(data_sheet_instance, mock_worksheet):
    """
    Test adding multiple new fields and modifying existing fields.
    Integration test: Verifies context manager updates sheet and DataFrame.
    """
    with data_sheet_instance.start_update() as change:
        # Add new fields (will become E, F, G)
        change.loc[0, 'description'] = 'Desc @ idx 0' # Sheet F2
        change.loc[1, 'category']    = 'Cat @ idx 1'  # Sheet E3
        change.loc[2, 'in_stock']    = True           # Sheet G4
        # Modify existing field
        change.loc[0, 'col_c']       = 15.0           # Sheet C2

    # Assertions
    # 1. Check sheet update attempt
    mock_worksheet.batch_update.assert_called_once()
    call_args, call_kwargs = mock_worksheet.batch_update.call_args
    # Check number of updates generated (3 headers + 4 values = 7)
    assert len(call_args[0]) == 7 
    assert call_kwargs.get('value_input_option') == 'USER_ENTERED'
    # (Payload content verified by unit test)

    # 2. Verify DataFrame state after update
    assert 'description' in data_sheet_instance.columns
    assert 'category' in data_sheet_instance.columns
    assert 'in_stock' in data_sheet_instance.columns
    
    assert data_sheet_instance.loc[0, 'description'] == 'Desc @ idx 0'
    assert data_sheet_instance.loc[1, 'category'] == 'Cat @ idx 1'
    assert data_sheet_instance.loc[2, 'in_stock'] == True
    assert data_sheet_instance.loc[0, 'col_c'] == 15.0
    
    # Check NaN/NA values in other cells of new columns
    assert pd.isna(data_sheet_instance.loc[1, 'description'])
    assert pd.isna(data_sheet_instance.loc[2, 'description'])
    assert pd.isna(data_sheet_instance.loc[0, 'category'])
    assert pd.isna(data_sheet_instance.loc[2, 'category'])
    assert pd.isna(data_sheet_instance.loc[0, 'in_stock'])
    assert pd.isna(data_sheet_instance.loc[1, 'in_stock'])

def test_explicitly_setting_nan_in_new_field(data_sheet_instance, mock_worksheet):
    """
    Test setting a NaN value explicitly in a new field.
    Integration test: Verifies update logic and DataFrame state.
    """
    new_field = 'notes'
    
    with data_sheet_instance.start_update() as change:
        # Add 'notes' column (becomes E), set E2='Note for idx 0', E3=NaN
        change.loc[0, new_field] = 'Note for idx 0'
        change.loc[1, new_field] = np.nan  # Explicitly set NaN
    
    # Assertions
    # 1. Check sheet update attempt
    mock_worksheet.batch_update.assert_called_once()
    call_args, call_kwargs = mock_worksheet.batch_update.call_args
    # Check expected number of updates (header + 1 value)
    assert len(call_args[0]) == 2 
    assert call_kwargs.get('value_input_option') == 'USER_ENTERED'
    # (Payload content verified by unit test)
    
    # 2. Verify DataFrame state
    assert new_field in data_sheet_instance.columns
    assert data_sheet_instance.loc[0, new_field] == 'Note for idx 0'
    assert pd.isna(data_sheet_instance.loc[1, new_field])
    assert pd.isna(data_sheet_instance.loc[2, new_field]) # Check the row not explicitly set

def test_add_field_with_dict_assignment(data_sheet_instance, mock_worksheet):
    """
    Test adding multiple new fields via dict assignment.
    Integration test: Verifies update logic and DataFrame state.
    """
    new_fields_dict = {
        'description': 'Dict Desc for idx 1',
        'category': 'Dict Cat for idx 1',
        'tags': 'Dict Tags for idx 1'
    }
    
    with data_sheet_instance.start_update() as change:
        # Assign multiple new fields at once to index 1 (Sheet Row 3)
        change.loc[1, list(new_fields_dict.keys())] = list(new_fields_dict.values())
    
    # Assertions
    # 1. Check sheet update attempt
    mock_worksheet.batch_update.assert_called_once()
    call_args, call_kwargs = mock_worksheet.batch_update.call_args
    # Check number of updates (3 headers + 3 values)
    assert len(call_args[0]) == 6
    assert call_kwargs.get('value_input_option') == 'USER_ENTERED'
    # (Payload content verified by unit test)
    
    # 2. Verify DataFrame state
    for field, value in new_fields_dict.items():
        assert field in data_sheet_instance.columns
        assert data_sheet_instance.loc[1, field] == value
        # Check other rows have NaN/NA
        assert pd.isna(data_sheet_instance.loc[0, field])
        assert pd.isna(data_sheet_instance.loc[2, field])

def test_add_new_field_with_formula(data_sheet_instance, mock_worksheet):
    """
    Test adding a new field with a formula and changing existing value.
    Integration test: Verifies update logic and DataFrame state.
    """
    new_field = 'image_formula'
    formula_value = '=IMAGE("formula_for_idx_0")'
    existing_col = 'col_c' 
    
    with data_sheet_instance.start_update() as change:
        # Add formula to new field at index 0 (Sheet Row 2)
        change.loc[0, new_field] = formula_value
        # Change existing col C at index 1 (Sheet Row 3)
        change.loc[1, existing_col] = 12.50
    
    # Assertions
    # 1. Check sheet update attempt
    mock_worksheet.batch_update.assert_called_once()
    call_args, call_kwargs = mock_worksheet.batch_update.call_args
    # Check number of updates (1 header + 1 formula val + 1 existing val)
    assert len(call_args[0]) == 3
    assert call_kwargs.get('value_input_option') == 'USER_ENTERED'
    # (Payload content verified by unit test)

    # 2. Verify DataFrame state
    assert new_field in data_sheet_instance.columns
    assert data_sheet_instance.loc[0, new_field] == formula_value
    assert data_sheet_instance.loc[1, existing_col] == 12.50
    # Check other values
    assert pd.isna(data_sheet_instance.loc[1, new_field]) # Formula col @ idx 1 should be NA
    # Check original value in col_c @ idx 0 (Sheet C2) is still 10
    assert data_sheet_instance.loc[0, existing_col] == 10 
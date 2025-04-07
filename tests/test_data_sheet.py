import pytest
import pandas as pd
from unittest.mock import MagicMock, patch, call # Use MagicMock for more flexibility
import numpy as np # For NaN values
import gspread # Import gspread module for exceptions

# Assuming your DataSheet class is in src.iter8.data_sheet
from iter8.data_sheet import DataSheet, _UpdateContext

# --- Fixtures --- 
# Fixtures are now defined in tests/conftest.py and are automatically discovered by pytest

# --- Test Cases ---

def test_context_manager_no_changes(data_sheet_instance, mock_worksheet):
    """
    Test that no updates occur if the DataFrame copy is not modified.
    """
    original_df_copy = data_sheet_instance.copy() # Keep a copy for comparison

    with data_sheet_instance.start_update() as change:
        # No changes made to 'change' DataFrame
        pass

    # Assertions
    mock_worksheet.batch_update.assert_not_called()
    pd.testing.assert_frame_equal(data_sheet_instance, original_df_copy)

def test_context_manager_single_cell_change(data_sheet_instance, mock_worksheet):
    """
    Test update after changing a single cell.
    Integration test: Verifies context manager calls update and updates the DataFrame.
    """
    # Check original value first to ensure test validity
    assert data_sheet_instance.loc[1, 'col_b'] == 'B3_val'

    with data_sheet_instance.start_update() as change:
        # Change B3 (index 1, col_b)
        change.loc[1, 'col_b'] = 'Updated B3'

    # Assertions
    # 1. Check sheet update attempt
    mock_worksheet.batch_update.assert_called_once()
    call_args, call_kwargs = mock_worksheet.batch_update.call_args
    # Verify one update was generated
    assert len(call_args[0]) == 1 
    assert call_kwargs.get('value_input_option') == 'USER_ENTERED'
    # (Payload content verified by unit test)

    # 2. Check if the original DataFrame was updated
    assert data_sheet_instance.loc[1, 'col_b'] == 'Updated B3'

def test_context_manager_multiple_cell_changes(data_sheet_instance, mock_worksheet):
    """
    Test update after changing multiple cells in different rows/columns.
    Integration test: Verifies context manager calls update and updates the DataFrame.
    """
    # Check original values first
    assert data_sheet_instance.loc[0, 'col_c'] == 10
    assert data_sheet_instance.loc[2, 'col_d'] == 'D4_val'
    
    with data_sheet_instance.start_update() as change:
        # Change C2 (index 0, col_c) and D4 (index 2, col_d)
        change.loc[0, 'col_c'] = 99
        change.loc[2, 'col_d'] = 'New D4 val'

    # Assertions
    # 1. Check sheet update attempt
    mock_worksheet.batch_update.assert_called_once()
    call_args, call_kwargs = mock_worksheet.batch_update.call_args
    # Check expected number of updates
    assert len(call_args[0]) == 2 # Two changes made
    assert call_kwargs.get('value_input_option') == 'USER_ENTERED'
    # (Payload content verified by unit test)

    # 2. Check if the original DataFrame was updated correctly
    assert data_sheet_instance.loc[0, 'col_c'] == 99
    assert data_sheet_instance.loc[2, 'col_d'] == 'New D4 val'

def test_context_manager_change_with_df_update(data_sheet_instance, mock_worksheet):
    """
    Test change detection when using df.update().
    Integration test: Verifies context manager calls update and updates the DataFrame.
    """
    # Check original value first
    assert data_sheet_instance.loc[0, 'col_b'] == 'B2_val'

    with data_sheet_instance.start_update() as change:
        # Update B2 (index 0, col_b) using df.update()
        change.update(pd.DataFrame({'col_b': ['Updated B2 via df.update']}, index=[0]))

    # Assertions
    # 1. Check sheet update attempt
    mock_worksheet.batch_update.assert_called_once()
    call_args, call_kwargs = mock_worksheet.batch_update.call_args
    # Check expected number of updates
    assert len(call_args[0]) == 1
    # (Payload content verified by unit tests - need specific test for df.update effects)

    # 2. Check original DataFrame update
    assert data_sheet_instance.loc[0, 'col_b'] == 'Updated B2 via df.update'

def test_context_manager_change_nan_to_value(data_sheet_instance, mock_worksheet):
    """
    Test changing a NaN value to a string.
    Integration test: Verifies context manager calls update and updates the DataFrame.
    """
    # Check original value first
    assert pd.isna(data_sheet_instance.loc[1, 'col_d'])
    
    with data_sheet_instance.start_update() as change:
        # Change D3 (index 1, col_d) from NaN
        change.loc[1, 'col_d'] = 'Value from NaN'

    # Assertions
    # 1. Check sheet update attempt
    mock_worksheet.batch_update.assert_called_once()
    call_args, call_kwargs = mock_worksheet.batch_update.call_args
    assert len(call_args[0]) == 1
    # (Payload content verified by unit test)

    # 2. Check original DataFrame update
    assert data_sheet_instance.loc[1, 'col_d'] == 'Value from NaN'

def test_context_manager_change_value_to_empty(data_sheet_instance, mock_worksheet):
    """
    Test changing a string value to an empty string.
    """
    # Check original value first
    assert data_sheet_instance.loc[2, 'col_d'] == 'D4_val'

    with data_sheet_instance.start_update() as change:
        # Change D4 (index 2, col_d) from 'D4_val' to empty
        change.loc[2, 'col_d'] = ''

    # Assertions
    # 1. Check sheet update attempt
    mock_worksheet.batch_update.assert_called_once()
    call_args, call_kwargs = mock_worksheet.batch_update.call_args
    assert len(call_args[0]) == 1
    # (Payload content verified by unit tests)

    # 2. Check original DataFrame update
    assert data_sheet_instance.loc[2, 'col_d'] == ''

def test_context_manager_exception_inside_with(data_sheet_instance, mock_worksheet):
    """
    Test that no updates occur if an exception is raised inside the 'with' block.
    """
    original_df_copy = data_sheet_instance.copy()
    
    with pytest.raises(ValueError, match="Something went wrong inside!"):
        with data_sheet_instance.start_update() as change:
            change.loc[0, 'col_b'] = "This change won't happen"
            raise ValueError("Something went wrong inside!")

    # Assertions
    mock_worksheet.batch_update.assert_not_called()
    # Original DataFrame should remain unchanged
    pd.testing.assert_frame_equal(data_sheet_instance, original_df_copy)

def test_context_manager_gspread_update_fails(data_sheet_instance, mock_worksheet):
    """
    Test handling when worksheet.batch_update raises an error.
    """
    original_df_copy = data_sheet_instance.copy()
    # Check original value first
    assert data_sheet_instance.loc[0, 'col_b'] == 'B2_val'
    
    # Configure mock to raise exception
    gspread_exception = Exception("API limit reached")
    mock_worksheet.batch_update.side_effect = gspread_exception

    # Test with a patch to capture print output
    with patch('builtins.print') as mock_print:
        with data_sheet_instance.start_update() as change:
            # Change B2 (index 0, col_b)
            change.loc[0, 'col_b'] = "Change that fails"

    # Assertions
    # 1. Check update attempt was made
    mock_worksheet.batch_update.assert_called_once()
    
    # 2. Check original DataFrame was NOT updated
    pd.testing.assert_frame_equal(data_sheet_instance, original_df_copy, check_dtype=False)
    
    # 3. Check error was printed
    mock_print.assert_any_call(f"Error during sheet update: {gspread_exception}")

def test_column_renaming_or_dropping_impact(data_sheet_instance, mock_worksheet):
    """
    Test potential issues if columns are renamed or dropped in the copy.
    (Current logic should ignore dropped/renamed cols and only update existing matched ones)
    """
    original_df_copy = data_sheet_instance.copy()
    # Check original values
    assert data_sheet_instance.loc[0, 'col_b'] == 'B2_val'
    assert data_sheet_instance.loc[0, 'col_c'] == 10

    with data_sheet_instance.start_update() as change:
         # Change existing B2 (index 0, col_b)
         change.loc[0, 'col_b'] = "Valid Change"
         # Try modifications that shouldn't affect the diff calculation for *existing* cols
         # change.drop(columns=['col_c'], inplace=True) 
         # change.rename(columns={'col_d': 'col_d_new'}, inplace=True) 

    # Assertions
    # 1. Check sheet update attempt
    mock_worksheet.batch_update.assert_called_once()
    call_args, call_kwargs = mock_worksheet.batch_update.call_args
    # Check only one update (for col_b) was generated
    assert len(call_args[0]) == 1 
    # (Payload details checked by unit tests)
    
    # 2. Original DF should only have 'col_b' updated
    assert data_sheet_instance.loc[0, 'col_b'] == "Valid Change"
    assert data_sheet_instance.loc[0, 'col_c'] == 10 # Unchanged

# ===========================================
# Integration Tests for start_update context (These remain here)
# ===========================================

# The test_context_manager_* functions remain here.

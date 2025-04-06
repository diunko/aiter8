import pytest
import pandas as pd
from unittest.mock import MagicMock, patch, call # Use MagicMock for more flexibility
import numpy as np # For NaN values
import gspread # Import gspread module for exceptions

# Assuming your DataSheet class is in src.iter8.data_sheet
from iter8.data_sheet import DataSheet, _UpdateContext

# --- Fixtures ---

@pytest.fixture
def mock_worksheet():
    """Provides a mock gspread worksheet object."""
    ws = MagicMock()
    # Simulate get_all_records returning a list of dicts
    ws.get_all_records.return_value = [
        {'id': 1, 'col_b': 'B1', 'col_c': 10, 'col_d': ''},
        {'id': 2, 'col_b': 'B2', 'col_c': 20, 'col_d': np.nan},
        {'id': 3, 'col_b': 'B3', 'col_c': 30, 'col_d': 'C3'},
    ]
    return ws

@pytest.fixture
@patch('iter8.data_sheet.gspread.oauth') # Mock gspread connection
@patch.object(DataSheet, 'gspread_client') # Ensure we use the mocked client
def data_sheet_instance(mock_gspread_client_cls, mock_oauth, mock_worksheet):
    """Provides a DataSheet instance initialized with mock data."""
    # Mock the client methods used in from_sheet
    mock_spreadsheet = MagicMock()
    mock_spreadsheet.worksheet.return_value = mock_worksheet
    mock_gspread_client_cls.open_by_key.return_value = mock_spreadsheet
    DataSheet.gspread_client = mock_gspread_client_cls # Assign the mocked client

    # Create instance using the class method which uses the mocked client
    ds = DataSheet.from_sheet(id='fake_id', sheet_id='fake_sheet')
    ds._worksheet = mock_worksheet # Ensure the instance holds the direct mock
    return ds

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
    Test update after changing a single cell using .loc.
    """
    row_index = 1 # Corresponds to id=2 in mock data, sheet row 3
    col_name = 'col_b'
    new_value = 'Updated B2'
    expected_range = 'B3' # col_b is 2nd col (B), sheet row 3 (after header)

    original_value = data_sheet_instance.loc[row_index, col_name]

    with data_sheet_instance.start_update() as change:
        change.loc[row_index, col_name] = new_value

    # Assertions
    mock_worksheet.batch_update.assert_called_once_with(
        [{'range': expected_range, 'values': [[new_value]]}]
    )
    # Check original DataFrame was updated
    assert data_sheet_instance.loc[row_index, col_name] == new_value
    # Check other values remain unchanged
    assert data_sheet_instance.loc[row_index, 'col_c'] == 20

def test_context_manager_multiple_cell_changes(data_sheet_instance, mock_worksheet):
    """
    Test update after changing multiple cells in different rows/columns.
    """
    changes = [
        {'idx': 0, 'col': 'col_c', 'val': 99, 'range': 'C2'}, # Row 0 -> Sheet Row 2, col_c is 3rd column (C)
        {'idx': 2, 'col': 'col_d', 'val': 'New D3', 'range': 'D4'}  # Row 2 -> Sheet Row 4, col_d is 4th column (D)
    ]
    expected_payload = [
        {'range': changes[0]['range'], 'values': [[changes[0]['val']]]},
        {'range': changes[1]['range'], 'values': [[changes[1]['val']]]},
    ]

    with data_sheet_instance.start_update() as change:
        change.loc[changes[0]['idx'], changes[0]['col']] = changes[0]['val']
        change.loc[changes[1]['idx'], changes[1]['col']] = changes[1]['val']

    # Assertions
    mock_worksheet.batch_update.assert_called_once()
    # Get the actual call arguments list
    actual_payload = mock_worksheet.batch_update.call_args[0][0]
    
    # Fix comparison method to handle lists
    # Sort the payloads by range for consistent comparison
    sorted_actual = sorted(actual_payload, key=lambda x: x['range'])
    sorted_expected = sorted(expected_payload, key=lambda x: x['range'])
    
    # Compare each item separately
    assert len(sorted_actual) == len(sorted_expected)
    for i in range(len(sorted_actual)):
        assert sorted_actual[i]['range'] == sorted_expected[i]['range']
        assert sorted_actual[i]['values'] == sorted_expected[i]['values']

    # Check original DataFrame updates
    assert data_sheet_instance.loc[changes[0]['idx'], changes[0]['col']] == changes[0]['val']
    assert data_sheet_instance.loc[changes[1]['idx'], changes[1]['col']] == changes[1]['val']

def test_context_manager_change_with_df_update(data_sheet_instance, mock_worksheet):
    """
    Test change detection when using df.update().
    """
    update_df = pd.DataFrame({'col_b': ['UPDATED B1']}, index=[0])
    expected_range = 'B2' # Row 0 -> Sheet Row 2, col_b is 2nd column (B)

    with data_sheet_instance.start_update() as change:
        change.update(update_df)

    # Assertions
    mock_worksheet.batch_update.assert_called_once_with(
         [{'range': expected_range, 'values': [['UPDATED B1']]}]
    )
    assert data_sheet_instance.loc[0, 'col_b'] == 'UPDATED B1'

def test_context_manager_change_nan_to_value(data_sheet_instance, mock_worksheet):
    """
    Test changing a NaN value to a string.
    """
    row_index = 1 # col_d is NaN here, Sheet Row 3
    col_name = 'col_d'
    new_value = 'Now Has Value'
    expected_range = 'D3' # col_d is 4th col (D)

    with data_sheet_instance.start_update() as change:
        change.loc[row_index, col_name] = new_value

    # Assertions
    mock_worksheet.batch_update.assert_called_once_with(
        [{'range': expected_range, 'values': [[new_value]]}]
    )
    assert data_sheet_instance.loc[row_index, col_name] == new_value

def test_context_manager_change_value_to_empty(data_sheet_instance, mock_worksheet):
    """
    Test changing a string value to an empty string.
    """
    row_index = 2 # col_d is 'C3', Sheet Row 4
    col_name = 'col_d'
    new_value = ''
    expected_range = 'D4' # col_d is 4th col (D)

    with data_sheet_instance.start_update() as change:
        change.loc[row_index, col_name] = new_value

    # Assertions
    # The code correctly handles NaN -> "" for gspread
    mock_worksheet.batch_update.assert_called_once_with(
        [{'range': expected_range, 'values': [[new_value]]}]
    )
    assert data_sheet_instance.loc[row_index, col_name] == new_value

def test_context_manager_exception_inside_with(data_sheet_instance, mock_worksheet):
    """
    Test that no updates occur if an exception is raised inside the 'with' block.
    """
    original_df_copy = data_sheet_instance.copy()
    custom_exception = ValueError("Something went wrong inside!")

    with pytest.raises(ValueError, match="Something went wrong inside!"):
        with data_sheet_instance.start_update() as change:
            change.loc[0, 'col_b'] = "This change won't happen"
            raise custom_exception

    # Assertions
    mock_worksheet.batch_update.assert_not_called()
    # Original DataFrame should remain unchanged
    pd.testing.assert_frame_equal(data_sheet_instance, original_df_copy)

def test_context_manager_gspread_update_fails(data_sheet_instance, mock_worksheet):
    """
    Test handling when worksheet.batch_update raises an error.
    """
    original_df_copy = data_sheet_instance.copy()
    
    # Create a simple exception instead of trying to use APIError directly
    gspread_exception = Exception("API limit reached")
    mock_worksheet.batch_update.side_effect = gspread_exception

    row_index = 0
    col_name = 'col_b'
    new_value = "Change that fails"

    # Test with a patch to capture print output
    with patch('builtins.print') as mock_print:
        with data_sheet_instance.start_update() as change:
            change.loc[row_index, col_name] = new_value

    # Assertions
    mock_worksheet.batch_update.assert_called_once() # It was called
    # Check that the original DataFrame was NOT updated because the API call failed
    pd.testing.assert_frame_equal(data_sheet_instance, original_df_copy, check_dtype=False)
    # Check if the error was printed
    mock_print.assert_any_call(f"Error during sheet update: {gspread_exception}")

def test_column_renaming_or_dropping_impact(data_sheet_instance, mock_worksheet):
    """
    Test potential issues if columns are renamed or dropped in the copy.
    (This might expose limitations based on the current comparison logic)
    """
    original_df_copy = data_sheet_instance.copy()

    with data_sheet_instance.start_update() as change:
         # This change should be detected
         change.loc[0, 'col_b'] = "Valid Change"
         # This modification makes comparison tricky if not handled carefully
         # change.drop(columns=['col_c'], inplace=True) # Dropping column
         # change.rename(columns={'col_d': 'col_d_new'}, inplace=True) # Renaming

    # Assertions (adapt based on how you expect/want it to handle this)
    # Current logic compares based on original columns. Dropped/Renamed cols in copy won't align.
    # It should likely only update 'col_b' and might print warnings for others.
    mock_worksheet.batch_update.assert_called_once_with(
        [{'range': 'B2', 'values': [['Valid Change']]}]
    )
    # Original DF should only have 'col_b' updated
    assert data_sheet_instance.loc[0, 'col_b'] == "Valid Change"
    assert data_sheet_instance.loc[0, 'col_c'] == 10 # Unchanged

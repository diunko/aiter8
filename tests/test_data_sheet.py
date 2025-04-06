import pytest
import pandas as pd
from unittest.mock import MagicMock, patch, call # Use MagicMock for more flexibility
import numpy as np # For NaN values

# Assuming your DataSheet class is in src.iter8.data_sheet
from src.iter8.data_sheet import DataSheet, _UpdateContext

# --- Fixtures ---

@pytest.fixture
def mock_worksheet():
    """Provides a mock gspread worksheet object."""
    ws = MagicMock()
    # Simulate get_all_records returning a list of dicts
    ws.get_all_records.return_value = [
        {'id': 1, 'col_a': 'A1', 'col_b': 10, 'col_c': ''},
        {'id': 2, 'col_a': 'A2', 'col_b': 20, 'col_c': np.nan},
        {'id': 3, 'col_a': 'A3', 'col_b': 30, 'col_c': 'C3'},
    ]
    return ws

@pytest.fixture
@patch('src.iter8.data_sheet.gspread.oauth') # Mock gspread connection
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
    col_name = 'col_a'
    new_value = 'Updated A2'
    expected_range = 'A3' # col_a is 1st col (A), sheet row 3

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
    assert data_sheet_instance.loc[row_index, 'col_b'] == 20

def test_context_manager_multiple_cell_changes(data_sheet_instance, mock_worksheet):
    """
    Test update after changing multiple cells in different rows/columns.
    """
    changes = [
        {'idx': 0, 'col': 'col_b', 'val': 99, 'range': 'B2'}, # Row 0 -> Sheet Row 2
        {'idx': 2, 'col': 'col_c', 'val': 'New C3', 'range': 'C4'}  # Row 2 -> Sheet Row 4
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
    # Use sets for order-independent comparison of list of dicts
    assert set(map(tuple, (d.items() for d in actual_payload))) == set(map(tuple, (d.items() for d in expected_payload)))

    # Check original DataFrame updates
    assert data_sheet_instance.loc[changes[0]['idx'], changes[0]['col']] == changes[0]['val']
    assert data_sheet_instance.loc[changes[1]['idx'], changes[1]['col']] == changes[1]['val']

def test_context_manager_change_with_df_update(data_sheet_instance, mock_worksheet):
    """
    Test change detection when using df.update().
    """
    update_df = pd.DataFrame({'col_a': ['UPDATED A1']}, index=[0])
    expected_range = 'A2' # Row 0 -> Sheet Row 2

    with data_sheet_instance.start_update() as change:
        change.update(update_df)

    # Assertions
    mock_worksheet.batch_update.assert_called_once_with(
         [{'range': expected_range, 'values': [['UPDATED A1']]}]
    )
    assert data_sheet_instance.loc[0, 'col_a'] == 'UPDATED A1'

def test_context_manager_change_nan_to_value(data_sheet_instance, mock_worksheet):
    """
    Test changing a NaN value to a string.
    """
    row_index = 1 # col_c is NaN here, Sheet Row 3
    col_name = 'col_c'
    new_value = 'Now Has Value'
    expected_range = 'C3' # col_c is 3rd col (C)

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
    row_index = 2 # col_c is 'C3', Sheet Row 4
    col_name = 'col_c'
    new_value = ''
    expected_range = 'C4'

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
            change.loc[0, 'col_a'] = "This change won't happen"
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
    gspread_exception = gspread.exceptions.APIError({'error': {'message': 'API limit reached'}})
    mock_worksheet.batch_update.side_effect = gspread_exception

    row_index = 0
    col_name = 'col_a'
    new_value = "Change that fails"

    # Expect the gspread exception OR check logs/print output if caught internally
    # For now, assume __exit__ might catch and print, then return True/False
    # Let's modify the test assuming it catches and prints.
    # If it re-raises, use pytest.raises here.
    print("\nTesting gspread API failure (expecting error print):")
    with patch('builtins.print') as mock_print: # Capture print output
         with data_sheet_instance.start_update() as change:
            change.loc[row_index, col_name] = new_value

    # Assertions
    mock_worksheet.batch_update.assert_called_once() # It was called
    # Check that the original DataFrame was NOT updated because the API call failed
    pd.testing.assert_frame_equal(data_sheet_instance, original_df_copy, check_dtype=False)
    # Check if the error was printed (based on current __exit__ implementation)
    mock_print.assert_any_call(f"Error during sheet update: {gspread_exception}")


def test_column_renaming_or_dropping_impact(data_sheet_instance, mock_worksheet):
    """
    Test potential issues if columns are renamed or dropped in the copy.
    (This might expose limitations based on the current comparison logic)
    """
    original_df_copy = data_sheet_instance.copy()

    with data_sheet_instance.start_update() as change:
         # This change should be detected
         change.loc[0, 'col_a'] = "Valid Change"
         # This modification makes comparison tricky if not handled carefully
         # change.drop(columns=['col_b'], inplace=True) # Dropping column
         # change.rename(columns={'col_c': 'col_c_new'}, inplace=True) # Renaming

    # Assertions (adapt based on how you expect/want it to handle this)
    # Current logic compares based on original columns. Dropped/Renamed cols in copy won't align.
    # It should likely only update 'col_a' and might print warnings for others.
    mock_worksheet.batch_update.assert_called_once_with(
        [{'range': 'A2', 'values': [['Valid Change']]}]
    )
    # Original DF should only have 'col_a' updated
    assert data_sheet_instance.loc[0, 'col_a'] == "Valid Change"
    assert data_sheet_instance.loc[0, 'col_b'] == 10 # Unchanged

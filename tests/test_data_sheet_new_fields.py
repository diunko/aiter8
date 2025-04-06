import pytest
import pandas as pd
import numpy as np
from unittest.mock import MagicMock, patch, call

from iter8.data_sheet import DataSheet, _UpdateContext

# --- Fixtures ---

@pytest.fixture
def mock_worksheet():
    """Provides a mock gspread worksheet object with initial data."""
    ws = MagicMock()
    # Simulate get_all_records returning a list of dicts with initial columns
    ws.get_all_records.return_value = [
        {'id': 1, 'name': 'Item 1', 'price': 10.0},
        {'id': 2, 'name': 'Item 2', 'price': 20.0},
        {'id': 3, 'name': 'Item 3', 'price': 30.0},
    ]
    return ws

@pytest.fixture
@patch('iter8.data_sheet.gspread.oauth')
@patch.object(DataSheet, 'gspread_client')
def data_sheet_instance(mock_gspread_client_cls, mock_oauth, mock_worksheet):
    """Provides a DataSheet instance initialized with mock data."""
    # Mock the client methods used in from_sheet
    mock_spreadsheet = MagicMock()
    mock_spreadsheet.worksheet.return_value = mock_worksheet
    mock_gspread_client_cls.open_by_key.return_value = mock_spreadsheet
    DataSheet.gspread_client = mock_gspread_client_cls

    # Create instance using the class method which uses the mocked client
    ds = DataSheet.from_sheet(id='fake_id', sheet_id='fake_sheet')
    ds._worksheet = mock_worksheet
    return ds

# --- Test Cases ---

def test_add_single_new_field_to_one_row(data_sheet_instance, mock_worksheet):
    """
    Test adding a single new field to one specific row.
    """
    original_cols = data_sheet_instance.columns.tolist()
    new_field = 'description'
    new_value = 'This is item 2'
    
    with data_sheet_instance.start_update() as change:
        # Add a new field to only one row
        change.loc[1, new_field] = new_value  # Row index 1 = id 2

    # Expected updates:
    # 1. Header update for the new column
    # 2. Cell update for the explicit new value
    expected_updates = [
        # Assuming columns are 0-indexed, and 'description' would be column D (4th column)
        {'range': 'D1', 'values': [[new_field]]},  # Header
        {'range': 'D3', 'values': [[new_value]]}   # Row index 1 -> Sheet row 3
    ]

    # Verify batch_update was called with the expected updates
    # Verify value_input_option is USER_ENTERED
    mock_worksheet.batch_update.assert_called_once()
    call_args, call_kwargs = mock_worksheet.batch_update.call_args
    actual_updates = call_args[0]
    assert call_kwargs.get('value_input_option') == 'USER_ENTERED'

    # Sort updates by range for comparison
    sorted_actual = sorted(actual_updates, key=lambda x: x['range'])
    sorted_expected = sorted(expected_updates, key=lambda x: x['range'])
    
    # Compare updates
    assert len(sorted_actual) == len(sorted_expected)
    for i in range(len(sorted_actual)):
        assert sorted_actual[i]['range'] == sorted_expected[i]['range']
        assert sorted_actual[i]['values'] == sorted_expected[i]['values']
    
    # Verify the original DataFrame has been updated with the new column
    assert new_field in data_sheet_instance.columns
    assert data_sheet_instance.loc[1, new_field] == new_value
    # Verify other rows have NaN for the new field
    assert pd.isna(data_sheet_instance.loc[0, new_field])
    assert pd.isna(data_sheet_instance.loc[2, new_field])

def test_add_multiple_new_fields_to_different_rows(data_sheet_instance, mock_worksheet):
    """
    Test adding multiple new fields to different rows.
    """
    original_cols = data_sheet_instance.columns.tolist()
    new_fields = ['description', 'category', 'in_stock']
    
    with data_sheet_instance.start_update() as change:
        # Add different new fields to different rows
        change.loc[0, new_fields[0]] = 'First item description'
        change.loc[1, new_fields[1]] = 'Electronics'
        change.loc[2, new_fields[2]] = True
        # Also set a value for an existing field to test mixed updates
        change.loc[0, 'price'] = 15.0

    # Our implementation sorts new columns alphabetically, so adjust expected order:
    # Alphabetical order: category, description, in_stock
    expected_updates = [
        # Headers for new columns (D, E, F assuming 0-indexed columns)
        {'range': 'D1', 'values': [['category']]},  # First alphabetically
        {'range': 'E1', 'values': [['description']]},  # Second alphabetically
        {'range': 'F1', 'values': [['in_stock']]},  # Third alphabetically
        # Data cells - need to adjust cell ranges based on revised column order
        {'range': 'E2', 'values': [['First item description']]},  # Row 0, description (now column E)
        {'range': 'D3', 'values': [['Electronics']]},            # Row 1, category (now column D)
        {'range': 'F4', 'values': [[True]]},                     # Row 2, in_stock (still column F)
        {'range': 'C2', 'values': [[15.0]]},                     # Row 0, price (existing column C)
    ]

    # Verify batch_update was called
    mock_worksheet.batch_update.assert_called_once()
    call_args, call_kwargs = mock_worksheet.batch_update.call_args
    actual_updates = call_args[0]
    assert call_kwargs.get('value_input_option') == 'USER_ENTERED'

    # Sort updates by range for comparison
    sorted_actual = sorted(actual_updates, key=lambda x: x['range'])
    sorted_expected = sorted(expected_updates, key=lambda x: x['range'])
    
    # Check count and content of updates
    assert len(sorted_actual) == len(sorted_expected)
    for i in range(len(sorted_actual)):
        assert sorted_actual[i]['range'] == sorted_expected[i]['range']
        assert sorted_actual[i]['values'] == sorted_expected[i]['values']
    
    # Verify DataFrame state after update
    for field in new_fields:
        assert field in data_sheet_instance.columns
    
    assert data_sheet_instance.loc[0, new_fields[0]] == 'First item description'
    assert data_sheet_instance.loc[1, new_fields[1]] == 'Electronics'
    assert data_sheet_instance.loc[2, new_fields[2]] == True
    assert data_sheet_instance.loc[0, 'price'] == 15.0
    
    # Check NaN values
    assert pd.isna(data_sheet_instance.loc[1, new_fields[0]])
    assert pd.isna(data_sheet_instance.loc[0, new_fields[1]])
    assert pd.isna(data_sheet_instance.loc[1, new_fields[2]])

def test_explicitly_setting_nan_in_new_field(data_sheet_instance, mock_worksheet):
    """
    Test setting a NaN value explicitly in a new field.
    This should not generate an update for that cell.
    """
    new_field = 'notes'
    
    with data_sheet_instance.start_update() as change:
        # Add a new field to two rows, with one explicit value and one NaN
        change.loc[0, new_field] = 'Important note'
        change.loc[1, new_field] = np.nan  # Explicitly set NaN
    
    # Expected updates - only the header and the non-NaN value
    expected_updates = [
        {'range': 'D1', 'values': [[new_field]]},           # Header
        {'range': 'D2', 'values': [['Important note']]},    # Row 0 value
        # No update for row 1 since we set np.nan
    ]
    
    # Verify batch_update was called
    mock_worksheet.batch_update.assert_called_once()
    call_args, call_kwargs = mock_worksheet.batch_update.call_args
    actual_updates = call_args[0]
    assert call_kwargs.get('value_input_option') == 'USER_ENTERED'

    # Sort updates by range for comparison
    sorted_actual = sorted(actual_updates, key=lambda x: x['range'])
    sorted_expected = sorted(expected_updates, key=lambda x: x['range'])
    
    # Check count and content of updates
    assert len(sorted_actual) == len(sorted_expected)
    for i in range(len(sorted_actual)):
        assert sorted_actual[i]['range'] == sorted_expected[i]['range']
        assert sorted_actual[i]['values'] == sorted_expected[i]['values']
    
    # Verify DataFrame state
    assert new_field in data_sheet_instance.columns
    assert data_sheet_instance.loc[0, new_field] == 'Important note'
    assert pd.isna(data_sheet_instance.loc[1, new_field])
    assert pd.isna(data_sheet_instance.loc[2, new_field])

def test_add_field_with_dict_assignment(data_sheet_instance, mock_worksheet):
    """
    Test adding multiple new fields in a single assignment using a dictionary.
    This is useful for bulk assignment of multiple fields to the same row.
    """
    new_fields = {
        'description': 'New description',
        'category': 'Household',
        'tags': 'discount,featured'
    }
    
    with data_sheet_instance.start_update() as change:
        # Assign multiple new fields at once using dictionary key assignment
        change.loc[1, new_fields.keys()] = list(new_fields.values())
    
    # Expected updates - alphabetical order: category, description, tags
    expected_updates = [
        # Headers (columns D, E, F)
        {'range': 'D1', 'values': [['category']]},
        {'range': 'E1', 'values': [['description']]},
        {'range': 'F1', 'values': [['tags']]},
        # Row values (row 3 = index 1)
        {'range': 'D3', 'values': [['Household']]},
        {'range': 'E3', 'values': [['New description']]},
        {'range': 'F3', 'values': [['discount,featured']]},
    ]
    
    # Verify batch_update was called
    mock_worksheet.batch_update.assert_called_once()
    call_args, call_kwargs = mock_worksheet.batch_update.call_args
    actual_updates = call_args[0]
    assert call_kwargs.get('value_input_option') == 'USER_ENTERED'

    # Sort updates by range for comparison
    sorted_actual = sorted(actual_updates, key=lambda x: x['range'])
    sorted_expected = sorted(expected_updates, key=lambda x: x['range'])
    
    # Check count and content of updates
    assert len(sorted_actual) == len(sorted_expected)
    for i in range(len(sorted_actual)):
        assert sorted_actual[i]['range'] == sorted_expected[i]['range']
        assert sorted_actual[i]['values'] == sorted_expected[i]['values']
    
    # Verify DataFrame state
    for field, value in new_fields.items():
        assert field in data_sheet_instance.columns
        assert data_sheet_instance.loc[1, field] == value
        # Check other rows have NaN
        assert pd.isna(data_sheet_instance.loc[0, field])
        assert pd.isna(data_sheet_instance.loc[2, field])

def test_add_new_field_with_formula(data_sheet_instance, mock_worksheet):
    """
    Test adding a new field that contains a Google Sheets formula.
    Ensures USER_ENTERED option is used.
    """
    new_field = 'image_formula'
    formula_value = '=IMAGE("http://example.com/img.jpg")'
    existing_col_change_field = 'price'
    existing_col_change_value = 12.50

    with data_sheet_instance.start_update() as change:
        # Add a new field with a formula
        change.loc[0, new_field] = formula_value
        # Also change an existing value
        change.loc[1, existing_col_change_field] = existing_col_change_value
    
    # Expected updates
    expected_updates = [
        # Header for new column (D)
        {'range': 'D1', 'values': [[new_field]]},
        # Data cell for the formula (Row 2)
        {'range': 'D2', 'values': [[formula_value]]},
        # Data cell for the existing column change (Row 3)
        {'range': 'C3', 'values': [[existing_col_change_value]]},
    ]

    # Verify batch_update was called with USER_ENTERED
    mock_worksheet.batch_update.assert_called_once()
    call_args, call_kwargs = mock_worksheet.batch_update.call_args
    actual_updates = call_args[0]
    assert call_kwargs.get('value_input_option') == 'USER_ENTERED'

    # Sort and compare updates
    sorted_actual = sorted(actual_updates, key=lambda x: x['range'])
    sorted_expected = sorted(expected_updates, key=lambda x: x['range'])

    assert len(sorted_actual) == len(sorted_expected)
    for i in range(len(sorted_actual)):
        assert sorted_actual[i]['range'] == sorted_expected[i]['range']
        assert sorted_actual[i]['values'] == sorted_expected[i]['values']

    # Verify DataFrame state
    assert new_field in data_sheet_instance.columns
    assert data_sheet_instance.loc[0, new_field] == formula_value
    assert data_sheet_instance.loc[1, existing_col_change_field] == existing_col_change_value
    assert pd.isna(data_sheet_instance.loc[1, new_field]) 
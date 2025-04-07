import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
import numpy as np
import gspread

# Assuming DataSheet class is in src.iter8.data_sheet
from iter8.data_sheet import DataSheet, _UpdateContext

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
    # Add a name attribute for easier debugging if needed
    ws.name = "MockWorksheet"
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

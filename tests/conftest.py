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
    # Values are named to reflect their expected Sheet cell location for clarity
    ws.get_all_records.return_value = [
        # DataFrame Index 0 -> Sheet Row 2
        {'id': 1, 'col_b': 'B2_val', 'col_c': 10,       'col_d': ''},       # A2, B2, C2, D2
        # DataFrame Index 1 -> Sheet Row 3
        {'id': 2, 'col_b': 'B3_val', 'col_c': 20,       'col_d': np.nan},   # A3, B3, C3, D3
        # DataFrame Index 2 -> Sheet Row 4
        {'id': 3, 'col_b': 'B4_val', 'col_c': 30,       'col_d': 'D4_val'}, # A4, B4, C4, D4
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

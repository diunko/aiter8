# iter8

A Python library for interactively updating Google Sheets, facilitating collaborative workflows involving LLMs and human edits.

## Installation

```bash
pip install iter8
```

## Basic Usage

```python
from iter8 import DataSheet
import pandas as pd

# Assuming you have gspread credentials set up

# Load data from a sheet
sheet_id = 'YOUR_SPREADSHEET_ID'
worksheet_name = 'Sheet1' # Or the specific worksheet name/index

try:
    ds = DataSheet.from_sheet(id=sheet_id, sheet_id=worksheet_name)
    print("Initial DataFrame:")
    print(ds.head())

    # Start an update context
    with ds.start_update() as changes:
        # Simulate LLM generating new data for row index 1
        llm_output = {'col_a': 'New value from LLM', 'col_b': 123}
        update_df = pd.DataFrame([llm_output], index=[1])
        changes.update(update_df)

        # Simulate another change
        changes.loc[0, 'col_c'] = "Manual Edit Example"

        print("\nDataFrame within context (changes applied to copy):")
        print(changes.head())
        # modifications made to 'changes' will be diffed and applied
        # to the Google Sheet and the original 'ds' DataFrame upon exiting

    print("\nFinal DataFrame (after update):")
    print(ds.head())
    print("\nCheck your Google Sheet for updates!")

except Exception as e:
    print(f"An error occurred: {e}")
```

## Features

* Load Google Sheet data into a pandas DataFrame.
* Use a context manager (`start_update`) to track changes made to the DataFrame.
* Automatically diff changes and apply only the modified cells back to the Google Sheet using `batch_update`.
* Preserves other cell data not modified within the context.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details. 
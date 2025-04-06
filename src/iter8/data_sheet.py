import pandas as pd
import os
import gspread

class DfSheet:
    def __init__(self, sheet):
        self.sheet = sheet
        self.df = pd.DataFrame(self.sheet.get_all_records())


def update_sheet(df, sheet):
    pass
    # take current sheet values
    # compare with df
    # only update cells that are different


class DataSheet(pd.DataFrame):
    CREDS_PATH = f"{os.environ['HOME']}/kk/hahaunt/"
    gspread_client = gspread.oauth(
        credentials_filename=f"{CREDS_PATH}/hahaunt-content-pipeline-client.json",
        authorized_user_filename=f"{CREDS_PATH}/authorized_user.json",
    )

    @classmethod
    def from_sheet(cls, id, sheet_id):
        spreadsheet = cls.gspread_client.open_by_key(id)
        worksheet = spreadsheet.worksheet(sheet_id)
        
        records = worksheet.get_all_records()
        df = cls(records)
        
        df._worksheet = worksheet

        return df
    
    def start_update(self):
        """Context manager for updating the sheet with changes.
        
        Usage:
            with datasheet.start_update() as change:
                change.loc[1, 'column'] = 'new value'
                # Make other changes to the copy...
        
        Upon exiting the context, changes are applied to both
        the original DataFrame and the Google Sheet.
        """
        return _UpdateContext(self)
    
class _UpdateContext:
    def __init__(self, original_df):
        self.original_df = original_df
        self.worksheet = original_df._worksheet
        
    def __enter__(self):
        # Create a copy for modifications
        self.copy_df = self.original_df.copy()
        return self.copy_df
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            # An exception occurred, don't apply updates
            print(f"Exception occurred: {exc_val}. No updates applied.")
            return False # Indicate exception was not handled here

        # Prepare batch updates list
        updates = []

        # 1. Identify new columns (columns in copy_df but not in original_df)
        original_columns = set(self.original_df.columns)
        copy_columns = set(self.copy_df.columns)
        new_columns = copy_columns - original_columns
        existing_columns = original_columns.intersection(copy_columns)

        # 2. Add header updates for new columns
        if new_columns:
            # Calculate column positions for new columns
            existing_col_count = len(original_columns)
            for i, col_name in enumerate(sorted(new_columns)):
                col_idx = existing_col_count + i
                col_letter = gspread.utils.rowcol_to_a1(1, col_idx + 1)[:-1]  # Get letter part of A1 notation
                
                # Add header update
                updates.append({
                    "range": f"{col_letter}1",
                    "values": [[col_name]]
                })
                
                # 3. Add updates for explicitly set values in new columns (ignore NaN)
                for idx in self.copy_df.index:
                    cell_value = self.copy_df.loc[idx, col_name]
                    if pd.notna(cell_value):  # Only include non-NaN values
                        sheet_row = idx + 2  # +2 for 1-based index and header
                        updates.append({
                            "range": f"{col_letter}{sheet_row}",
                            "values": [[cell_value]]
                        })

        # 4. Handle updates for existing columns
        if existing_columns:
            # Convert to string for reliable comparison across types (like int vs float)
            orig_str = self.original_df[list(existing_columns)].astype(str)
            copy_str = self.copy_df[list(existing_columns)].astype(str)

            # Find cells that are different
            changed_mask = orig_str != copy_str

            # Check if any changes were made to existing columns
            if changed_mask.any().any():
                # Prepare updates for each changed cell in existing columns
                index_to_row = {idx: idx + 2 for idx in self.original_df.index}  # +2 for 1-based index and header

                for idx in changed_mask.index:
                    if idx not in index_to_row: continue  # Skip if index not found
                    row_changes = changed_mask.loc[idx]
                    for col_name in row_changes[row_changes].index:  # Iterate through changed columns
                        try:
                            # Get row and column position in the sheet
                            sheet_row = index_to_row[idx]
                            # Find column index (0-based) and convert to letter (A=1)
                            col_idx = self.original_df.columns.get_loc(col_name)
                            col_letter = gspread.utils.rowcol_to_a1(1, col_idx + 1)[:-1]  # Get letter part of A1 notation

                            # Get the new value from the modified copy
                            new_value = self.copy_df.loc[idx, col_name]

                            # Handle potential NaN values for gspread
                            if pd.isna(new_value):
                                new_value = ""  # Convert NaN to empty string for existing columns

                            # Add to batch updates
                            updates.append({
                                "range": f"{col_letter}{sheet_row}",
                                "values": [[new_value]]
                            })
                        except KeyError:
                            print(f"Warning: Column '{col_name}' not found in original columns. Skipping update.")
                            continue

        # 5. Apply updates to the sheet and the original DataFrame
        if updates:
            try:
                self.worksheet.batch_update(updates, value_input_option='USER_ENTERED')
                print(f"Updated {len(updates)} cells in Google Sheet.")

                # Update the original DataFrame to match the copy
                # Simply replace the original with the copy to include new columns
                for col in self.copy_df.columns:
                    if col in self.original_df.columns:
                        # For existing columns, only update changed values
                        self.original_df[col] = self.copy_df[col]
                    else:
                        # For new columns, add the entire column
                        self.original_df[col] = self.copy_df[col]

                print(f"Updated original DataFrame in memory.")
            except Exception as e:
                print(f"Error during sheet update: {e}")
        else:
            print("No changes detected.")

        return True  # Indicate successful exit
    
def test_something():
    f = DataSheet.from_sheet(id='apsodifjapdosifj', sheet_id='step1')

    with f.start_update() as change:
        llm_new_fields = {
            'verse': 'something funny',
            'image': "=IMAGE('http://testing.com/something.jpg')",
        }

        change.loc[1, llm_new_fields.keys()] = list(llm_new_fields.values())












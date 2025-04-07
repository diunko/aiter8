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
    def from_sheet(cls, id, sheet_id) -> 'DataSheet':
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
        self.worksheet: gspread.Worksheet = original_df._worksheet
        
    def __enter__(self):
        # Create a copy for modifications
        self.copy_df = self.original_df.copy()
        return self.copy_df

    def _calculate_updates(self):
        """Compares the copied DataFrame with the original and returns a list of updates for gspread."""
        updates = []
        # Convert DataFrames to string for reliable comparison across types
        # Make copies to avoid modifying the originals during calculation
        orig_str = self.original_df.astype(str)
        copy_str = self.copy_df.astype(str)

        # 1. Identify new columns (columns in copy_df but not in original_df)
        original_columns = set(self.original_df.columns)
        copy_columns = set(self.copy_df.columns)
        new_columns = copy_columns - original_columns
        existing_columns = original_columns.intersection(copy_columns)

        # 2. Add header updates for new columns
        if new_columns:
            existing_col_count = len(original_columns)
            # Sort new columns alphabetically for consistent sheet layout
            sorted_new_columns = sorted(list(new_columns))
            col_letter_map_new = {} # Store A1 letters for new columns

            for i, col_name in enumerate(sorted_new_columns):
                # Calculate 0-based column index in the *final* sheet layout
                sheet_col_idx = existing_col_count + i
                # Get A1 letter (e.g., 'A', 'B', ... 'AA') using 1-based index
                col_letter = gspread.utils.rowcol_to_a1(1, sheet_col_idx + 1)[:-1]
                col_letter_map_new[col_name] = col_letter

                # Add header update
                updates.append({
                    "range": f"{col_letter}1",
                    "values": [[col_name]]
                })

            # 3. Add updates for explicitly set values in new columns (ignore NaN in the *copy*)
            # Map DataFrame index to sheet row number (+2 for 1-based index and header)
            index_to_sheet_row = {idx: idx + 2 for idx in self.copy_df.index}

            for col_name in sorted_new_columns: # Iterate using the same sorted order
                col_letter = col_letter_map_new[col_name]
                for idx in self.copy_df.index:
                    if idx not in index_to_sheet_row: continue # Should not happen if index is consistent

                    cell_value = self.copy_df.loc[idx, col_name]
                    # Only add updates for non-NaN values in the copy
                    if pd.notna(cell_value):
                        sheet_row = index_to_sheet_row[idx]
                        updates.append({
                            "range": f"{col_letter}{sheet_row}",
                            # Use the original value type, let gspread handle it with USER_ENTERED
                            "values": [[cell_value]] # Removed str() conversion
                        })

        # 4. Handle updates for existing columns
        if existing_columns:
            # Use the string-converted dataframes for comparison
            orig_subset_str = orig_str[list(existing_columns)]
            copy_subset_str = copy_str[list(existing_columns)]

            # Find cells that are different (ignoring NaNs in the copy potentially)
            # We need the original comparison logic here based on the copy_df, not copy_str
            changed_mask = self.original_df[list(existing_columns)] != self.copy_df[list(existing_columns)]
            # Consider NaN != non-NaN as a change. Also consider non-NaN != NaN as a change.
            # The string comparison approach might handle this subtly, let's refine
            # A more robust way: Check where values differ OR where one is NaN and the other isn't.
            changed_mask = (self.original_df[list(existing_columns)] != self.copy_df[list(existing_columns)]) & \
                           ~(self.original_df[list(existing_columns)].isna() & self.copy_df[list(existing_columns)].isna())


            if changed_mask.any().any():
                # Pre-calculate A1 letters for existing columns for efficiency
                col_letter_map_existing = {}
                for col_name in existing_columns:
                    try:
                        col_idx = self.original_df.columns.get_loc(col_name) # 0-based
                        col_letter_map_existing[col_name] = gspread.utils.rowcol_to_a1(1, col_idx + 1)[:-1]
                    except KeyError:
                        # Should not happen if column is in existing_columns
                        print(f"Warning: Column '{col_name}' somehow not found during A1 calculation.")
                        continue

                # Map DataFrame index to sheet row number (+2 for 1-based index and header)
                index_to_sheet_row = {idx: idx + 2 for idx in self.original_df.index}

                # Iterate through the boolean mask to find changed cells
                for idx in changed_mask.index:
                    if idx not in index_to_sheet_row: continue # Skip if index somehow changed/removed

                    row_changes = changed_mask.loc[idx]
                    sheet_row = index_to_sheet_row[idx]

                    for col_name in row_changes[row_changes].index: # Iterate only through changed columns in this row
                        if col_name not in col_letter_map_existing: continue # Skip if A1 mapping failed

                        col_letter = col_letter_map_existing[col_name]
                        new_value = self.copy_df.loc[idx, col_name]

                        # Convert NaN to empty string for gspread compatibility in existing columns
                        # Formulas starting with '=' should be preserved
                        if pd.isna(new_value):
                            update_value = ""
                        else:
                            update_value = new_value # Removed str() conversion

                        updates.append({
                            "range": f"{col_letter}{sheet_row}",
                            "values": [[update_value]]
                        })
        return updates

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            # An exception occurred within the 'with' block, don't apply updates
            print(f"Exception occurred during context: {exc_val}. No updates applied.")
            return False # Indicate exception was not handled here

        # Calculate the necessary updates by comparing the copy with the original
        updates = self._calculate_updates() # Use the new method

        # Apply updates to the sheet and the original DataFrame only if there are changes
        if updates:
            try:
                # Apply batch updates to the Google Sheet
                self.worksheet.batch_update(updates, value_input_option='USER_ENTERED')
                print(f"Updated {len(updates)} cells in Google Sheet.")

                # Update the original DataFrame in memory to match the modified copy
                # First, update existing values using the copy_df (handles modifications)
                # Note: update doesn't add new columns or handle NaN overwrites perfectly by default
                # self.original_df.update(self.copy_df) # Let's replace with direct assignment for clarity

                # A simpler way to reflect all changes (including NaNs and new cols)
                # might be to just replace relevant parts or the whole df if structures changed.
                # Since we handle new columns separately and calculated exact diffs,
                # let's update based on the copy precisely.

                # Add new columns first
                new_cols = self.copy_df.columns.difference(self.original_df.columns)
                for col in new_cols:
                     # Ensure new columns are added with the correct index alignment
                    self.original_df[col] = self.copy_df[col]

                # Update existing columns (including setting values to NaN if needed)
                existing_cols = self.original_df.columns.intersection(self.copy_df.columns)
                self.original_df[list(existing_cols)] = self.copy_df[list(existing_cols)]


                print(f"Updated original DataFrame in memory.")

            except Exception as e:
                # Handle errors during the sheet update process
                print(f"Error during sheet update: {e}")
                # Optionally, decide if the exception should be propagated.
                # For now, print the error and indicate successful context exit (True).
                # Consider returning False or re-raising if the update failure is critical.
                # return False # Or raise e
        else:
            # No changes were detected between the original and the copy
            print("No changes detected.")

        return True # Indicate successful exit from the context manager
    
def test_something():
    f = DataSheet.from_sheet(id='apsodifjapdosifj', sheet_id='step1')

    with f.start_update() as change:
        llm_new_fields = {
            'verse': 'something funny',
            'image': "=IMAGE('http://testing.com/something.jpg')",
        }

        change.loc[1, llm_new_fields.keys()] = list(llm_new_fields.values())












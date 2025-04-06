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

        # 1. Compare the original state with the final state of the copy
        # Use the original DataFrame from __init__ and the copy_df modified within the block
        # Convert to string for reliable comparison across types (like int vs float)
        orig_str = self.original_df.astype(str)
        copy_str = self.copy_df.astype(str)

        # Find cells that are different
        changed_mask = orig_str != copy_str

        # Check if any changes were actually made
        if not changed_mask.any().any():
            print("No changes detected in the DataFrame.")
            return True # Exit cleanly

        # 2. Prepare batch updates ONLY for the detected changes
        updates = []
        # Assuming the DataFrame index corresponds to sheet rows (after header)
        # If using a different index (e.g., 'id'), map it to row numbers
        # For simplicity, assuming default RangeIndex 0, 1, 2...
        index_to_row = {idx: idx + 2 for idx in self.original_df.index} # +2 for 1-based index and header

        for idx in changed_mask.index:
            if idx not in index_to_row: continue # Skip if index not found (e.g., after dropping rows)
            row_changes = changed_mask.loc[idx]
            for col_name in row_changes[row_changes].index: # Iterate only through changed columns for this row
                try:
                    # Get row and column position in the sheet
                    sheet_row = index_to_row[idx]
                    # Find column index (0-based) and convert to letter (A=1)
                    col_idx = self.original_df.columns.get_loc(col_name)
                    col_letter = gspread.utils.rowcol_to_a1(1, col_idx + 1)[:-1] # Get letter part of A1 notation

                    # Get the new value from the modified copy
                    new_value = self.copy_df.loc[idx, col_name]

                    # Handle potential NaN values for gspread
                    if pd.isna(new_value):
                        new_value = "" # Or use a specific marker if needed

                    # Add to batch updates
                    updates.append({
                        "range": f"{col_letter}{sheet_row}",
                        "values": [[new_value]]
                    })
                except KeyError:
                    print(f"Warning: Column '{col_name}' not found in original columns. Skipping update.")
                    continue


        # 3. Apply updates to the sheet and the original DataFrame
        if updates:
            try:
                self.worksheet.batch_update(updates)
                print(f"Updated {len(updates)} cells in Google Sheet.")

                # Update the original DataFrame in memory to match the copy
                # Use the changed_mask to apply changes efficiently
                self.original_df.update(self.copy_df[changed_mask])
                # self.original_df = self.copy_df # Or simply replace if easier

                print(f"Updated original DataFrame in memory.")
            except Exception as e:
                print(f"Error during sheet update: {e}")
                # Decide if you want to propagate the error or handle it
                # return False # Or re-raise e
        else:
            print("Detected changes, but no valid updates generated (check column names?).")


        return True # Indicate successful exit (or exception handled)
    
def test_something():
    f = DataSheet.from_sheet(id='apsodifjapdosifj', sheet_id='step1')

    with f.start_update() as change:
        llm_new_fields = {
            'verse': 'something funny',
            'image': "=IMAGE('http://testing.com/something.jpg')",
        }

        # Can use the simpler DataFrame.update() method
        temp = pd.DataFrame([llm_new_fields], index=[1])
        change.update(temp)
        
        # Or the original method
        # change.loc[1, llm_new_fields.keys()] = list(llm_new_fields.values())












import pandas as pd
import os
import gspread
import numpy as np

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
        
        # Convert string boolean values to Python booleans
        for col in df.columns:
            # Check if column contains potential boolean string values
            if df[col].dtype == 'object':  # String columns
                # Create masks for case-insensitive TRUE/FALSE values
                true_mask = df[col].astype(str).str.upper() == 'TRUE'
                false_mask = df[col].astype(str).str.upper() == 'FALSE'
                
                # If either TRUE or FALSE values exist in this column
                if true_mask.any() or false_mask.any():
                    # Create a series to avoid SettingWithCopyWarning
                    series = df[col].copy()
                    series[true_mask] = True
                    series[false_mask] = False
                    df[col] = series
        
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
        self.original_df: pd.DataFrame = original_df
        self.worksheet: gspread.Worksheet = original_df._worksheet
        
    def __enter__(self):
        # Create a copy for modifications
        self.copy_df = self.original_df.copy()
        return self.copy_df

    def _calculate_updates(self):
        """Compares the copied DataFrame with the original and returns a list of updates for gspread, preserving column order."""
        header_updates = []
        cell_updates = []

        # Step 1: Identify Column Sets & Order
        original_columns_list = list(self.original_df.columns)
        final_columns_list = list(self.copy_df.columns) # Target order
        original_columns_set = set(original_columns_list)
        final_columns_set = set(final_columns_list)

        new_columns_list = [col for col in final_columns_list if col not in original_columns_set]
        existing_columns_list = [col for col in final_columns_list if col in original_columns_set]

        # Map final column names to their 0-based index for A1 calculation
        final_col_to_idx = {col_name: i for i, col_name in enumerate(final_columns_list)}

        # Step 2: Prepare Header Updates (Preserving Order)
        if new_columns_list:
            for col_name in new_columns_list: # Iterate in the order they appear in final_columns_list
                sheet_col_idx = final_col_to_idx[col_name]
                col_letter = gspread.utils.rowcol_to_a1(1, sheet_col_idx + 1)[:-1]
                header_updates.append({
                    "range": f"{col_letter}1",
                    "values": [[col_name]]
                })

        # Step 3: Create Aligned Temporary DataFrames (Preserving Order)
        # Ensure consistent index type for comparison/reindexing if needed
        # self.original_df.index = self.original_df.index.astype(self.copy_df.index.dtype)

        temp_original_df = self.original_df.copy()
        temp_copy_df = self.copy_df.copy()

        # Reindex both using the final desired column order
        # Use pd.NA for compatible missing value representation if possible
        try:
            temp_original_df = temp_original_df.reindex(columns=final_columns_list, fill_value=pd.NA)
            # temp_copy_df doesn't strictly need reindexing if final_columns_list came from it,
            # but doing so ensures the columns object is identical, which might be safer.
            temp_copy_df = temp_copy_df.reindex(columns=final_columns_list, fill_value=pd.NA)
        except TypeError: # Fallback for older pandas versions that might not support pd.NA in fill_value
             temp_original_df = temp_original_df.reindex(columns=final_columns_list, fill_value=np.nan)
             temp_copy_df = temp_copy_df.reindex(columns=final_columns_list, fill_value=np.nan)

        # Ensure indices match for proper comparison row-wise
        # This shouldn't be necessary if copy_df starts as a copy and indices aren't modified,
        # but as a safeguard:
        common_index = self.original_df.index.intersection(self.copy_df.index)
        # If indices *could* diverge, filter temps: (but let's assume they don't for this use case)
        # temp_original_df = temp_original_df.loc[common_index]
        # temp_copy_df = temp_copy_df.loc[common_index]

        # Step 4: Calculate Cell Value Updates (Simplified Diff, Order-Aware)
        # Use .compare() for detailed differences, handling NaNs correctly by default
        # compare() returns a DataFrame with multi-index columns ('self', 'other')
        diff = temp_original_df.compare(temp_copy_df, keep_shape=False, keep_equal=False)
        print("\nDifferences found:")
        print(diff.to_string())
        
        # Map DataFrame index to sheet row number (+2 for 1-based index and header)
        index_to_sheet_row = {idx: idx + 2 for idx in self.original_df.index}

        for idx in diff.index:
            assert idx in index_to_sheet_row

            sheet_row = index_to_sheet_row[idx]
            
            # Get columns where differences were found
            diff_cols = diff.columns.get_level_values(0).unique()
            
            # For each changed column, get the 'other' value
            for col_name in diff_cols:
                # Access the original and new values using the MultiIndex
                orig_value = diff.loc[idx, (col_name, 'self')]
                new_value = diff.loc[idx, (col_name, 'other')]
                
                # Skip if both values are NaN (this is the key fix)
                if pd.isna(orig_value) and pd.isna(new_value):
                    continue
                
                sheet_col_idx = final_col_to_idx[col_name]
                col_letter = gspread.utils.rowcol_to_a1(1, sheet_col_idx + 1)[:-1]
                
                # Format value for sheet (NaN/NA -> "")
                update_value = "" if pd.isna(new_value) else new_value
                
                cell_updates.append({
                    "range": f"{col_letter}{sheet_row}",
                    "values": [[update_value]]
                })

                
        # Step 5: Combine and Return
        return header_updates + cell_updates

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            # An exception occurred within the 'with' block, don't apply updates
            print(f"Exception occurred during context: {exc_val}. No updates applied.")
            return False # Indicate exception was not handled here

        # Calculate the necessary updates by comparing the copy with the original
        # This now uses the order-preserving logic
        updates = self._calculate_updates() # Use the new method

        # Apply updates to the sheet and the original DataFrame only if there are changes
        if updates:
            try:
                # Apply batch updates to the Google Sheet
                self.worksheet.batch_update(updates, value_input_option='USER_ENTERED')
                print(f"Updated {len(updates)} cells in Google Sheet.")
                print(f"{updates=}")

                # Update the original DataFrame in memory to match the modified copy
                # Preserve the original object ID

                new_cols = self.copy_df.columns.difference(self.original_df.columns)
                cols_to_update = self.copy_df.columns.intersection(self.original_df.columns)
                final_col_order = list(self.copy_df.columns) # Target order

                # Update existing column values
                if not cols_to_update.empty:
                    self.original_df[cols_to_update] = self.copy_df[cols_to_update]

                # Add any new columns
                if not new_cols.empty:
                    for col in new_cols:
                        self.original_df[col] = self.copy_df[col]

                # Ensure the column order matches copy_df
                current_order = list(self.original_df.columns)
                if current_order != final_col_order:
                    # Reindex creates a *new* DataFrame, we need to update inplace.
                    # A common way is to replace the internal manager object.
                    # This is somewhat internal, but often necessary for inplace reordering.
                    temp_reordered = self.original_df[final_col_order].copy()
                    self.original_df._mgr = temp_reordered._mgr
                    # Alternatively, clear and refill, but _mgr replacement is more direct
                    # self.original_df.drop(self.original_df.columns, axis=1, inplace=True)
                    # for col in final_col_order:
                    #     self.original_df[col] = temp_reordered[col]


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












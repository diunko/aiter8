import gspread
import os
import pandas as pd
import concurrent.futures
from typing import Dict, Any, List
from tqdm import tqdm

from iter8.data_sheet import DataSheet
from iter8.llm import llm_json

CREDS_PATH = f"{os.environ['HOME']}/kk/hahaunt/"
gc = gspread.oauth(
    credentials_filename=f"{CREDS_PATH}/hahaunt-content-pipeline-client.json",
    authorized_user_filename=f"{CREDS_PATH}/authorized_user.json",
)

# Number of parallel processes
PARALLEL_BATCH_SIZE = 10
# Number of results to accumulate before updating sheet
UPDATE_BATCH_SIZE = 20


def process_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process a single record using LLM.
    
    Args:
        record: Dictionary containing record data
        
    Returns:
        Dictionary with LLM analysis results
    """
    prompt = (
        f"is in this record {record} a transcription a correct or almost correct "
        f"transcription of english word 'en' in ukrainian? it shouldn't be a translation, "
        f"it should be a transcription of how english word sounds with ukraininan letters:, "
        "use this output format: {{'thinking': '...', 'is_correct_transcription': true/false}} "
        "consider it's not correct if transcription is very wrong, and in that case also add field "
        "{{'corrected_transcription': '...'}}. Use capital letter for stressed letter, e.g. 'нЕтворк'"
    )

    if record.get('thinking') and type(record.get('is_correct_transcription')) == bool:
        return record
    
    print('calling llm')
    
    result = llm_json(prompt)
    return result


def update_datasheet(datasheet: DataSheet, accumulated_results: List[tuple]) -> None:
    """
    Update the datasheet with accumulated results.
    
    Args:
        datasheet: The DataSheet object to update
        accumulated_results: List of (index, result_dict) tuples
    """
    print(f"Updating sheet with {len(accumulated_results)} results")
    with datasheet.start_update() as change:
        for idx, result in accumulated_results:
            change.loc[idx, result.keys()] = result.values()


def test_sample_2():
    # Get the spreadsheet
    files = gc.list_spreadsheet_files(folder_id="1fgjHAyc56O_IgYnJRH48Qe8OFmzMWO1_")
    print(f"Found {len(files)} spreadsheets")
    
    # Load the data
    ds = DataSheet.from_sheet(id=files[0]["id"], sheet_id='step-00')
    total_records = len(ds)
    print(f"Loaded datasheet with {total_records} records")
    
    # Initialize accumulator for results
    accumulated_results = []
    processed_count = 0
    
    # Process in batches
    with concurrent.futures.ThreadPoolExecutor(max_workers=PARALLEL_BATCH_SIZE) as executor:
        futures = []
        
        # Submit all tasks
        print("Submitting tasks to process records...")
        for idx, row in ds.iterrows():
            record_dict = row.to_dict()
            future = executor.submit(process_record, record_dict)
            # Store the future with its index for later identification
            futures.append((idx, future))
        
        # Process results as they complete with progress bar
        print(f"Processing {len(futures)} records with {PARALLEL_BATCH_SIZE} workers:")
        progress_bar = tqdm(total=len(futures), desc="Processing records", unit="record")
        
        for idx, future in futures:
            try:
                result = future.result()
                progress_bar.update(1)
                processed_count += 1
                
                # Log occasional details
                if processed_count % 5 == 0:
                    progress_bar.set_postfix({"Last record": idx, "Accumulated": len(accumulated_results)})
                
                accumulated_results.append((idx, result))
                
                # When we've accumulated enough results, update the sheet
                if len(accumulated_results) >= UPDATE_BATCH_SIZE:
                    update_datasheet(ds, accumulated_results)
                    progress_bar.set_postfix({"Updated": processed_count})
                    accumulated_results = []  # Reset accumulator
                    
            except Exception as e:
                progress_bar.write(f"Error processing record at index {idx}: {e}")
        
        progress_bar.close()
        
        # Handle any remaining results
        if accumulated_results:
            update_datasheet(ds, accumulated_results)
            print(f"Final update completed. Total records processed: {processed_count}")


if __name__ == "__main__":
    test_sample_2() 
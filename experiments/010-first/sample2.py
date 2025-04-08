import gspread
import os
import pandas as pd

from iter8.data_sheet import DataSheet
from iter8.llm import llm_json

CREDS_PATH = f"{os.environ['HOME']}/kk/hahaunt/"
gc = gspread.oauth(
    credentials_filename=f"{CREDS_PATH}/hahaunt-content-pipeline-client.json",
    authorized_user_filename=f"{CREDS_PATH}/authorized_user.json",
)


def test_sample_1():
    ff = gc.list_spreadsheet_files(folder_id="1fgjHAyc56O_IgYnJRH48Qe8OFmzMWO1_")
    print(f"{ff=}")

    ds = DataSheet.from_sheet(id=ff[0]["id"], sheet_id='step-00')

    for n, (idx, r) in enumerate(ds.iterrows()):
        # if n > 3: break

        print(f"{n=}, {idx=}, {r.to_dict()=}")

        result = llm_json(
            f"is in this record {r.to_dict()} a transcription a correct or almost correct transcription of english word 'en' in ukrainian? it shouldn't be a translation, it should be a transcription of how english word sounds with ukraininan letters:, "
            "use this output format: {{'thinking': '...', 'is_correct_transcription': true/false}}",
        )
        print(f"LLM Analysis: {result}")

        with ds.start_update() as change:
            change.loc[idx, result.keys()] = result.values()
            # change.loc[0, 'memo3'] = "ch"
            # change.loc[0, 'pic'] = '=image("https://hahaunt-v1.s3.amazonaws.com/pictures-v17/2025-01-03/alone/00.jpg")'
            # change.loc[0, 'ch?'] = True
            # change.loc[0, 'kukuku'] = 'aosidjfoiasdjoijfd'



        # change.loc[0, 'bla'] = False

        # r = change.loc[5].to_dict()
        # result = eval_opinion(r)
        
        # change.loc[5, list(result.keys())] = list(result.values())




if __name__ == "__main__":
    test_sample_1()

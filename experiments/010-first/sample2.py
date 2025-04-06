import gspread
import os
import pandas as pd

from iter8.data_sheet import DataSheet

CREDS_PATH = f"{os.environ['HOME']}/kk/hahaunt/"
gc = gspread.oauth(
    credentials_filename=f"{CREDS_PATH}/hahaunt-content-pipeline-client.json",
    authorized_user_filename=f"{CREDS_PATH}/authorized_user.json",
)


def sample_1():
    ff = gc.list_spreadsheet_files(folder_id="1fgjHAyc56O_IgYnJRH48Qe8OFmzMWO1_")
    print(f"{ff=}")

    ds = DataSheet.from_sheet(id=ff[0]["id"], sheet_id='step-00')

    with ds.start_update() as change:
        change.loc[5, 'memo3'] = "ch"
        change.loc[5, 'pic'] = '=image("https://hahaunt-v1.s3.amazonaws.com/pictures-v17/2025-01-03/alone/00.jpg")'



if __name__ == "__main__":
    sample_1()

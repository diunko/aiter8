import gspread
import os
import pandas as pd

CREDS_PATH = f"{os.environ['HOME']}/kk/hahaunt/"
gc = gspread.oauth(
    credentials_filename=f"{CREDS_PATH}/hahaunt-content-pipeline-client.json",
    authorized_user_filename=f"{CREDS_PATH}/authorized_user.json",
)


def sample_1():
    ff = gc.list_spreadsheet_files(folder_id="1fgjHAyc56O_IgYnJRH48Qe8OFmzMWO1_")
    print(f"{ff=}")
    s01 = gc.open_by_key(ff[0]["id"])
    print(f"{s01=}")
    ww = s01.worksheets()
    print(f"{ww=}")
    W = ww[0]
    print(f"{W=}")
    print(f"{W.get_all_records()[:3]=}")


def sample_2():

    ff = gc.list_spreadsheet_files(folder_id="1fgjHAyc56O_IgYnJRH48Qe8OFmzMWO1_")
    print(f"{ff=}")
    s01 = gc.open_by_key(ff[0]["id"])
    print(f"{s01=}")
    ww = s01.worksheets()
    print(f"{ww=}")
    W = ww[0]
    print(f"{W=}")
    print(f"{W.get_all_records()[:3]=}")

    records = W.get_all_records()  # returns a list of dicts
    for i, row_data in enumerate(records, start=2):
        # "start=2" because row 1 in the sheet is typically headers
        if row_data["id"] == 123:
            # Example: Update 'memo' column (assume column 5 is 'memo')
            W.update_cell(i, 5, "test17")
            # Or update a few columns in one shot using the range:
            # Example: columns 5-7 in row i => "E{i}:G{i}"
            W.update(f"E{i}:G{i}", [["memoVal", "memo2Val", "TRUE"]])
            print(f"Row {i} updated for record #123!")
            break


example = [
    {
        "id": 1,
        "en": "nest",
        "ua": "гніздо",
        "transcription": "гнІздо",
        "memo": "небо",
        "memo2": "несеться",
        "works": "FALSE",
        "ch?": "TRUE",
    },
    {
        "id": 2,
        "en": "nest",
        "ua": "гніздо",
        "transcription": "гнІздо",
        "memo": "ніс",
        "memo2": "ніс",
        "works": "TRUE",
        "ch?": "FALSE",
    },
    {
        "id": 3,
        "en": "nest",
        "ua": "гніздо",
        "transcription": "гнІздо",
        "memo": "ніж",
        "memo2": "ніж",
        "works": "FALSE",
        "ch?": "FALSE",
    },
]


def sample_3():
    df = pd.DataFrame(example)
    print(f"{df=}")


if __name__ == "__main__":
    sample_3()

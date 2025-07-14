import os
import zipfile
import pandas as pd
from glob import glob

def clean_campaign_data():
    input_dir = "files/input/"
    output_dir = "files/output/"
    os.makedirs(output_dir, exist_ok=True)

    all_dfs = []

    # Cargar todos los CSVs dentro de ZIP sin descomprimirlos
    for zip_file in glob(os.path.join(input_dir, "*.csv.zip")):
        with zipfile.ZipFile(zip_file) as archive:
            for filename in archive.namelist():
                with archive.open(filename) as file:
                    df = pd.read_csv(file, sep=",")
                    all_dfs.append(df)

    df = pd.concat(all_dfs, ignore_index=True)

    # CLIENT
    df_client = df[[
        "client_id", "age", "job", "marital", "education",
        "credit_default", "mortgage"
    ]].copy()

    df_client["job"] = df_client["job"].str.replace(".", "", regex=False).str.replace("-", "_", regex=False)
    df_client["education"] = df_client["education"].str.replace(".", "_", regex=False)
    df_client["education"] = df_client["education"].replace("unknown", pd.NA)
    df_client["credit_default"] = df_client["credit_default"].apply(lambda x: 1 if str(x).lower() == "yes" else 0)
    df_client["mortgage"] = df_client["mortgage"].apply(lambda x: 1 if str(x).lower() == "yes" else 0)

    df_client.to_csv(os.path.join(output_dir, "client.csv"), index=False)

    # CAMPAIGN
    df_campaign = df[[
        "client_id", "number_contacts", "contact_duration",
        "previous_campaign_contacts", "previous_outcome",
        "campaign_outcome", "day", "month"
    ]].copy()

    df_campaign["previous_outcome"] = df_campaign["previous_outcome"].apply(lambda x: 1 if str(x).lower() == "success" else 0)
    df_campaign["campaign_outcome"] = df_campaign["campaign_outcome"].apply(lambda x: 1 if str(x).lower() == "yes" else 0)

    df_campaign["last_contact_date"] = pd.to_datetime(
        "2022-" + df_campaign["month"].astype(str) + "-" + df_campaign["day"].astype(str),
        format="%Y-%b-%d", errors="coerce"
    ).dt.strftime("%Y-%m-%d")

    df_campaign = df_campaign[[
        "client_id", "number_contacts", "contact_duration",
        "previous_campaign_contacts", "previous_outcome",
        "campaign_outcome", "last_contact_date"
    ]]

    df_campaign.to_csv(os.path.join(output_dir, "campaign.csv"), index=False)

    # ECONOMICS
    df_economics = df[[
        "client_id", "cons_price_idx", "euribor_three_months"
    ]].copy()

    df_economics.to_csv(os.path.join(output_dir, "economics.csv"), index=False)

if __name__ == "__main__":
    clean_campaign_data()


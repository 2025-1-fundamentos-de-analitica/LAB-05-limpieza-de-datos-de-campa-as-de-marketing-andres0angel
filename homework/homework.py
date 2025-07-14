import os
import zipfile
import pandas as pd
from glob import glob

def clean_campaign_data():
    input_dir = "files/input/"
    output_dir = "files/output/"
    os.makedirs(output_dir, exist_ok=True)

    client_rows = []
    campaign_rows = []
    economics_rows = []

    for zip_path in glob(os.path.join(input_dir, "*.csv.zip")):
        with zipfile.ZipFile(zip_path) as z:
            for filename in z.namelist():
                with z.open(filename) as f:
                    df = pd.read_csv(f)

                    # Asegurar que client_id esté presente
                    if 'client_id' not in df.columns:
                        continue

                    # CLIENT
                    df_client = df[['client_id', 'age', 'job', 'marital', 'education',
                                    'credit_default', 'mortgage']].copy()

                    df_client['job'] = df_client['job'].str.replace('.', '', regex=False).str.replace('-', '_', regex=False)
                    df_client['education'] = df_client['education'].str.replace('.', '_', regex=False)
                    df_client['education'] = df_client['education'].replace('unknown', pd.NA)
                    df_client['credit_default'] = df_client['credit_default'].apply(lambda x: 1 if str(x).lower() == "yes" else 0)
                    df_client['mortgage'] = df_client['mortgage'].apply(lambda x: 1 if str(x).lower() == "yes" else 0)

                    client_rows.append(df_client)

                    # CAMPAIGN
                    df_campaign = df[['client_id', 'number_contacts', 'contact_duration',
                                      'previous_campaing_contacts', 'previous_outcome',
                                      'campaign_outcome', 'day', 'month']].copy()

                    df_campaign['previous_outcome'] = df_campaign['previous_outcome'].apply(lambda x: 1 if str(x).lower() == "success" else 0)
                    df_campaign['campaign_outcome'] = df_campaign['campaign_outcome'].apply(lambda x: 1 if str(x).lower() == "yes" else 0)

                    # Generar fecha
                    df_campaign['last_contact_day'] = pd.to_datetime(
                        '2022-' + df_campaign['month'].astype(str) + '-' + df_campaign['day'].astype(str),
                        errors='coerce'
                    ).dt.strftime('%Y-%m-%d')

                    df_campaign = df_campaign[['client_id', 'number_contacts', 'contact_duration',
                                               'previous_campaing_contacts', 'previous_outcome',
                                               'campaign_outcome', 'last_contact_day']]

                    campaign_rows.append(df_campaign)

                    # ECONOMICS
                    df_econ = df[['client_id', 'const_price_idx', 'eurobor_three_months']].copy()
                    economics_rows.append(df_econ)

    # Concatenar y guardar
    pd.concat(client_rows, ignore_index=True).to_csv(os.path.join(output_dir, "client.csv"), index=False)
    pd.concat(campaign_rows, ignore_index=True).to_csv(os.path.join(output_dir, "campaign.csv"), index=False)
    pd.concat(economics_rows, ignore_index=True).to_csv(os.path.join(output_dir, "economics.csv"), index=False)

if __name__ == "__main__":
    clean_campaign_data()

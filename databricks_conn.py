import os
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from databricks import sql

# Load environment variables
load_dotenv()
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")

# Validate environment variables
if not all([DATABRICKS_TOKEN, DATABRICKS_HOST, DATABRICKS_HTTP_PATH]):
    raise EnvironmentError("Missing one or more Databricks connection environment variables.")

# --- Helper to open a new connection ---
def get_connection():
    return sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN
    )

# --- Reusable Data Functions ---
def get_customer_data():
    query = """
            SELECT p.CompanyName as Name,
                   p.ShipAddr1 as Address,
                   p.ShipState as State,
                   p.ShipZip as Zip,
                   p.ShipCity as City,
                   concat_ws(', ', p.Email, p.PaymentNotificationEmail, p.AdditionalBillingEmail, p.CustInvoiceEmail) as Emails,
                   concat_ws(', ', collect_list(concat_ws(' ', d.FirstName, d.LastName))) as Doctors,
                   p.PracticeId as MatchedEntityID
            FROM `sa`.`netsuite`.`customer_practices` p
            LEFT JOIN `sa`.`netsuite`.`customer_doctors` d on p.PracticeId = d.ParentID
            GROUP BY ALL
        """
    with get_connection() as conn:
        return pd.read_sql(query, conn)

def delete_matched_data_for_dso(dso_name: str):
    query = f"""
        DELETE FROM sa.dso_recon.matched_data
        WHERE Source = '{dso_name}'
    """
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            print(f"Deleted old matched data for DSO: {dso_name}")

def upload_to_datalake(df: pd.DataFrame):
    if df.empty:
        print("DataFrame is empty. No data to upload.")
        return

    df['UploadedDate'] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    columns = [
        'Name', 'Address', 'State', 'City', 'Zip', 'Emails', 'Doctors', 'SourceID', 'Source',
        'MatchedName', 'MatchedEmails', 'MatchedAddress', 'MatchedDoctors', 'TotalScore',
        'MatchedEntityID', 'MatchedPracticeName', 'AlreadyApproved', 'FileName', 'UploadedDate'
    ]

    def _quote_sql_value(value):
        if pd.isna(value) or value is None:
            return "NULL"
        if isinstance(value, (int, float)):
            return str(value)
        escaped_value = str(value).replace("'", "''")
        return f"'{escaped_value}'"

    all_values = []
    for _, row in df.iterrows():
        row_values = [_quote_sql_value(row.get(col)) for col in columns]
        all_values.append(f"({', '.join(row_values)})")

    values_string = ',\n'.join(all_values)
    column_string = ', '.join([f'`{col}`' for col in columns])

    bulk_insert_query = f"""
        INSERT INTO `sa`.`dso_recon`.`matched_data` ({column_string})
        VALUES {values_string};
    """

    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                print("Executing bulk insert (no transaction)...")
                cursor.execute(bulk_insert_query)
                print("Bulk insert successful.")
    except Exception as e:
        print(f"Bulk insert failed. Error: {e}")
        raise

def get_dso_config_data():
    with get_connection() as conn:
        query = "SELECT * FROM sa.dso_recon.dso_config"
        return pd.read_sql(query, conn)

def get_dso_dropdown_options():
    with get_connection() as conn:
        query = "SELECT CompanyName AS Name, DSOId AS ID FROM sa.netsuite.customer_dso"
        return pd.read_sql(query, conn)

def get_approved_source_ids():
    """
    Fetches all approved source_ids from the sa._sigma_write_schema.approved_dso table
    Returns a set of approved source_ids for quick lookup
    """
    try:
        with get_connection() as conn:
            query = "SELECT source_id FROM sa._sigma_write_schema.approved_dso"
            df = pd.read_sql(query, conn)
            return set(df['source_id'].astype(str).tolist())
    except Exception as e:
        print(f"Error fetching approved source_ids: {e}")
        return set()  # Return empty set if query fails

def insert_or_update_dso_config(record):
    for key in ["ConcatSourceID", "ConcatDoctorName"]:
        record[key] = str(record.get(key, "")).lower() in ["true", "1", "on", "yes"]

    def format_value(v):
        if v is None or str(v).strip() == "":
            return "NULL"
        escaped = str(v).replace("'", "''")
        return f"'{escaped}'"

    columns = list(record.keys())
    values = [format_value(record[col]) for col in columns]

    select_clause = "SELECT " + ", ".join(
        f"{v} AS `{k}`" for k, v in zip(columns, values)
    )

    update_clause = ", ".join([
        f"target.`{col}` = source.`{col}`" for col in columns if col != "NSEntityID"
    ])
    insert_cols = ", ".join(f"`{col}`" for col in columns)
    insert_vals = ", ".join(values)

    merge_query = f"""
        MERGE INTO sa.dso_recon.dso_config AS target
        USING ({select_clause}) AS source
        ON target.NSEntityID = source.NSEntityID
        WHEN MATCHED THEN UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """

    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(merge_query)

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

# Establish connection
connection = sql.connect(
    server_hostname=DATABRICKS_HOST,
    http_path=DATABRICKS_HTTP_PATH,
    access_token=DATABRICKS_TOKEN
)


def get_customer_data():
    """Fetches the master customer data from the datalake."""
    # CORRECTED QUERY: Each part of the table name is now enclosed in backticks (`).
    query = """
            SELECT p.CompanyName                                                                                      as Name, \
                   p.ShipAddr1                                                                                        as Address, \
                   p.ShipState                                                                                        as State, \
                   p.ShipZip                                                                                          as Zip, \
                   p.ShipCity                                                                                         as City, \
                   concat_ws(', ', p.Email, p.PaymentNotificationEmail, p.AdditionalBillingEmail, \
                             p.CustInvoiceEmail)                                                                      as Emails, \
                   concat_ws(', ', \
                             collect_list(concat_ws(' ', d.FirstName, d.LastName)))                                   as Doctors, \
                   p.PracticeId                                                                                       as MatchedEntityID
            FROM `sa`.`netsuite`.`customer_practices` p
                     LEFT JOIN `sa`.`netsuite`.`customer_doctors` d on p.PracticeId = d.ParentID
            GROUP BY ALL \
            """
    return pd.read_sql(query, connection)


def upload_to_datalake(df: pd.DataFrame):
    """
    Uploads the matched data DataFrame to the datalake using a bulk INSERT.
    Compatible with Databricks SQL (no transactions).
    """
    if df.empty:
        print("DataFrame is empty. No data to upload.")
        return

    df['UploadedDate'] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    columns = [
        'Name', 'Address', 'State', 'City', 'Zip', 'Emails', 'Doctors', 'ExternalID', 'Source',
        'MatchedName', 'MatchedEmails', 'MatchedAddress', 'MatchedDoctors', 'Total_Score',
        'MatchedEntityID', 'MatchedPracticeName', 'FileName', 'UploadedDate'
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

    cursor = connection.cursor()
    try:
        print("Executing bulk insert (no transaction)...")
        cursor.execute(bulk_insert_query)
        print("Bulk insert successful.")
    except Exception as e:
        print(f"Bulk insert failed. Error: {e}")
    finally:
        cursor.close()



def get_dso_list():
    """Fetches a list of DSOs from the datalake."""
    cursor = connection.cursor()
    dso_names = []
    try:
        # CORRECTED QUERY: Used backticks (`) for the table identifier.
        cursor.execute("SELECT CompanyName, DSOId FROM `sa`.`netsuite`.`customer_dso`")
        for row in cursor.fetchall():
            dso_names.append(f"{row['CompanyName']} | {row['DSOId']}")
    finally:
        cursor.close()
    return dso_names
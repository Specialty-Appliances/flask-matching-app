import os
from dotenv import load_dotenv
load_dotenv()
from databricks import sql
import pandas as pd  # <-- Needed for read_sql()

#  Load env vars
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")

#  Validate
if not all([DATABRICKS_TOKEN, DATABRICKS_HOST, DATABRICKS_HTTP_PATH]):
    raise EnvironmentError("Missing one or more Databricks connection environment variables.")

#  Connection
connection = sql.connect(
    server_hostname=DATABRICKS_HOST,
    http_path=DATABRICKS_HTTP_PATH,
    access_token=DATABRICKS_TOKEN
)

def get_customer_data():
    query = """
        SELECT 
            p.CompanyName as Name,
            p.ShipAddr1 as Address,
            p.ShipState as State,
            p.ShipZip as Zip,
            concat_ws(', ', p.Email, p.PaymentNotificationEmail, p.AdditionalBillingEmail, p.CustInvoiceEmail) as Emails,
            concat_ws(', ', collect_list(concat_ws(' ', d.FirstName, d.LastName))) as Doctors,
            p.PracticeId as MatchedEntityID,
            p.CompanyName as MatchedPracticeName
        FROM sa.netsuite.customer_practices p
        LEFT OUTER JOIN sa.netsuite.customer_doctors d on p.PracticeId = d.ParentID
        GROUP BY ALL
    """
    df = pd.read_sql(query, connection)
    return df

def upload_to_datalake(df):
    from datetime import datetime

    uploaded_date = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    df['UploadedDate'] = uploaded_date
    cursor = connection.cursor()

    insert_query = """
    INSERT INTO sa.dso_recon.matched_data (
        MatchedName, MatchedEmails, MatchedAddress, MatchedDoctors, Total_Score,
        Name, Address, State, Zip, Emails, Doctors, ExternalID,
        Source, MatchedEntityID, MatchedPracticeName, UploadedDate, FileName
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    rows = [
        (
            row.get('MatchedName', ''),
            row.get('MatchedEmails', ''),
            row.get('MatchedAddress', ''),
            row.get('MatchedDoctors', ''),
            float(row.get('Total_Score', 0)),
            row.get('Name', ''),
            row.get('Address', ''),
            row.get('State', ''),
            row.get('Zip', ''),
            row.get('Emails', ''),
            row.get('Doctors', ''),
            row.get('ExternalID', ''),
            row.get('Source', ''),
            row.get('MatchedEntityID', ''),
            row.get('MatchedPracticeName', ''),
            row.get('UploadedDate') if isinstance(row.get('UploadedDate'), str) else row.get('UploadedDate').strftime("%Y-%m-%d %H:%M:%S"),
            row.get('FileName', '')
        )
        for _, row in df.iterrows()
    ]

    cursor.executemany(insert_query, rows)
    connection.commit()
    cursor.close()


#  Add this at the bottom
def get_dso_list():
    cursor = connection.cursor()
    dso_names = []
    cursor.execute("SELECT CompanyName, DSOId FROM sa.netsuite.customer_dso")
    for row in cursor.fetchall():
        dso_names.append(f"{row['CompanyName']} | {row['DSOId']}")
    cursor.close()
    return dso_names


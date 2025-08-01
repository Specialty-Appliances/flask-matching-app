import os
import re
import uuid
import logging
import json
import tempfile
import pandas as pd
from flask import Flask, request, render_template, redirect, url_for, session
from werkzeug.utils import secure_filename
from databricks_conn import get_customer_data, upload_to_datalake, get_dso_dropdown_options, insert_or_update_dso_config, get_dso_config_data, delete_matched_data_for_dso, get_approved_source_ids
from match_logic import match_records_by_fields
from datetime import datetime

# --- Config ---
UPLOAD_FOLDER = 'temp_uploads'
ALLOWED_EXTENSIONS = {'xlsx'}

app = Flask(__name__)
app.secret_key = "6cb3dd47625397ec6703cd04dbf6014e2671977d0ca1a16c2c858fa195b00255"
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
logging.basicConfig(level=logging.INFO)

# --- Helper Functions ---
def load_dso_data():
    return get_dso_config_data()

def save_dso_data(data):
    with open('data.json', 'w') as f:
        json.dump(data, f, indent=2)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# --- Routes ---
@app.route('/')
def welcome():
    return render_template('welcome.html')

@app.route('/dsorecon')
def dsorecon_home():
    return render_template('dsorecon_home.html')

@app.route('/upload', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'GET':
        dso_data = load_dso_data().to_dict(orient='records')
        dso_list = [f"{d['Name']} | {d['NSEntityID']}" for d in dso_data]
        return render_template('upload.html', dso_names=dso_list)

    if 'file' not in request.files:
        return "No file part in the request.", 400

    file = request.files['file']
    if file.filename == '':
        return "No file selected.", 400

    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        dso = request.form.get('dso')
        if not dso or '|' not in dso:
            return "Invalid DSO selection.", 400

        dso_name, dso_id = dso.split('|')
        dso_name = dso_name.strip()
        dso_id = dso_id.strip()
        session['dso'] = dso_name
        session['original_filename'] = filename

        config_df = load_dso_data().to_dict(orient='records')
        config_entry = next((e for e in config_df if e["Name"] == dso_name and str(e["NSEntityID"]) == dso_id), None)
        if not config_entry:
            return f"No config for DSO '{dso_name}'", 400

        dso_header = int(config_entry.get("Header", 0))
        concat_dr = config_entry.get("Concat_Doctor", [])
        sheet_name = config_entry.get("SheetName") or 0  # default to first sheet if not specified
        df = pd.read_excel(file, dtype=str, header=dso_header, sheet_name=sheet_name).fillna('')

        # Clean column names
        df.columns = df.columns.astype(str).str.strip().str.replace(r'[\r\n]+', '', regex=True)
        print("\nUploaded Columns:", df.columns.tolist())
        print(df.head())

        cleaned_mapping = {
            k: v for k, v in config_entry.items()
            if k not in ['ID', 'Name', 'NSEntityID', 'Type', 'Header', 'Concat_Doctor','SheetName']
               and isinstance(v, str)
               and v.strip().lower() != 'none'
        }

        try:
            df = df[list(cleaned_mapping.values())]
        except KeyError as e:
            return f"Missing expected column(s): {e}", 400

        df.columns = list(cleaned_mapping.keys())

        if concat_dr and all(col in df.columns for col in concat_dr):
            df['Doctors'] = df[concat_dr[0]].fillna('') + ' ' + df[concat_dr[1]].fillna('')

        if 'Emails' in df.columns and 'AddEmail' in df.columns:
            df['Emails'] = df[['Emails', 'AddEmail']].astype(str).agg(','.join, axis=1)
            df.drop(columns=['AddEmail'], inplace=True)
        elif 'AddEmail' in df.columns:
            df['Emails'] = df['AddEmail'].astype(str)
            df.drop(columns=['AddEmail'], inplace=True)
        elif 'Emails' not in df.columns:
            df['Emails'] = None

        df['Emails'] = df['Emails'].apply(
            lambda x: ','.join(sorted(set(e.strip() for e in str(x).split(',') if e.strip())))
            if pd.notnull(x) else None
        )

        df['Source'] = dso_name
        df['DSO_Id'] = config_entry.get("NSEntityID", '')
        df['Type'] = config_entry.get("Type", '')

        if 'SourceID' not in df.columns and 'PracticeName' in df.columns and 'Address' in df.columns:
            df['SourceID'] = df['PracticeName'].astype(str) + ' | ' + df['Address'].astype(str)

        temp_path = os.path.join(tempfile.gettempdir(), f"{uuid.uuid4()}.parquet")
        df.to_parquet(temp_path)
        session['prepared_data_path'] = temp_path
        print("df")

        df_preview = df.head(30)
        return render_template(
            'preview.html',
            table=df_preview.to_html(classes='table table-bordered', index=False),
            dso_name=dso_name
        )

@app.route('/run-matching', methods=['POST'])
def run_matching():
    dso_name = session.get('dso')
    original_filename = session.get('original_filename')
    prepared_path = session.get('prepared_data_path')

    if not all([dso_name, prepared_path, original_filename]):
        logging.error("Missing session data for matching")
        return redirect(url_for('home'))

    df = pd.read_parquet(prepared_path)

    if 'PracticeName' in df.columns:
        df['Name'] = df['PracticeName']
    if 'Addr1' in df.columns:
        df['Address'] = df['Addr1']
    df.drop(columns=['Addr1', 'PracticeName'], inplace=True, errors='ignore')

    for col in ['Name', 'Address', 'State', 'Zip', 'Emails', 'Doctors', 'ExternalID', 'City']:
        if col not in df.columns:
            df[col] = ''

    df['Source'] = dso_name
    
    # Check for already approved SourceIDs and flag them instead of filtering
    approved_source_ids = get_approved_source_ids()
    initial_count = len(df)
    approved_count = 0
    
    # Add AlreadyApproved flag column
    df['AlreadyApproved'] = False
    
    if 'SourceID' in df.columns and approved_source_ids:
        # Convert SourceID to string for comparison
        df['SourceID'] = df['SourceID'].astype(str)
        
        # Flag records that are already approved instead of filtering them out
        df['AlreadyApproved'] = df['SourceID'].isin(approved_source_ids)
        approved_count = df['AlreadyApproved'].sum()
        
        logging.info(f" Flagged {approved_count} already approved records.")
    
    # Separate approved and non-approved records for matching
    approved_records = df[df['AlreadyApproved'] == True].copy()
    records_to_match = df[df['AlreadyApproved'] == False].copy()
    
    # If there are records to match, run the matching process
    if not records_to_match.empty:
        customer_df = get_customer_data()
        matched_df = match_records_by_fields(records_to_match, customer_df, min_score=2)
    else:
        # If no records to match, create empty matched dataframe with same structure
        matched_df = records_to_match.copy()
        # Add matching columns with default values
        for col in ['MatchedName', 'MatchedEmails', 'MatchedAddress', 'MatchedDoctors', 'TotalScore', 'MatchedEntityID', 'MatchedPracticeName']:
            matched_df[col] = 0.0 if col in ['MatchedName', 'MatchedEmails', 'MatchedAddress', 'MatchedDoctors', 'TotalScore'] else ''
    
    # For already approved records, set matching columns to indicate they're pre-approved
    if not approved_records.empty:
        for col in ['MatchedName', 'MatchedEmails', 'MatchedAddress', 'MatchedDoctors', 'TotalScore']:
            approved_records[col] = 0.0
        for col in ['MatchedEntityID', 'MatchedPracticeName']:
            approved_records[col] = 'PRE-APPROVED'
    
    # Combine approved and matched records
    if not approved_records.empty and not matched_df.empty:
        final_df = pd.concat([matched_df, approved_records], ignore_index=True)
    elif not approved_records.empty:
        final_df = approved_records
    else:
        final_df = matched_df

    final_df = final_df.loc[:, ~final_df.columns.duplicated()]
    final_df['FileName'] = original_filename
    final_df['UploadedDate'] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    try:
        delete_matched_data_for_dso(dso_name)
        upload_to_datalake(final_df)
        logging.info(f" Uploaded {len(final_df)} records to Databricks ({approved_count} pre-approved, {len(final_df) - approved_count} newly matched).")
    except Exception as e:
        logging.error(f" Upload failed: {e}")
        return f"Upload failed: {e}", 500

    for path in [session.get('prepared_data_path')]:
        if path and os.path.exists(path):
            os.remove(path)

    return render_template('success.html', 
                          message=f"Matching complete and data uploaded to Databricks.",
                          stats={
                              'total': initial_count,
                              'approved': approved_count,
                              'matched': len(final_df) - approved_count
                          })

@app.route('/setup')
def setup():
    data_df = get_dso_config_data()
    data = data_df.to_dict(orient='records')
    return render_template('setup.html', data=data)

@app.route('/setup/add', methods=['GET', 'POST'])
def setup_add():
    if request.method == 'POST':
        new_dso = {k: v for k, v in request.form.items()}
        insert_or_update_dso_config(new_dso)
        return redirect(url_for('setup'))

    dso_df = get_dso_dropdown_options()
    dso_list = dso_df.to_dict(orient='records')
    columns = list(get_dso_config_data().columns)
    return render_template('setup_add.html', dso_list=dso_list, columns=columns)

@app.route('/setup/edit/<org_id>', methods=['GET', 'POST'])
def setup_edit(org_id):
    data_df = get_dso_config_data()
    data = data_df.to_dict(orient='records')
    org = next((d for d in data if str(d["NSEntityID"]) == org_id), None)

    if not org:
        return "DSO not found", 404

    if request.method == 'POST':
        updated_dso = {k: v for k, v in request.form.items()}
        insert_or_update_dso_config(updated_dso)
        return redirect(url_for('setup'))

    dso_df = get_dso_dropdown_options()
    dso_list = dso_df.to_dict(orient='records')
    columns = list(data_df.columns)

    return render_template('setup_edit.html', org=org, dso_list=dso_list, columns=columns)

@app.route('/setup/delete/<org_id>', methods=['POST'])
def setup_delete(org_id):
    data = load_dso_data()
    data = [d for d in data if d["NSEntityID"] != org_id]
    save_dso_data(data)
    return redirect(url_for('setup'))

if __name__ == '__main__':
    app.run(debug=True)

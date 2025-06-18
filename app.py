import os
import re
import uuid
import logging
import json
import tempfile
import pandas as pd
from flask import Flask, request, render_template, redirect, url_for, session
from werkzeug.utils import secure_filename
from databricks_conn import get_customer_data, upload_to_datalake ,get_dso_dropdown_options,insert_or_update_dso_config, get_dso_config_data
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
def home():
    return render_template('home.html')

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
        df = pd.read_excel(file, dtype=str, header=dso_header).fillna('')
        df.columns = df.columns.str.strip()

        # Extract valid mappings (ignore metadata keys)
        cleaned_mapping = {
            k: v for k, v in config_entry.items()
            if k not in ['ID', 'Name', 'NSEntityID', 'Type', 'Header', 'Concat_Doctor']
            and isinstance(v, str) and v.strip()
        }

        try:
            df = df[list(cleaned_mapping.values())]
        except KeyError as e:
            return f"Missing expected column(s): {e}", 400

        df.columns = list(cleaned_mapping.keys())

        # Handle doctor name concatenation if configured
        if concat_dr and all(col in df.columns for col in concat_dr):
            df['Doctors'] = df[concat_dr[0]].fillna('') + ' ' + df[concat_dr[1]].fillna('')

        # Combine email and add_email into one 'Emails' column
        if 'Emails' in df.columns and 'AddEmail' in df.columns:
            df['Emails'] = df['Emails'].astype(str) + ',' + df['AddEmail'].astype(str)
        elif 'AddEmail' in df.columns:
            df['Emails'] = df['AddEmail'].astype(str)

        # Add metadata columns
        df['Source'] = dso_name
        df['DSO_Id'] = config_entry.get("NSEntityID", '')
        df['Type'] = config_entry.get("Type", '')

        # Generate SourceID if not already present
        if 'SourceID' not in df.columns and 'PracticeName' in df.columns and 'Addr1' in df.columns:
            df['SourceID'] = df['PracticeName'].astype(str) + ' | ' + df['Addr1'].astype(str)

        # Save prepared file temporarily
        temp_path = os.path.join(tempfile.gettempdir(), f"{uuid.uuid4()}.parquet")
        df.to_parquet(temp_path)
        session['prepared_data_path'] = temp_path

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

    for col in ['APEmail', 'OfficeEmail']:
        if col not in df.columns:
            df[col] = ''
    df['Emails'] = df[['APEmail', 'OfficeEmail']].apply(
        lambda row: ', '.join(filter(None, row.astype(str).str.strip())), axis=1
    )
    df.drop(columns=['APEmail', 'OfficeEmail'], inplace=True)

    for col in ['Name', 'Address', 'State', 'Zip', 'Emails', 'Doctors', 'ExternalID', 'City']:
        if col not in df.columns:
            df[col] = ''

    df['Source'] = dso_name

    customer_df = get_customer_data()
    matched_df = match_records_by_fields(df, customer_df, min_score=2)

    matched_df = matched_df.loc[:, ~matched_df.columns.duplicated()]
    matched_df['FileName'] = original_filename
    matched_df['UploadedDate'] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    result_path = os.path.join(tempfile.gettempdir(), f"{uuid.uuid4()}.parquet")
    matched_df.to_parquet(result_path)
    session['results_filepath'] = result_path

    return render_template(
        "results.html",
        tables=[matched_df.to_html(classes='table table-striped', index=False)],
        titles=matched_df.columns.values
    )

@app.route('/send-to-datalake', methods=['POST'])
def send_to_datalake():
    results_filepath = session.get('results_filepath')
    if not results_filepath:
        return "No matched data found to upload.", 404

    try:
        matched_df_to_upload = pd.read_parquet(results_filepath)
        logging.info(f"Uploading {len(matched_df_to_upload)} records to Datalake.")
        upload_to_datalake(matched_df_to_upload)

        for path in [results_filepath, session.get('prepared_data_path')]:
            if path and os.path.exists(path):
                os.remove(path)

        return "Data uploaded to Databricks successfully!"
    except Exception as e:
        logging.error(f"Error uploading to Data Lake: {e}")
        return f"Upload failed: {e}", 500

# --- DSO Setup Routes ---
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
    data = load_dso_data()
    org = next((d for d in data if d["NSEntityID"] == org_id), None)
    if not org:
        return "DSO not found", 404

    if request.method == 'POST':
        for k in org:
            if k in request.form:
                org[k] = request.form[k]
        save_dso_data(data)
        return redirect(url_for('setup'))

    return render_template('setup_edit.html', org=org)

@app.route('/setup/delete/<org_id>', methods=['POST'])
def setup_delete(org_id):
    data = load_dso_data()
    data = [d for d in data if d["NSEntityID"] != org_id]
    save_dso_data(data)
    return redirect(url_for('setup'))

if __name__ == '__main__':
    app.run(debug=True)

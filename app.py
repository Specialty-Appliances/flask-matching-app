import os
import re
import uuid
import logging
from datetime import datetime

import pandas as pd
from flask import (Flask, request, render_template, redirect, url_for,
                   session)
from werkzeug.utils import secure_filename

# Make sure these local files exist and are correct
from databricks_conn import (get_customer_data, get_dso_list,
                             upload_to_datalake)
from match_logic import match_records_by_fields

# --- Configuration ---
UPLOAD_FOLDER = 'temp_uploads'
ALLOWED_EXTENSIONS = {'csv', 'xlsx', 'json'}

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
# REQUIRED for session to work. A random key is generated on each start.
app.secret_key = os.urandom(24)

# Create the upload folder if it doesn't exist
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
logging.basicConfig(level=logging.INFO)


# --- Helper Functions ---

def allowed_file(filename):
    """Checks if a file has an allowed extension."""
    return '.' in filename and \
        filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def load_file_by_extension(filepath, ext):
    """Loads a file into a pandas DataFrame based on its extension."""
    try:
        if ext == 'csv':
            return pd.read_csv(filepath, dtype=str).fillna('')
        elif ext == 'xlsx':
            return pd.read_excel(filepath, dtype=str).fillna('')
        elif ext == 'json':
            return pd.read_json(filepath, dtype=str).fillna('')
        else:
            raise ValueError("Unsupported file type")
    except Exception as e:
        raise RuntimeError(f"Error reading file: {e}")


def clean_and_dedup(values):
    distinct_parts = set()

    for val in values:
        if not val or not isinstance(val, str):
            continue

        # Split by comma, semicolon, or space
        parts = re.split(r'[;, ]+', val.strip())

        for part in parts:
            cleaned_part = part.strip()
            if cleaned_part:
                distinct_parts.add(cleaned_part)

    return ", ".join(sorted(distinct_parts))



# --- Flask Routes ---

@app.route('/')
def index():
    """Displays the main file upload page."""
    dso_list = get_dso_list()
    return render_template('upload.html', dso_names=dso_list)


@app.route('/upload', methods=['POST'])
def upload_file():
    """Handles the initial file upload."""
    if 'file' not in request.files:
        return "No file part in the request.", 400
    file = request.files['file']
    if file.filename == '':
        return "No file selected.", 400

    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        # Store file info in the user's session
        session['original_filename'] = filename
        session['dso'] = request.form['dso']

        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)

        return redirect(url_for('map_columns'))

    return "Invalid file format.", 400


@app.route('/map-columns')
def map_columns():
    """Displays the column mapping interface."""
    original_filename = session.get('original_filename')
    dso_name = session.get('dso')

    if not original_filename:
        return redirect(url_for('index'))

    filepath = os.path.join(app.config['UPLOAD_FOLDER'], original_filename)
    ext = original_filename.rsplit('.', 1)[1].lower()
    df = load_file_by_extension(filepath, ext)

    # Store filepath in session to use in the next step
    session['original_filepath'] = filepath

    sample_data = {col: df[col].dropna().head(5).tolist() for col in df.columns}
    return render_template(
        'map_columns.html',
        columns=df.columns,
        sample_data=sample_data,
        dso_name=dso_name
    )


@app.route('/run-matching', methods=['POST'])
def run_matching():
    """Processes mappings, runs matching logic, and shows results."""
    dso_name = request.form.get('dso_name')
    original_filepath = session.get('original_filepath')
    original_filename = session.get('original_filename')

    if not all([dso_name, original_filepath, original_filename]):
        logging.error("Session information missing in /run-matching")
        return redirect(url_for('index'))

    ext = original_filename.rsplit('.', 1)[1].lower()

    # Build a map of {standard_field: [user_column_1, user_column_2]}
    reverse_map = {}
    for user_column_name, standard_field_name in request.form.items():
        if user_column_name in ['dso_name', 'temp_filename'] or \
                standard_field_name == "(Ignore this column)":
            continue

        # --- THIS IS THE FIX ---
        # We now strip whitespace from the user column name to match the
        # cleaned DataFrame column names.
        cleaned_user_column = user_column_name.strip()
        reverse_map.setdefault(standard_field_name, []).append(cleaned_user_column)

    df = load_file_by_extension(original_filepath, ext)
    # This line cleans the DataFrame columns. Our fix above makes sure
    # the keys in reverse_map match these cleaned names.
    df.columns = df.columns.str.strip()

    final_df = pd.DataFrame()
    for field, columns in reverse_map.items():
        # This part will now work because the `columns` list contains
        # cleaned names that exist in the `df.columns` index.
        final_df[field] = df[columns].fillna('').agg(clean_and_dedup, axis=1)

    required_fields = [
        'Name', 'Address', 'State', 'Zip', 'Emails', 'Doctors', 'ExternalID', 'City'
    ]
    for field in required_fields:
        if field not in final_df.columns:
            final_df[field] = ''

    final_df['Source'] = dso_name

    # Run the matching logic
    customer_df = get_customer_data()
    matched_df = match_records_by_fields(final_df, customer_df, min_score=2)

    if not matched_df.empty:
        matched_df['FileName'] = original_filename
        matched_df['UploadedDate'] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        # Save results to a temporary file
        results_filename = f"{uuid.uuid4()}.parquet"
        results_filepath = os.path.join(app.config['UPLOAD_FOLDER'], results_filename)
        matched_df.to_parquet(results_filepath)
        session['results_filepath'] = results_filepath
    else:
        session['results_filepath'] = None

    return render_template(
        "results.html",
        tables=[matched_df.to_html(classes='table table-striped', index=False)],
        titles=matched_df.columns.values
    )


@app.route('/send-to-datalake', methods=['POST'])
def send_to_datalake():
    """Handles sending the final matched data to the datalake."""
    results_filepath = session.get('results_filepath')

    if not results_filepath:
        return "No matched data found to upload.", 404

    try:
        matched_df_to_upload = pd.read_parquet(results_filepath)
        logging.info(f"Uploading {len(matched_df_to_upload)} records to Datalake.")
        upload_to_datalake(matched_df_to_upload)

        # Clean up temporary files
        os.remove(results_filepath)
        if session.get('original_filepath'):
            try:
                os.remove(session.get('original_filepath'))
            except OSError as e:
                logging.error(f"Error removing original file: {e}")

        return "Data uploaded to Databricks successfully!"
    except Exception as e:
        logging.error(f"Error uploading to Data Lake: {e}")
        return f"Upload failed: {e}", 500


if __name__ == '__main__':
    # Set debug=False for a production environment
    app.run(debug=True)

import os
import pandas as pd
import logging
from datetime import datetime
from flask import Flask, request, render_template, redirect, url_for
from werkzeug.utils import secure_filename
from match_logic import match_records_by_fields
from databricks_conn import get_customer_data, get_dso_list, upload_to_datalake
print("🚀 Flask app starting...")

UPLOAD_FOLDER = 'temp_uploads'
ALLOWED_EXTENSIONS = {'csv', 'xlsx', 'json'}
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
uploaded_date = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


uploaded_df = None
matched_df = None

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def load_file_by_extension(filepath, ext):
    try:
        if ext == 'csv':
            try:
                return pd.read_csv(filepath, dtype=str, encoding='utf-8').fillna('')
            except UnicodeDecodeError:
                return pd.read_csv(filepath, dtype=str, encoding='ISO-8859-1').fillna('')
        elif ext == 'xlsx':
            return pd.read_excel(filepath, dtype=str).fillna('')
        elif ext == 'json':
            return pd.read_json(filepath, dtype=str).fillna('')
        else:
            raise ValueError("Unsupported file type")
    except Exception as e:
        raise RuntimeError(f"Error reading file: {e}")

@app.route('/')
def index():
    dso_list = get_dso_list()
    return render_template('upload.html', dso_names=dso_list)

@app.route('/upload', methods=['POST'])
def upload_file():
    file = request.files['file']
    dso = request.form['dso']

    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)

        with open(os.path.join(app.config['UPLOAD_FOLDER'], 'filename.txt'), 'w') as f:
            f.write(filename)
        with open(os.path.join(app.config['UPLOAD_FOLDER'], 'dso.txt'), 'w') as f:
            f.write(dso)

        return redirect(url_for('map_columns'))
    return "Invalid file format."

@app.route('/map-columns')
def map_columns():
    with open(os.path.join(app.config['UPLOAD_FOLDER'], 'filename.txt')) as f:
        temp_filename = f.read().strip()
    with open(os.path.join(app.config['UPLOAD_FOLDER'], 'dso.txt')) as f:
        dso_name = f.read().strip()

    filepath = os.path.join(app.config['UPLOAD_FOLDER'], temp_filename)
    ext = temp_filename.rsplit('.', 1)[1].lower()
    df = load_file_by_extension(filepath, ext)

    sample_data = {col: df[col].dropna().head(5).tolist() for col in df.columns}
    return render_template('map_columns.html', columns=df.columns, sample_data=sample_data, temp_filename=temp_filename, dso_name=dso_name)

@app.route('/run-matching', methods=['POST'])
def run_matching():
    global uploaded_df, matched_df

    temp_filename = request.form.get('temp_filename')
    dso_name = request.form.get('dso_name')
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], temp_filename)
    ext = temp_filename.rsplit('.', 1)[1].lower()

    user_column_to_standard_map = {
        k: v for k, v in request.form.items()
        if k not in ['temp_filename', 'dso_name'] and v != "(Ignore this column)"
    }

    try:
        df = load_file_by_extension(filepath, ext)
        df = df.rename(columns=user_column_to_standard_map)

        required_fields = ['Name', 'Address', 'State', 'Zip', 'Emails', 'Doctors', 'ExternalID']
        for field in required_fields:
            if field not in df.columns:
                df[field] = None

        df = df[required_fields]
        df['Source'] = dso_name
        df.attrs['original_filename'] = temp_filename

        uploaded_df = df
        customer_df = get_customer_data()
        matched_df = match_records_by_fields(uploaded_df, customer_df, min_score=2)
        matched_df['FileName'] = temp_filename
        matched_df['UploadedDate'] = uploaded_date        # <- Fix line
        matched_df.to_csv("temp_uploads/matched_results.csv", index=False)


        return render_template("results.html", tables=[matched_df.to_html(classes='table table-striped', index=False)], titles=matched_df.columns.values)

    except Exception as e:
        logging.error(f"Match error: {e}")
        return f"\u274c Match error: {e}", 500


@app.route('/send-to-datalake', methods=['POST'])
def send_to_datalake():
    try:
        matched_df = pd.read_csv("temp_uploads/matched_results.csv")
        matched_df = matched_df.fillna('')
        matched_df['FileName'] = matched_df.attrs.get('original_filename', 'unknown.csv')
        upload_to_datalake(matched_df)
        return " Data uploaded to Databricks!"
    except Exception as e:
        logging.error(f"Error uploading to Data Lake: {e}")
        return f" Upload failed: {e}", 500


if __name__ == '__main__':
    print("Flask run starting...")
    app.run(debug=True)
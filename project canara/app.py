from flask import Flask, request, send_file, jsonify
from werkzeug.utils import secure_filename
import subprocess
import os
import uuid
import json
import pandas as pd
import yaml
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Load pipeline config
with open("config.yaml", "r") as f:
    CONFIG = yaml.safe_load(f)

TABULA_JAR_PATH = "tabula.jar"
EXPORT_FOLDER = "exports"
TEMP_FOLDER = "temp"
os.makedirs(EXPORT_FOLDER, exist_ok=True)
os.makedirs(TEMP_FOLDER, exist_ok=True)

def validate_template(template_path):
    try:
        with open(template_path, 'r') as f:
            data = json.load(f)
        if not isinstance(data, list):
            return False, "Template must be a JSON array"
        for item in data:
            if not all(k in item for k in ('x1', 'y1', 'x2', 'y2')):
                return False, "Missing one of x1, y1, x2, y2 in template"
        return True, "Valid template"
    except Exception as e:
        return False, f"Template validation error: {e}"

def convert_template_to_areas(template_path):
    with open(template_path, 'r') as f:
        data = json.load(f)

    area_groups = {}
    for item in data:
        y1, x1, y2, x2 = item['y1'], item['x1'], item['y2'], item['x2']
        page = item.get("page", 1)
        key = f"{y1},{x1},{y2},{x2}"
        area_groups.setdefault(key, []).append(page)
    return area_groups

def run_script_with_logging(script_name, script_path, input_file, output_file):
    try:
        logger.info(f"\n🚀 Running {script_name}...")
        logger.info(f"Command: python {script_path} {input_file} {output_file}")

        result = subprocess.run(
            ["python", script_path, input_file, output_file],
            capture_output=True,
            text=True,
            timeout=300
        )

        if result.stdout:
            logger.info(f"{script_name} stdout:\n{result.stdout.strip()}")
        if result.stderr:
            logger.warning(f"{script_name} stderr:\n{result.stderr.strip()}")

        if result.returncode != 0:
            return False, f"{script_name} failed with code {result.returncode}. STDERR:\n{result.stderr}"

        if not os.path.exists(output_file):
            return False, f"{script_name} did not produce expected output: {output_file}"

        output_size = os.path.getsize(output_file)
        if output_size == 0:
            return False, f"{script_name} produced empty file: {output_file}"

        logger.info(f"✅ {script_name} completed. Output size: {output_size} bytes")
        return True, f"{script_name} completed successfully"

    except subprocess.TimeoutExpired:
        return False, f"{script_name} timed out after 5 minutes"
    except FileNotFoundError:
        return False, f"Script not found: {script_path}"
    except Exception as e:
        logger.exception(f"⚠️ Unexpected exception in {script_name}")
        return False, f"Unexpected error in {script_name}: {str(e)}"

@app.route("/extract", methods=["POST"])
def extract_and_process():
    if 'file' not in request.files:
        return jsonify({"error": "No PDF file provided"}), 400

    pdf_file = request.files['file']
    if pdf_file.filename == '':
        return jsonify({"error": "Empty filename"}), 400

    filename = secure_filename(pdf_file.filename)
    temp_pdf_path = os.path.join(TEMP_FOLDER, f"{uuid.uuid4().hex}_{filename}")
    pdf_file.save(temp_pdf_path)

    # Load paths from config
    extracted_csv = CONFIG['paths']['intermediate_csv']
    cleaned_csv = CONFIG['paths']['cleaned_csv']
    consolidated_csv = CONFIG['paths']['consolidated_csv']
    final_csv = CONFIG['paths']['final_csv']

    temp_template_path = None
    area_groups = {}

    template_file = request.files.get("template")
    if template_file:
        temp_template_path = os.path.join(TEMP_FOLDER, f"{uuid.uuid4().hex}.json")
        template_file.save(temp_template_path)
        valid, msg = validate_template(temp_template_path)
        if not valid:
            os.remove(temp_template_path)
            return jsonify({"error": f"Invalid template: {msg}"}), 400
        area_groups = convert_template_to_areas(temp_template_path)

    try:
        logger.info("[1/4] Extracting tables from PDF using Tabula...")
        combined_data = ""

        for area, pages in area_groups.items() if area_groups else [("", ["all"])]:
            extract_cmd = [
                "java", "-jar", TABULA_JAR_PATH,
                "-f", "CSV",
                "-p", ",".join(map(str, pages))
            ]

            if area:
                extract_cmd += ["-a", area]

            if CONFIG['api'].get("auto_detect", False) and not area_groups:
                extract_cmd.append("-g")

            mode = CONFIG['api'].get("extraction_mode", "stream")
            extract_cmd.append("-l" if mode == "lattice" else "-t")

            extract_cmd.append(temp_pdf_path)
            result = subprocess.run(extract_cmd, capture_output=True, text=True)
            if result.returncode != 0:
                return jsonify({"error": f"Tabula extraction failed: {result.stderr}"}), 500

            combined_data += result.stdout

        with open(extracted_csv, "w", encoding="utf-8") as f:
            f.write(combined_data)

        # Pipeline scripts
        pipeline = [
            ("Clean CSV", CONFIG['scripts']['clean_csv'], extracted_csv, cleaned_csv),
            ("Consolidate", CONFIG['scripts']['consolidate'], cleaned_csv, consolidated_csv),
            ("Add Branch Code", CONFIG['scripts']['branchcode'], consolidated_csv, final_csv)
        ]

        for idx, (name, script_path, input_file, output_file) in enumerate(pipeline, start=2):
            logger.info(f"[{idx}/4] {name}...")
            if not os.path.exists(input_file):
                return jsonify({"error": f"Missing input: {input_file}", "step": name}), 500

            success, message = run_script_with_logging(name, script_path, input_file, output_file)
            if not success:
                return jsonify({"error": message, "step": name}), 500

        if os.path.exists(final_csv):
            df_preview = pd.read_csv(final_csv, nrows=5)
            logger.info(f"Final CSV preview:\n{df_preview.to_string(index=False)}")

        logger.info("✅ Full pipeline successful.")
        return send_file(final_csv, as_attachment=True)

    except Exception as e:
        logger.exception("Unexpected error in pipeline")
        return jsonify({"error": str(e), "type": type(e).__name__}), 500

    finally:
        try:
            if os.path.exists(temp_pdf_path):
                os.remove(temp_pdf_path)
            if temp_template_path and os.path.exists(temp_template_path):
                os.remove(temp_template_path)
        except Exception as cleanup_err:
            logger.warning(f"Cleanup failed: {cleanup_err}")

@app.route("/health", methods=["GET"])
def health_check():
    return jsonify({
        "status": "healthy",
        "timestamp": pd.Timestamp.now().isoformat(),
        "exports_folder_exists": os.path.exists(EXPORT_FOLDER),
        "temp_folder_exists": os.path.exists(TEMP_FOLDER),
        "tabula_jar_exists": os.path.exists(TABULA_JAR_PATH)
    })

if __name__ == "__main__":
    debug_mode = os.environ.get("FLASK_DEBUG", "false").lower() == "true"
    port = int(os.environ.get("PORT", 5000))
    app.run(debug=debug_mode, port=port, host="0.0.0.0")

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
    """Run a script and capture both stdout and stderr for better error reporting"""
    try:
        logger.info(f"Running {script_name}...")
        logger.info(f"Command: python {script_path} {input_file} {output_file}")
        
        result = subprocess.run(
            ["python", script_path, input_file, output_file], 
            capture_output=True, 
            text=True,
            timeout=300  # 5 minute timeout
        )
        
        if result.stdout:
            logger.info(f"{script_name} stdout: {result.stdout}")
        
        if result.stderr:
            logger.warning(f"{script_name} stderr: {result.stderr}")
            
        if result.returncode != 0:
            error_msg = f"{script_name} failed with return code {result.returncode}"
            if result.stderr:
                error_msg += f". Error: {result.stderr}"
            if result.stdout:
                error_msg += f". Output: {result.stdout}"
            return False, error_msg
            
        return True, f"{script_name} completed successfully"
        
    except subprocess.TimeoutExpired:
        return False, f"{script_name} timed out after 5 minutes"
    except FileNotFoundError:
        return False, f"Script not found: {script_path}"
    except Exception as e:
        return False, f"Unexpected error running {script_name}: {str(e)}"

@app.route("/extract", methods=["POST"])
def extract_and_process():
    if 'file' not in request.files:
        return jsonify({"error": "No PDF file provided"}), 400

    pdf_file = request.files['file']
    if pdf_file.filename == '':
        return jsonify({"error": "Empty PDF filename"}), 400

    filename = secure_filename(pdf_file.filename)
    temp_pdf_path = os.path.join(TEMP_FOLDER, f"{uuid.uuid4().hex}_{filename}")
    pdf_file.save(temp_pdf_path)

    # Paths from config
    extracted_csv = CONFIG['paths']['intermediate_csv']
    cleaned_csv = CONFIG['paths']['cleaned_csv']
    merged_csv = CONFIG['paths']['merged_csv']
    shifted_csv = CONFIG['paths']['shifted_csv']
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
        logger.info("[1/5] Extracting tables from PDF...")
        combined_data = ""

        for i, (area, pages) in enumerate(area_groups.items() if area_groups else [("", ["all"])]):
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
            if mode == "lattice":
                extract_cmd.append("-l")
            elif mode == "stream":
                extract_cmd.append("-t")

            extract_cmd.append(temp_pdf_path)
            result = subprocess.run(extract_cmd, capture_output=True, text=True)
            if result.returncode != 0:
                return jsonify({"error": f"PDF extraction failed: {result.stderr}"}), 500

            combined_data += result.stdout

        with open(extracted_csv, "w", encoding="utf-8") as out_csv:
            out_csv.write(combined_data)

        # Run the pipeline scripts with better error handling
        pipeline_steps = [
            ("Clean CSV", CONFIG['scripts']['clean_csv'], extracted_csv, cleaned_csv),
            ("Merge Narration", CONFIG['scripts']['merge_narration'], cleaned_csv, merged_csv),
            ("Shift HDFC", CONFIG['scripts']['shift_hdfc'], merged_csv, shifted_csv),
            ("Closing Balance", CONFIG['scripts']['closing_balance'], shifted_csv, final_csv)
        ]

        for step_num, (step_name, script_path, input_file, output_file) in enumerate(pipeline_steps, 2):
            logger.info(f"[{step_num}/5] {step_name}...")
            
            # Check if input file exists
            if not os.path.exists(input_file):
                return jsonify({
                    "error": f"Input file for {step_name} does not exist: {input_file}",
                    "step": step_name,
                    "step_number": step_num
                }), 500
            
            success, message = run_script_with_logging(step_name, script_path, input_file, output_file)
            
            if not success:
                return jsonify({
                    "error": message,
                    "step": step_name,
                    "step_number": step_num,
                    "input_file": input_file,
                    "output_file": output_file
                }), 500
            
            # Check if output file was created
            if not os.path.exists(output_file):
                return jsonify({
                    "error": f"{step_name} completed but output file was not created: {output_file}",
                    "step": step_name,
                    "step_number": step_num
                }), 500
            
            logger.info(f"✅ {step_name} completed successfully")

        logger.info("✅ Pipeline completed successfully.")
        
        # Check final file exists and has content
        if not os.path.exists(final_csv):
            return jsonify({"error": "Final CSV file was not created"}), 500
            
        file_size = os.path.getsize(final_csv)
        if file_size == 0:
            return jsonify({"error": "Final CSV file is empty"}), 500
            
        logger.info(f"Final CSV created successfully: {final_csv} ({file_size} bytes)")
        return send_file(final_csv, as_attachment=True)

    except Exception as e:
        logger.exception("Unexpected error occurred")
        return jsonify({
            "error": f"Unexpected error: {str(e)}",
            "type": type(e).__name__
        }), 500

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
    """Health check endpoint to verify the service is running"""
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
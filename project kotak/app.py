from flask import Flask, request, send_file, jsonify
import subprocess
import os
import uuid
import json
import pandas as pd
import yaml

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

@app.route("/extract", methods=["POST"])
def extract_and_process():
    if 'file' not in request.files:
        return jsonify({"error": "No PDF file provided"}), 400

    pdf_file = request.files['file']
    if pdf_file.filename == '':
        return jsonify({"error": "Empty PDF filename"}), 400

    temp_pdf_path = os.path.join(TEMP_FOLDER, f"{uuid.uuid4().hex}.pdf")
    pdf_file.save(temp_pdf_path)

    extracted_csv = CONFIG['paths']['intermediate_csv']
    cleaned_csv = CONFIG['paths']['cleaned_csv']
    merged_csv = CONFIG['paths']['merged_csv']
    final_csv = CONFIG['paths']['final_csv']
    processed_csv = CONFIG['paths']['processed_csv']

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
        print("[1/5] Extracting tables from PDF...")
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
                return jsonify({"error": result.stderr}), 500

            combined_data += result.stdout

        with open(extracted_csv, "w", encoding="utf-8") as out_csv:
            out_csv.write(combined_data)

        print("[2/5] Running clean_csv.py...")
        subprocess.run(["python", CONFIG['scripts']['clean_csv'], extracted_csv, cleaned_csv], check=True)

        print("[3/5] Running mergenarr.py...")
        subprocess.run(["python", CONFIG['scripts']['merge_narration'], cleaned_csv, merged_csv], check=True)

        print("[4/5] Running split.py...")
        subprocess.run(["python", CONFIG['scripts']['split_csv'], merged_csv, final_csv], check=True)

        print("[5/5] Running delete_empt_row.py...")
        subprocess.run(["python", CONFIG['scripts']['delete_empt_row'], final_csv, processed_csv], check=True)

        print("✅ Pipeline completed successfully.")
        return send_file(processed_csv, as_attachment=True)

    except subprocess.CalledProcessError as e:
        return jsonify({"error": f"Subprocess failed: {e}"}), 500

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        if os.path.exists(temp_pdf_path):
            os.remove(temp_pdf_path)
        if temp_template_path and os.path.exists(temp_template_path):
            os.remove(temp_template_path)
            
            
if __name__ == "__main__":
    app.run(debug=True, port=5000)
            
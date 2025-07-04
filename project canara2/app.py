from flask import Flask, request, send_file, jsonify
import subprocess
import os
import uuid
import pandas as pd
import yaml

app = Flask(__name__)

# Load pipeline config
with open("config.yaml", "r") as f:
    CONFIG = yaml.safe_load(f)

EXPORT_FOLDER = "exports"
TEMP_FOLDER = "temp"
os.makedirs(EXPORT_FOLDER, exist_ok=True)
os.makedirs(TEMP_FOLDER, exist_ok=True)

@app.route("/process-csv", methods=["POST"])
def process_csv():
    """
    Process CSV file through the pipeline:
    1. Upload CSV file
    2. Run 01_empty_column_ident.py
    3. Run 02_consolidate.py
    4. Return processed CSV
    """
    if 'file' not in request.files:
        return jsonify({"error": "No CSV file provided"}), 400

    csv_file = request.files['file']
    if csv_file.filename == '':
        return jsonify({"error": "Empty CSV filename"}), 400

    # Check if file is CSV
    if not csv_file.filename.lower().endswith('.csv'):
        return jsonify({"error": "Please upload a CSV file"}), 400

    # Save uploaded file to temp directory
    temp_csv_path = os.path.join(TEMP_FOLDER, f"{uuid.uuid4().hex}.csv")
    csv_file.save(temp_csv_path)

    # Define pipeline file paths
    empty_column_output = CONFIG['paths']['empty_column_output']
    consolidated_output = CONFIG['paths']['consolidated_output']

    try:
        print("[1/2] Running empty column identification...")
        
        # Run 01_empty_column_ident.py
        empty_col_cmd = [
            "python", 
            CONFIG['scripts']['empty_column_ident'],
            temp_csv_path,
            empty_column_output
        ]
        
        result = subprocess.run(empty_col_cmd, capture_output=True, text=True)
        if result.returncode != 0:
            return jsonify({
                "error": f"Empty column identification failed: {result.stderr}",
                "stdout": result.stdout
            }), 500
            
        print(f"✅ Empty column identification completed. Output: {result.stdout}")

        print("[2/2] Running consolidation...")
        
        # Run 02_consolidate.py
        consolidate_cmd = [
            "python",
            CONFIG['scripts']['consolidate'],
            empty_column_output,
            consolidated_output
        ]
        
        result = subprocess.run(consolidate_cmd, capture_output=True, text=True)
        if result.returncode != 0:
            return jsonify({
                "error": f"Consolidation failed: {result.stderr}",
                "stdout": result.stdout
            }), 500
            
        print(f"✅ Consolidation completed. Output: {result.stdout}")

        print("✅ Pipeline completed successfully.")
        
        # Check if final output file exists
        if not os.path.exists(consolidated_output):
            return jsonify({"error": "Final output file was not created"}), 500
            
        # Return the processed CSV file
        return send_file(
            consolidated_output, 
            as_attachment=True,
            download_name=f"processed_{csv_file.filename}"
        )

    except subprocess.CalledProcessError as e:
        return jsonify({
            "error": f"Subprocess failed: {e}",
            "command": e.cmd,
            "returncode": e.returncode
        }), 500

    except Exception as e:
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 500

    finally:
        # Clean up temp file
        if os.path.exists(temp_csv_path):
            os.remove(temp_csv_path)

@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "pipeline": "CSV Processing Pipeline",
        "scripts": {
            "empty_column_ident": CONFIG['scripts']['empty_column_ident'],
            "consolidate": CONFIG['scripts']['consolidate']
        }
    })

@app.route("/config", methods=["GET"])
def get_config():
    """Get current configuration"""
    return jsonify(CONFIG)

@app.errorhandler(413)
def too_large(e):
    return jsonify({"error": "File too large"}), 413

@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": "Endpoint not found"}), 404

@app.errorhandler(500)
def internal_error(e):
    return jsonify({"error": "Internal server error"}), 500

if __name__ == "__main__":
    # Set max file size (16MB)
    app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024
    
    print("Starting CSV Processing Pipeline API...")
    print(f"Configuration loaded: {CONFIG}")
    print("Available endpoints:")
    print("  POST /process-csv - Process CSV file through pipeline")
    print("  GET /health - Health check")
    print("  GET /config - View configuration")
    
    app.run(debug=True, host='0.0.0.0', port=5000)
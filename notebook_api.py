import sys
import os
from flask import Flask, jsonify, request
from flask_cors import CORS

sys.path.append(os.path.abspath("python_scripts"))
from python_scripts.main import run_pipeline

app = Flask(__name__)
CORS(app)


def update_config_and_cleanup(file_name):
    import yaml
    import shutil
    config_path = os.path.join(os.path.dirname(__file__), 'config/config.yaml')
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    config['report']['name'] = file_name.strip('.twb')
    with open(config_path, 'w') as f:
        yaml.safe_dump(config, f)

    # Delete existing log folder for this report
    log_path = config['paths']['log_path']
    report_type = config['report']['type']
    report_name = config['report']['name']
    from datetime import datetime
    today_str = datetime.now().strftime('%Y-%m-%d')
    log_dir = os.path.join(log_path, report_type, today_str, report_name)

    # Close all logging handlers before deleting log folder
    import logging
    for handler in logging.root.handlers[:]:
        try:
            handler.close()
        except Exception:
            pass
        logging.root.removeHandler(handler)
    if os.path.exists(log_dir):
        shutil.rmtree(log_dir)

@app.route('/update-config', methods=['POST'])
def update_config_route():
    try:
        file_name = request.form.get('fileName')
        if not file_name:
            return jsonify({'status': 'error', 'message': 'fileName is required'}), 400
        update_config_and_cleanup(file_name)
        return jsonify({'status': 'success'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/run-pipeline', methods=['POST'])
def run_pipeline_api():
    try:
        result = run_pipeline()
        return jsonify({'status': 'success', 'result': result})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    app.run(port=5000, debug=True)

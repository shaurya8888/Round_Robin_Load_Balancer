from flask import Flask, request
import logging
import os

app = Flask(__name__)
LOG_FILE = "logs/server_logs.txt"

# Logging Configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()])

@app.route('/', methods=['POST'])
def process_request():
    data = request.get_data(as_text=True)
    logging.info(f"Processing request: {data}")
    return f"Processed: {data}"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)  # Adjust port for each instance

from flask import Flask, request, jsonify
import socket
import threading
from queue import Queue
import json
import sqlite3
import logging
import ssl
from typing import List, Tuple

# Flask App
app = Flask(__name__)

# Configuration and Globals
with open("config.json") as config_file:
    CONFIG = json.load(config_file)

SERVERS = CONFIG["servers"]
DB_FILE = "database/requests.db"
LOG_FILE = "logs/load_balancer.log"
QUEUE = Queue()
THREAD_POOL = []
MAX_WORKERS = CONFIG["max_workers"]
current_index = 0  # For Round-Robin


# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()])


# Database Initialization
def init_database():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS requests (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        data TEXT,
                        server TEXT,
                        status TEXT,
                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                    )''')
    conn.commit()
    conn.close()


# Request Logging to DB
def log_request(data, server, status):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("INSERT INTO requests (data, server, status) VALUES (?, ?, ?)", (data, server, status))
    conn.commit()
    conn.close()


# Forward Request to Target Server
def forward_request(server: Tuple[str, int], data: str) -> Tuple[str, str]:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(server)
            s.sendall(data.encode())
            response = s.recv(1024).decode()
        return response, "success"
    except Exception as e:
        return str(e), "failure"


# Health Check for Servers
def is_server_alive(server: Tuple[str, int]) -> bool:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(1)
            s.connect(server)
        return True
    except:
        return False


# Worker Function for Processing Requests
def worker():
    global current_index
    while True:
        data = QUEUE.get()  # Blocking call
        if data is None:
            break

        server = SERVERS[current_index]
        current_index = (current_index + 1) % len(SERVERS)

        if is_server_alive(server):
            response, status = forward_request(server, data)
            log_request(data, f"{server[0]}:{server[1]}", status)
            logging.info(f"Request '{data}' -> Server: {server}, Status: {status}")
        else:
            logging.error(f"Server {server} is down. Request '{data}' failed.")
        QUEUE.task_done()


@app.route('/process', methods=['POST'])
def load_balancer():
    data = request.get_data(as_text=True)
    QUEUE.put(data)
    logging.info(f"Request queued: {data}")
    return jsonify({"message": "Request queued"}), 202


@app.route('/health', methods=['GET'])
def health_check():
    statuses = {f"{server[0]}:{server[1]}": is_server_alive(server) for server in SERVERS}
    return jsonify(statuses)


if __name__ == '__main__':
    init_database()

    # Start thread pool
    for _ in range(MAX_WORKERS):
        thread = threading.Thread(target=worker, daemon=True)
        THREAD_POOL.append(thread)
        thread.start()

    # Start Flask App
    context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    context.load_cert_chain(certfile=CONFIG["ssl_cert"], keyfile=CONFIG["ssl_key"])
    app.run(host='0.0.0.0', port=CONFIG["port"], ssl_context=context)

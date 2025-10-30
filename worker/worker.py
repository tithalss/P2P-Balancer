import socket
import json
import threading
import time
import uuid
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")

WORKER_ID = str(uuid.uuid4())
HOST = "10.62.217.16"
PORT = 6000
MASTER_HOST = "10.62.217.204"
MASTER_PORT = 5000
active_tasks = 0
lock = threading.Lock()


def send_json(sock, obj):
    sock.sendall(json.dumps(obj).encode())


def register_master():
    payload = {"type": "register_worker", "worker_id": WORKER_ID, "port": PORT}
    try:
        with socket.socket() as s:
            s.connect((MASTER_HOST, MASTER_PORT))
            send_json(s, payload)
    except:
        pass


def report_result(task_id):
    try:
        with socket.socket() as s:
            s.connect((MASTER_HOST, MASTER_PORT))
            send_json(s, {"type": "task_result", "worker_id": WORKER_ID, "task_id": task_id})
    except:
        pass


def process_task(task):
    global active_tasks
    with lock:
        if active_tasks >= 1:
            return
        active_tasks += 1
    time.sleep(2)
    report_result(task["task_id"])
    with lock:
        active_tasks -= 1


def worker_server():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((HOST, PORT))
    s.listen(5)
    while True:
        conn, _ = s.accept()
        try:
            msg = json.loads(conn.recv(4096).decode())
            if msg.get("type") == "assign_task":
                threading.Thread(target=process_task, args=(msg["payload"],), daemon=True).start()
        except:
            pass
        finally:
            conn.close()


if __name__ == "__main__":
    threading.Thread(target=worker_server, daemon=True).start()
    time.sleep(1)
    register_master()
    while True:
        time.sleep(1)

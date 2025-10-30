import socket
import json
import threading
import time
import uuid
import random
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")

MASTER_ID = str(uuid.uuid4())
PORT = 5000
NEIGHBOR_MASTER = ("10.62.217.201", 5000)
THRESHOLD = 8
HOST = "10.62.217.204"
HEARTBEAT_INTERVAL = 5
HEARTBEAT_TIMEOUT = 15

workers = {}
pending_tasks = []
lock = threading.Lock()
known_masters = {}


def send_json(sock, obj):
    sock.sendall(json.dumps(obj).encode())


def handle_client(conn, addr):
    try:
        msg = json.loads(conn.recv(4096).decode())

        if msg.get("SERVER") == "ALIVE" and msg.get("TASK") == "REQUEST":
            send_json(conn, {"SERVER": "ALIVE", "TASK": "RECIEVE"})
            with lock:
                known_masters[addr[0]] = {"last_seen": time.time()}
            return

        if msg.get("SERVER") == "ALIVE" and msg.get("TASK") == "RECIEVE":
            with lock:
                known_masters[addr[0]] = {"last_seen": time.time()}
            return

        if msg.get("type") == "register_worker":
            wid = msg["worker_id"]
            port = msg.get("port", 6000)
            with lock:
                workers[wid] = {"host": addr[0], "port": port, "status": "PARADO", "emprestado": False, "original_master": None}
            send_json(conn, {"status": "registered"})

        if msg.get("TASK") == "WORKER_REQUEST":
            n = msg.get("WORKERS_NEEDED", 1)
            with lock:
                available = [(wid, w) for wid, w in workers.items() if w["status"] == "PARADO" and not w["emprestado"]]
            if available:
                offered = []
                for wid, w in available[:n]:
                    offered.append({"WORKER_UUID": wid, "host": w["host"], "port": w["port"], "MASTER_ORIGIN": HOST})
                    workers[wid]["status"] = "TRANSFERIDO"
                    workers[wid]["emprestado"] = True
                    workers[wid]["original_master"] = HOST
                send_json(conn, {"TASK": "WORKER_RESPONSE", "STATUS": "ACK", "MASTER_UUID": MASTER_ID, "WORKERS": offered})
            else:
                send_json(conn, {"TASK": "WORKER_RESPONSE", "STATUS": "NACK", "WORKERS": []})

        if msg.get("WORKER") == "ALIVE":
            wid = msg.get("WORKER_UUID")
            with lock:
                workers[wid] = {"host": addr[0], "port": 6000, "status": "PARADO", "emprestado": True, "original_master": None}

        if msg.get("type") == "task_result":
            wid = msg["worker_id"]
            task_id = msg["task_id"]
            with lock:
                if wid in workers:
                    workers[wid]["status"] = "PARADO"

    except Exception as e:
        logging.error(f"ERRO: {e}")
    finally:
        conn.close()


def start_master_server():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((HOST, PORT))
    s.listen(10)
    while True:
        conn, addr = s.accept()
        threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()


def heartbeat():
    while True:
        try:
            with socket.socket() as s:
                s.connect(NEIGHBOR_MASTER)
                send_json(s, {"SERVER": "ALIVE", "TASK": "REQUEST"})
                s.recv(4096)
        except:
            pass
        time.sleep(HEARTBEAT_INTERVAL)


def monitor_masters():
    while True:
        now = time.time()
        with lock:
            inactive = [m for m, i in known_masters.items() if now - i["last_seen"] > HEARTBEAT_TIMEOUT]
            for m in inactive:
                del known_masters[m]
        time.sleep(5)


def distribute_tasks():
    while True:
        with lock:
            idle = [wid for wid, w in workers.items() if w["status"] == "PARADO"]
            while idle and pending_tasks:
                wid = idle.pop(0)
                task = pending_tasks.pop(0)
                w = workers[wid]
                try:
                    with socket.socket() as s:
                        s.connect((w["host"], w["port"]))
                        send_json(s, {"type": "assign_task", "payload": task, "MASTER_HOST": HOST, "MASTER_PORT": PORT})
                    workers[wid]["status"] = "OCUPADO"
                except:
                    workers.pop(wid, None)
        time.sleep(1)


def monitor_load():
    while True:
        with lock:
            count = len(pending_tasks)
        if count > THRESHOLD:
            request_support()
        time.sleep(3)


def request_support():
    try:
        with socket.socket() as s:
            s.connect(NEIGHBOR_MASTER)
            send_json(s, {"TASK": "WORKER_REQUEST", "WORKERS_NEEDED": 5})
            response = json.loads(s.recv(4096).decode())
            if response.get("TASK") == "WORKER_RESPONSE" and response.get("STATUS") == "ACK":
                for w in response["WORKERS"]:
                    wid = w["WORKER_UUID"]
                    with lock:
                        workers[wid] = {"host": w["host"], "port": w["port"], "status": "PARADO", "emprestado": True, "original_master": w["MASTER_ORIGIN"]}
    except:
        pass


def generate_tasks():
    i = 1
    while True:
        with lock:
            pending_tasks.append({"task_id": f"T{i}", "workload": [random.randint(1, 100)]})
        i += 1
        time.sleep(2)


if __name__ == "__main__":
    threading.Thread(target=start_master_server, daemon=True).start()
    threading.Thread(target=heartbeat, daemon=True).start()
    threading.Thread(target=monitor_masters, daemon=True).start()
    threading.Thread(target=distribute_tasks, daemon=True).start()
    threading.Thread(target=monitor_load, daemon=True).start()
    threading.Thread(target=generate_tasks, daemon=True).start()
    while True:
        time.sleep(10)
        with lock:
            logging.info(f"Pendentes: {len(pending_tasks)} | Workers: {len(workers)} | Masters ativos: {len(known_masters)}")

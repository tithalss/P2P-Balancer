import socket
import json
import threading
import time
import uuid
import random
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)

MASTER_ID = str(uuid.uuid4())
PORT = 5000
NEIGHBOR_MASTER = ("10.62.217.13", 5000)
THRESHOLD = 8
HOST = "10.62.217.201"
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

        if msg.get("type") == "MASTER_ALIVE":
            master_id = msg.get("MASTER_ID")
            with lock:
                known_masters[master_id] = {"host": addr[0], "last_seen": time.time()}
            send_json(conn, {"type": "ALIVE_ACK", "MASTER_ID": MASTER_ID})
            logging.info(f"Recebeu heartbeat do Master {master_id}")
            return

        elif msg.get("type") == "ALIVE_ACK":
            mid = msg.get("MASTER_ID")
            with lock:
                if mid in known_masters:
                    known_masters[mid]["last_seen"] = time.time()
            logging.info(f"{mid} respondeu o heartbeat")
            return

        if msg.get("type") == "register_worker":
            wid = msg["worker_id"]
            port = msg.get("port", 6000)
            with lock:
                workers[wid] = {"host": addr[0], "port": port, "status": "PARADO"}
            logging.info(f"Worker {wid} registrado ({addr[0]}:{port})")
            send_json(conn, {"status": "registered"})

        elif msg.get("WORKER") == "ALIVE":
            wid = msg.get("WORKER_UUID")
            origin = msg.get("MASTER_ORIGIN")
            worker_host = addr[0]
            worker_port = msg.get("port", 6000)
            with lock:
                workers[wid] = {"host": worker_host, "port": worker_port, "status": "PARADO"}
            logging.info(f"Worker redirecionado do {origin}: {wid}")

        elif msg.get("type") == "task_result":
            wid = msg["worker_id"]
            task_id = msg["task_id"]
            result = msg["result"]
            logging.info(f"Resultado de {wid}: {task_id} = {result}")
            with lock:
                if wid in workers:
                    workers[wid]["status"] = "PARADO"

        elif msg.get("TASK") == "WORKER_REQUEST":
            requester = msg.get("MASTER")
            with lock:
                available = [(wid, w) for wid, w in workers.items() if w["status"] == "PARADO"]

            if available:
                offered = []
                for wid, w in available:
                    offered.append({"WORKER_UUID": wid, "host": w["host"], "port": w["port"]})
                    workers[wid]["status"] = "TRANSFERIDO"

                send_json(conn, {
                    "RESPONSE": "AVAILABLE",
                    "MASTER": MASTER_ID,
                    "WORKERS": offered
                })
                logging.info(f"Ofereceu {len(offered)} worker(s) para {requester}")
            else:
                send_json(conn, {"RESPONSE": "UNAVAILABLE", "MASTER": MASTER_ID})
                logging.info(f"Sem workers disponíveis para {requester}")

    except Exception as e:
        logging.error(f"ERRO handle_client: {e}")
    finally:
        conn.close()


def start_master_server():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((HOST, PORT))
    s.listen(10)
    logging.info(f"Servidor ativo em {HOST}:{PORT}")
    while True:
        conn, addr = s.accept()
        threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()


def master_heartbeat():
    while True:
        try:
            with socket.socket() as s:
                s.settimeout(5)
                s.connect(NEIGHBOR_MASTER)
                send_json(s, {"type": "MASTER_ALIVE", "MASTER_ID": MASTER_ID})
                response = json.loads(s.recv(4096).decode())
                if response.get("type") == "ALIVE_ACK":
                    mid = response["MASTER_ID"]
                    with lock:
                        known_masters[mid] = {"host": NEIGHBOR_MASTER[0], "last_seen": time.time()}
                    logging.info(f"Heartbeat OK com {mid}")
        except Exception as e:
            logging.warning(f"Falha no heartbeat com {NEIGHBOR_MASTER[0]}: {e}")
        time.sleep(HEARTBEAT_INTERVAL)


def monitor_masters():
    while True:
        now = time.time()
        with lock:
            inactive = [mid for mid, info in known_masters.items() if now - info["last_seen"] > HEARTBEAT_TIMEOUT]
            for mid in inactive:
                logging.warning(f"Master {mid} inativo há mais de {HEARTBEAT_TIMEOUT}s")
                del known_masters[mid]
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
                        send_json(s, {
                            "type": "assign_task",
                            "payload": task,
                            "MASTER_HOST": HOST,
                            "MASTER_PORT": PORT
                        })
                    workers[wid]["status"] = "OCUPADO"
                    logging.info(f"Enviou {task['task_id']} para {wid}: {task['numbers']}")
                except Exception as e:
                    logging.error(f"Erro enviando tarefa para {wid}: {e}")
                    workers.pop(wid, None)
        time.sleep(1)


def monitor_load():
    while True:
        time.sleep(3)
        with lock:
            count = len(pending_tasks)
        if count > THRESHOLD:
            logging.warning(f"ALERTA: {count} tarefas pendentes! Solicitando suporte...")
            request_support()


def request_support():
    try:
        with socket.socket() as s:
            s.connect(NEIGHBOR_MASTER)
            send_json(s, {"MASTER": MASTER_ID, "TASK": "WORKER_REQUEST"})
            response = json.loads(s.recv(4096).decode())

            if response.get("RESPONSE") == "AVAILABLE":
                for w in response["WORKERS"]:
                    wid = w["WORKER_UUID"]
                    host = w["host"]
                    port = w["port"]
                    with lock:
                        workers[wid] = {"host": host, "port": port, "status": "PARADO"}
                    logging.info(f"Worker emprestado registrado: {wid} ({host}:{port})")
            else:
                logging.info(f"Sem suporte disponível do {response['MASTER']}")
    except Exception as e:
        logging.error(f"Erro solicitando suporte: {e}")


def generate_tasks():
    i = 1
    while True:
        task = {"task_id": f"T{i}", "numbers": [random.randint(1, 100) for _ in range(5)]}
        with lock:
            pending_tasks.append(task)
        logging.debug(f"Tarefa gerada: {task['task_id']}")
        i += 1
        time.sleep(2)


if __name__ == "__main__":
    threading.Thread(target=start_master_server, daemon=True).start()
    threading.Thread(target=master_heartbeat, daemon=True).start()
    threading.Thread(target=monitor_masters, daemon=True).start()
    threading.Thread(target=distribute_tasks, daemon=True).start()
    threading.Thread(target=monitor_load, daemon=True).start()
    threading.Thread(target=generate_tasks, daemon=True).start()

    while True:
        time.sleep(10)
        with lock:
            active_workers = len(workers)
            active_masters = len(known_masters)
            pending = len(pending_tasks)
        logging.info(f"Pendentes: {pending} | Workers: {active_workers} | Masters ativos: {active_masters}")

import socket
import json
import threading
import time
import uuid
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)

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
            logging.info(f"[REGISTER] Registrado no master {MASTER_HOST}:{MASTER_PORT}")
    except Exception as e:
        logging.error(f"[REGISTER] Falha ao registrar: {e}")


def report_result(task_id):
    try:
        with socket.socket() as s:
            s.connect((MASTER_HOST, MASTER_PORT))
            send_json(s, {"type": "task_result", "worker_id": WORKER_ID, "task_id": task_id})
            logging.info(f"[TASK_RESULT] Task {task_id} concluída e enviada ao master")
    except Exception as e:
        logging.error(f"[TASK_RESULT] Erro ao enviar resultado: {e}")


def process_task(task):
    global active_tasks
    with lock:
        if active_tasks >= 1:
            logging.warning("[WORKER] Ocupado, não pode aceitar nova tarefa")
            return
        active_tasks += 1

    logging.info(f"[TASK] Iniciando {task['task_id']} - workload={task['workload']}")
    time.sleep(2)
    report_result(task["task_id"])
    with lock:
        active_tasks -= 1
    logging.info(f"[TASK] Finalizada {task['task_id']}")


def worker_server():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((HOST, PORT))
    s.listen(5)
    logging.info(f"[WORKER] Ativo em {HOST}:{PORT}")
    while True:
        conn, addr = s.accept()
        try:
            msg = json.loads(conn.recv(4096).decode())
            logging.info(f"[RECV] {addr} -> {msg}")
            if msg.get("type") == "assign_task":
                threading.Thread(target=process_task, args=(msg["payload"],), daemon=True).start()
        except Exception as e:
            logging.error(f"[WORKER] Erro ao processar mensagem: {e}")
        finally:
            conn.close()


if __name__ == "__main__":
    threading.Thread(target=worker_server, daemon=True).start()
    time.sleep(1)
    register_master()
    while True:
        time.sleep(1)

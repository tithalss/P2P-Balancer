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
ORIGINAL_MASTER = (MASTER_HOST, MASTER_PORT)
active_tasks = 0
lock = threading.Lock()


def send_json(sock, obj):
    sock.sendall(json.dumps(obj).encode())


def register_master(host=None, port=None):
    global MASTER_HOST, MASTER_PORT
    if host:
        MASTER_HOST = host
    if port:
        MASTER_PORT = port
    payload = {
        "MASTER": MASTER_HOST,
        "MASTER_ORIGIN": MASTER_HOST,
        "WORKER": "ALIVE",
        "WORKER_UUID": WORKER_ID
    }
    try:
        with socket.socket() as s:
            s.connect((MASTER_HOST, MASTER_PORT))
            send_json(s, payload)
            logging.info(f"Registrado no Master {MASTER_HOST}:{MASTER_PORT}")
    except Exception as e:
        logging.error(f"Falha ao registrar no master {MASTER_HOST}:{MASTER_PORT} - {e}")


def report_result(task_id, result, master_host=None, master_port=None):
    host = master_host or MASTER_HOST
    port = master_port or MASTER_PORT
    try:
        with socket.socket() as s:
            s.connect((host, port))
            send_json(s, {"type": "task_result", "worker_id": WORKER_ID, "task_id": task_id, "result": result})
            logging.info(f"Resultado enviado: {task_id} = {result} para {host}:{port}")
    except Exception as e:
        logging.error(f"Falha ao enviar resultado para {host}:{port} - {e}")


def process_task(task, master_host=None, master_port=None):
    global active_tasks
    with lock:
        if active_tasks >= 1:
            return
        active_tasks += 1
    logging.info(f"Executando {task['task_id']}: {task['workload']}")
    time.sleep(1)
    logging.info(f"Task concluída {task['task_id']}")
    report_result(task['task_id'], master_host, master_port)
    with lock:
        active_tasks -= 1
        if active_tasks == 0 and (MASTER_HOST, MASTER_PORT) != ORIGINAL_MASTER:
            return_to_original_master()


def return_to_original_master():
    global MASTER_HOST, MASTER_PORT
    host, port = ORIGINAL_MASTER
    try:
        with socket.socket() as s:
            s.connect((MASTER_HOST, MASTER_PORT))
            send_json(s, {"type": "worker_returning", "worker_id": WORKER_ID})
        logging.info(f"Retornando ao Master original {host}:{port}")
        MASTER_HOST, MASTER_PORT = host, port
        register_master()
    except Exception as e:
        logging.error(f"Erro ao retornar ao master original {host}:{port} - {e}")


def worker_server():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((HOST, PORT))
    s.listen(5)
    logging.info(f"Servidor ativo {HOST}:{PORT}")
    while True:
        conn, addr = s.accept()
        try:
            msg = json.loads(conn.recv(4096).decode())
            if msg.get("type") == "assign_task":
                master_host = msg.get("MASTER_HOST")
                master_port = msg.get("MASTER_PORT")
                threading.Thread(target=process_task, args=(msg['payload'], master_host, master_port), daemon=True).start()
            elif msg.get("TASK") == "REDIRECT":
                master_host = msg["host"]
                master_port = msg["port"]
                logging.info(f"Redirecionado para novo Master {master_host}:{master_port}")
                register_master(master_host, master_port)
        except Exception as e:
            logging.error(f"Erro ao processar mensagem: {e}")
        finally:
            conn.close()


if __name__ == "__main__":
    threading.Thread(target=worker_server, daemon=True).start()
    time.sleep(1)
    threading.Thread(target=register_master, daemon=True).start()
    while True:
        time.sleep(1)

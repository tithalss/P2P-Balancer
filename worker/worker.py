import socket
import json
import threading
import time
import uuid

WORKER_ID = str(uuid.uuid4())
HOST = "127.0.0.1"
PORT = 6000
MASTER_HOST = "127.0.0.1"
MASTER_PORT = 5000
active_tasks = 0
lock = threading.Lock()


def send_json(sock, obj):
    sock.sendall(json.dumps(obj).encode())


def register_master(host=None, port=None):
    global MASTER_HOST, MASTER_PORT
    if host: MASTER_HOST = host
    if port: MASTER_PORT = port
    try:
        with socket.socket() as s:
            s.connect((MASTER_HOST, MASTER_PORT))
            send_json(s, {"type": "register_worker", "worker_id": WORKER_ID, "port": PORT})
            print(f"[WORKER] Registrado no Master {MASTER_HOST}:{MASTER_PORT}")
    except:
        pass


def report_result(task_id, result, master_host=None, master_port=None):
    host = master_host or MASTER_HOST
    port = master_port or MASTER_PORT
    try:
        with socket.socket() as s:
            s.connect((host, port))
            send_json(s, {"type": "task_result", "worker_id": WORKER_ID, "task_id": task_id, "result": result})
    except:
        pass


def process_task(task, master_host=None, master_port=None):
    global active_tasks
    with lock:
        if active_tasks >= 1:
            print(f"[WORKER] Limite de tasks simultâneas atingido, esperando...")
            return
        active_tasks += 1

    print(f"[WORKER] Executando {task['task_id']}: {task['numbers']}")
    time.sleep(10)
    result = sum(task['numbers'])
    print(f"[WORKER] Concluído {task['task_id']} = {result}")
    report_result(task['task_id'], result, master_host, master_port)

    with lock:
        active_tasks -= 1


def worker_server():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((HOST, PORT))
    s.listen(5)
    print(f"[WORKER] Servidor ativo {HOST}:{PORT}")
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
                print(f"[WORKER] Redirecionado para novo Master {master_host}:{master_port}")
                register_master(master_host, master_port)

        except Exception as e:
            print(f"[WORKER] ERRO servidor: {e}")
        finally:
            conn.close()


if __name__ == "__main__":
    threading.Thread(target=worker_server, daemon=True).start()
    time.sleep(1)
    register_master()

    while True:
        time.sleep(1)

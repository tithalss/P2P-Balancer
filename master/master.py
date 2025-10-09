import socket
import json
import threading
import time
import uuid
import random

MASTER_ID = str(uuid.uuid4())
HOST = "127.0.0.1"
PORT = 5000

workers = {}
pending_tasks = []
lock = threading.Lock()


def send_json(sock, obj):
    sock.sendall(json.dumps(obj).encode())


def handle_client(conn, addr):
    print(f"[MASTER] Conexão recebida de {addr}")
    try:
        msg = json.loads(conn.recv(4096).decode())
        tipo = msg.get("type")

        if tipo == "register_worker":
            worker_id = msg["worker_id"]
            port = msg.get("port", 6000)
            with lock:
                workers[worker_id] = {"host": addr[0], "port": port, "status": "PARADO"}
            print(f"[MASTER] Worker {worker_id} registrado ({addr[0]}:{port})")
            send_json(conn, {"status": "registered"})

        elif tipo == "status_update":
            worker_id = msg["worker_id"]
            status = msg["status"]
            with lock:
                if worker_id in workers:
                    workers[worker_id]["status"] = status
            print(f"[MASTER] Status do Worker {worker_id}: {status}")

        elif tipo == "task_result":
            worker_id = msg["worker_id"]
            task_id = msg["task_id"]
            result = msg["result"]
            print(f"[MASTER] Resultado recebido do Worker {worker_id}: Tarefa {task_id} = {result}")

        elif tipo == "client_task":
            task = msg["payload"]
            with lock:
                pending_tasks.append(task)
            print(f"[MASTER] Nova tarefa recebida: {task['task_id']}")
            send_json(conn, {"status": "queued"})

    except Exception as e:
        print(f"[ERRO] handle_client: {e}")
    finally:
        conn.close()


def start_master_server():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.bind((HOST, PORT))
        server.listen()
        print(f"[MASTER] Servidor ativo em {HOST}:{PORT} (ID {MASTER_ID})")
        while True:
            conn, addr = server.accept()
            threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()


def distribute_tasks():
    while True:
        with lock:
            idle_workers = [wid for wid, w in workers.items() if w["status"] == "PARADO"]
            if idle_workers and pending_tasks:
                for wid in idle_workers:
                    if not pending_tasks:
                        break
                    task = pending_tasks.pop(0)
                    worker = workers[wid]
                    try:
                        with socket.socket() as s:
                            s.connect((worker["host"], worker["port"]))
                            send_json(s, {"type": "assign_task", "payload": task})
                        workers[wid]["status"] = "OCUPADO"
                        print(f"[MASTER] Enviou tarefa {task['task_id']} para {wid}: {task['numbers']}")
                    except Exception as e:
                        workers[wid]["status"] = "offline"
                        print(f"[ERRO] Falha ao enviar tarefa para {wid}: {e}")
        time.sleep(1)


if __name__ == "__main__":
    threading.Thread(target=start_master_server, daemon=True).start()
    threading.Thread(target=distribute_tasks, daemon=True).start()

    i = 1
    while True:
        task = {
            "task_id": f"T{i}",
            "numbers": [random.randint(1, 1000) for _ in range(5)]
        }
        pending_tasks.append(task)
        print(f"[MASTER] Nova tarefa: {task['task_id']} -> {task['numbers']}")
        i += 1
        time.sleep(10)

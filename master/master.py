import socket
import json
import threading
import time
import uuid
import random

MASTER_ID = str(uuid.uuid4())
PORT = 5000
NEIGHBOR_MASTER = ("127.0.0.1", 5001)
THRESHOLD = 8
HOST = "127.0.0.1"

workers = {}
pending_tasks = []
lock = threading.Lock()


def send_json(sock, obj):
    sock.sendall(json.dumps(obj).encode())


def handle_client(conn, addr):
    try:
        msg = json.loads(conn.recv(4096).decode())
        if msg.get("type") == "register_worker":
            wid = msg["worker_id"]
            port = msg.get("port", 6000)
            with lock:
                workers[wid] = {"host": addr[0], "port": port, "status": "PARADO"}
            print(f"[{MASTER_ID}] Worker {wid} registrado ({addr[0]}:{port})")
            send_json(conn, {"status": "registered"})

        elif msg.get("WORKER") == "ALIVE":
            wid = msg.get("WORKER_UUID")
            origin = msg.get("MASTER_ORIGIN")
            worker_host = addr[0]
            worker_port = msg.get("port", 6000)
            with lock:
                workers[wid] = {"host": worker_host, "port": worker_port, "status": "PARADO"}
            print(f"[{MASTER_ID}] Worker redirecionado do {origin}: {wid}")

        elif msg.get("type") == "task_result":
            wid = msg["worker_id"]
            task_id = msg["task_id"]
            result = msg["result"]
            print(f"[{MASTER_ID}] Resultado de {wid}: {task_id} = {result}")
            with lock:
                if wid in workers:
                    workers[wid]["status"] = "PARADO"

        elif msg.get("TASK") == "WORKER_REQUEST":
            requester = msg.get("MASTER")
            with lock:
                available = [ (wid, w) for wid, w in workers.items() if w["status"] == "PARADO"]

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
                print(f"[{MASTER_ID}] Ofereceu {len(offered)} worker(s) para {requester}")
            else:
                send_json(conn, {"RESPONSE": "UNAVAILABLE", "MASTER": MASTER_ID})

    except Exception as e:
        print(f"[{MASTER_ID}] ERRO handle_client: {e}")
    finally:
        conn.close()


def start_master_server():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((HOST, PORT))
    s.listen(10)
    print(f"[{MASTER_ID}] Servidor ativo em {HOST}:{PORT}")
    while True:
        conn, addr = s.accept()
        threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()


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
                    print(f"[{MASTER_ID}] Enviou {task['task_id']} para {wid}: {task['numbers']}")
                except Exception as e:
                    print(f"[{MASTER_ID}] ERRO enviando tarefa para {wid}: {e}")
                    workers.pop(wid, None)
        time.sleep(1)


def monitor_load():
    while True:
        time.sleep(3)
        with lock:
            count = len(pending_tasks)
        if count > THRESHOLD:
            print(f"[{MASTER_ID}] ALERTA: {count} tarefas pendentes! Solicitando suporte...")
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
                    print(f"[{MASTER_ID}] Worker emprestado registrado: {wid} ({host}:{port})")
            else:
                print(f"[{MASTER_ID}] Sem suporte disponível do {response['MASTER']}")
    except Exception as e:
        print(f"[{MASTER_ID}] ERRO solicitando suporte: {e}")


def generate_tasks():
    i = 1
    while True:
        task = {"task_id": f"T{i}", "numbers": [random.randint(1, 100) for _ in range(5)]}
        with lock:
            pending_tasks.append(task)
        i += 1
        time.sleep(8)


if __name__ == "__main__":
    threading.Thread(target=start_master_server, daemon=True).start()
    threading.Thread(target=distribute_tasks, daemon=True).start()
    threading.Thread(target=monitor_load, daemon=True).start()
    threading.Thread(target=generate_tasks, daemon=True).start()

    while True:
        time.sleep(10)
        with lock:
            active_workers = len(workers)
            print(f"[{MASTER_ID}] Pendentes: {len(pending_tasks)} | Workers ativos: {active_workers}")

import socket
import json
import threading
import time
import uuid

WORKER_ID = str(uuid.uuid4())
HOST = "127.0.0.1"
PORT = 6000
MASTER = "127.0.0.1:5000"


def send_json(sock, obj):
    sock.sendall(json.dumps(obj).encode())


def register_master():
    h, p = MASTER.split(":")
    try:
        with socket.socket() as s:
            s.connect((h, int(p)))
            send_json(s, {"type": "register_worker", "worker_id": WORKER_ID, "port": PORT})
            print(f"[WORKER] Registrado no Master {MASTER}")
    except Exception as e:
        print(f"[WORKER] Erro ao registrar: {e}")


def report_status(status="PARADO"):
    h, p = MASTER.split(":")
    try:
        with socket.socket() as s:
            s.connect((h, int(p)))
            send_json(s, {"type": "status_update", "worker_id": WORKER_ID, "status": status})
            print(f"[WORKER] Status enviado: {status}")
    except Exception:
        pass


def send_result(task_id, result):
    h, p = MASTER.split(":")
    try:
        with socket.socket() as s:
            s.connect((h, int(p)))
            send_json(s, {"type": "task_result", "worker_id": WORKER_ID, "task_id": task_id, "result": result})
            print(f"[WORKER] Resultado enviado: Tarefa {task_id} = {result}")
    except Exception as e:
        print(f"[WORKER] Erro ao enviar resultado: {e}")


def process_task(payload):
    task_id = payload.get("task_id")
    numbers = payload.get("numbers", [])
    report_status("OCUPADO")
    print(f"[WORKER] Executando tarefa {task_id}: {numbers}")
    result = sum(numbers)
    print(f"[WORKER] Tarefa {task_id} concluída: {result}")
    send_result(task_id, result)
    report_status("PARADO")


def start_worker_server():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((HOST, PORT))
    s.listen(5)
    print(f"[WORKER] Servidor ativo em {HOST}:{PORT} (ID {WORKER_ID})")

    while True:
        conn, _ = s.accept()
        try:
            msg = json.loads(conn.recv(4096).decode())
            tipo = msg.get("type")
            payload = msg.get("payload", {})

            if tipo == "assign_task":
                threading.Thread(target=process_task, args=(payload,), daemon=True).start()

        except Exception as e:
            print(f"[WORKER] Erro no servidor: {e}")
        finally:
            conn.close()


if __name__ == "__main__":
    threading.Thread(target=start_worker_server, daemon=True).start()
    time.sleep(1)
    register_master()

    while True:
        time.sleep(5)
        report_status("PARADO")

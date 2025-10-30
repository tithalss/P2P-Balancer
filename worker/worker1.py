import socket
import json
import threading
import time
import uuid

# 🧩 Identificação e configuração do Worker
WORKER_ID = str(uuid.uuid4())
HOST = "127.0.0.1"
PORT = 6000
MASTER_HOST = "127.0.0.1"
MASTER_PORT = 5000

active_tasks = 0
lock = threading.Lock()


# =====================================================
# 🔧 Funções utilitárias
# =====================================================
def send_json(sock, obj):
    """Envia objeto JSON pelo socket."""
    sock.sendall(json.dumps(obj).encode())


def register_master(host=None, port=None):
    """Registra o worker no master especificado."""
    global MASTER_HOST, MASTER_PORT
    if host:
        MASTER_HOST = host
    if port:
        MASTER_PORT = port

    try:
        with socket.socket() as s:
            s.connect((MASTER_HOST, MASTER_PORT))
            send_json(s, {"type": "register_worker", "worker_id": WORKER_ID, "port": PORT})
            print(f"[WORKER] ✅ Registrado no Master {MASTER_HOST}:{MASTER_PORT}")
    except Exception as e:
        print(f"[WORKER] ⚠️ Falha ao registrar no Master {MASTER_HOST}:{MASTER_PORT} → {e}")


def report_result(task_id, result, master_host=None, master_port=None):
    """Envia o resultado da tarefa processada para o master."""
    host = master_host or MASTER_HOST
    port = master_port or MASTER_PORT
    try:
        with socket.socket() as s:
            s.connect((host, port))
            send_json(s, {
                "type": "task_result",
                "worker_id": WORKER_ID,
                "task_id": task_id,
                "result": result
            })
    except Exception as e:
        print(f"[WORKER] ⚠️ Erro ao reportar resultado: {e}")


# =====================================================
# ⚙️ Execução das tarefas
# =====================================================
def process_task(task, master_host=None, master_port=None):
    """Executa a tarefa recebida."""
    global active_tasks
    with lock:
        if active_tasks >= 1:
            print(f"[WORKER] 🚫 Limite de tasks simultâneas atingido, aguardando...")
            return
        active_tasks += 1

    try:
        print(f"[WORKER] 🏗️ Executando {task['task_id']}: {task['numbers']}")
        time.sleep(10)  # Simula processamento
        result = sum(task['numbers'])
        print(f"[WORKER] ✅ Concluído {task['task_id']} = {result}")
        report_result(task['task_id'], result, master_host, master_port)
    except Exception as e:
        print(f"[WORKER] ❌ Erro ao processar tarefa: {e}")
    finally:
        with lock:
            active_tasks -= 1


# =====================================================
# 🧠 Lógica principal do servidor Worker
# =====================================================
def worker_server():
    """Servidor TCP do worker para receber comandos e tarefas."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((HOST, PORT))
    s.listen(5)
    print(f"[WORKER] 🚀 Servidor ativo em {HOST}:{PORT}")

    while True:
        conn, addr = s.accept()
        try:
            msg = json.loads(conn.recv(4096).decode())

            # 🧩 Recebe nova tarefa
            if msg.get("type") == "assign_task":
                master_host = msg.get("MASTER_HOST")
                master_port = msg.get("MASTER_PORT")
                threading.Thread(
                    target=process_task,
                    args=(msg['payload'], master_host, master_port),
                    daemon=True
                ).start()

            # 🔁 Redirecionamento para novo master
            elif msg.get("TASK") == "REDIRECT":
                master_host = msg["host"]
                master_port = msg["port"]
                print(f"[WORKER] 🔄 Redirecionado para novo Master {master_host}:{master_port}")
                register_master(master_host, master_port)

            # 🔙 Devolução automática (Regra de Negócio |5|)
            elif msg.get("type") == "command_release":
                origin_host = msg.get("origin_host")
                origin_port = msg.get("origin_port")
                print(f"[WORKER] 🧭 Recebeu comando de devolução → retornando para {origin_host}:{origin_port}")

                # Re-registra no master de origem
                register_master(origin_host, origin_port)

                # Confirma retorno
                try:
                    with socket.socket() as s2:
                        s2.connect((origin_host, origin_port))
                        send_json(s2, {
                            "WORKER": "ALIVE",
                            "WORKER_UUID": WORKER_ID,
                            "MASTER_ORIGIN": f"{MASTER_HOST}:{MASTER_PORT}",
                            "port": PORT
                        })
                    print(f"[WORKER] ✅ Re-registrado com sucesso no master de origem {origin_host}:{origin_port}")
                except Exception as e:
                    print(f"[WORKER] ⚠️ Falha ao confirmar retorno: {e}")

        except Exception as e:
            print(f"[WORKER] ❌ ERRO servidor: {e}")
        finally:
            conn.close()


# =====================================================
# 🚀 Inicialização
# =====================================================
if __name__ == "__main__":
    threading.Thread(target=worker_server, daemon=True).start()
    time.sleep(1)
    register_master()

    # Mantém o worker ativo
    while True:
        time.sleep(1)

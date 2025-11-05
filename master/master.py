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
NEIGHBOR_MASTER = ("127.0.0.1", 5001)  # Outro master
HOST = "127.0.0.1"
THRESHOLD = 10  
HEARTBEAT_INTERVAL = 5
HEARTBEAT_TIMEOUT = 15
RELEASE_BATCH = 2  # Quantos workers liberar por vez

# Separação das listas de workers
workers_filhos = {}         # wid -> {host, port, status}
workers_emprestados = {}    # wid -> {host, port, status, original_master}
pending_tasks = []
known_masters = {}
pending_releases = {}  # wid -> requester_master (aguardando retorno)
lock = threading.Lock()


def send_json(sock, obj):
    data = json.dumps(obj) + "\n"  # Adiciona delimitador \n
    sock.sendall(data.encode())


def recv_json(conn, timeout=5):
    """Recebe JSON com delimitador \n"""
    conn.settimeout(timeout)
    buf = b""
    try:
        while b"\n" not in buf:
            chunk = conn.recv(4096)
            if not chunk:
                break
            buf += chunk
    except socket.timeout:
        pass
    except Exception as e:
        logging.debug(f"[recv_json] erro: {e}")
    
    if not buf:
        return None
    
    try:
        raw = buf.decode(errors="ignore")
        return json.loads(raw.split("\n")[0])
    except Exception as e:
        logging.error(f"[recv_json] erro parse: {e}")
        return None


def handle_client(conn, addr):
    try:
        msg = recv_json(conn)
        if not msg:
            return
        logging.info(f"[RECV] {addr} -> {msg}")

        if msg.get("SERVER") == "ALIVE" and msg.get("TASK") == "REQUEST":
            send_json(conn, {"SERVER": "ALIVE", "TASK": "RECIEVE"})
            with lock:
                known_masters[addr[0]] = {"last_seen": time.time()}
            logging.info(f"[HEARTBEAT] Recebido REQUEST de {addr[0]}")
            return

        if msg.get("SERVER") == "ALIVE" and msg.get("TASK") == "RECIEVE":
            with lock:
                known_masters[addr[0]] = {"last_seen": time.time()}
            logging.info(f"[HEARTBEAT] Recebido RECIEVE de {addr[0]}")
            return

        if msg.get("type") == "register_worker":
            wid = msg["worker_id"]
            port = msg.get("port", 6000)
            with lock:
                workers_filhos[wid] = {
                    "host": addr[0],
                    "port": port,
                    "status": "PARADO"
                }
            send_json(conn, {"status": "registered"})
            logging.info(f"[WORKER FILHO] Registrado {wid} de {addr[0]}:{port}")
            return

        if msg.get("TASK") == "WORKER_REQUEST":
            needed = msg.get("WORKERS_NEEDED", 1)
            logging.info(f"[WORKER_REQUEST] Pedido de {needed} workers vindo de {addr[0]}")
            with lock:
                available = [(wid, w) for wid, w in workers_filhos.items() if w["status"] == "PARADO"]

            if available:
                offered = []
                for wid, w in available[:needed]:
                    offered.append({
                        "WORKER_UUID": wid,
                        "host": w["host"],
                        "port": w["port"],
                        "MASTER_ORIGIN": HOST
                    })
                    with lock:
                        # marcar como transferido (não disponível localmente)
                        workers_filhos[wid]["status"] = "TRANSFERIDO"

                response = {
                    "TASK": "WORKER_RESPONSE",
                    "STATUS": "ACK",
                    "MASTER_UUID": MASTER_ID,
                    "WORKERS": offered
                }
                send_json(conn, response)
                logging.info(f"[WORKER_RESPONSE] Enviando ACK com {len(offered)} workers")
            else:
                response = {"TASK": "WORKER_RESPONSE", "STATUS": "NACK", "WORKERS": []}
                send_json(conn, response)
                logging.warning(f"[WORKER_RESPONSE] NACK - sem workers disponíveis")
            return

        # |5.4| Worker → Servidor (re-registro WORKER ALIVE)
        if msg.get("WORKER") == "ALIVE":
            wid = msg.get("WORKER_UUID")
            port = msg.get("port", 6000)
            origin = msg.get("MASTER_ORIGIN")
            
            with lock:
                if wid in workers_filhos:
                    # Worker filho re-registrou (ou retornou de transferência)
                    workers_filhos[wid]["status"] = "PARADO"
                    logging.info(f"[WORKER FILHO] {wid} PARADO")
                else:
                    # Novo worker (emprestado para este master)
                    workers_emprestados[wid] = {
                        "host": addr[0],
                        "port": port,
                        "status": "PARADO",
                        "original_master": origin
                    }
                    logging.info(f"[WORKER EMPRESTADO] Ativo: {wid} (origin={origin})")
            
            # Se estava aguardando retorno, notifica o master que emprestou (|5.5|)
            with lock:
                if wid in pending_releases:
                    requester = pending_releases.pop(wid)
                    threading.Thread(
                        target=notify_worker_returned, 
                        args=(requester, wid), 
                        daemon=True
                    ).start()
            return

        if msg.get("type") == "task_result":
            wid = msg["worker_id"]
            task_id = msg["task_id"]
            with lock:
                if wid in workers_filhos:
                    workers_filhos[wid]["status"] = "PARADO"
                if wid in workers_emprestados:
                    workers_emprestados[wid]["status"] = "PARADO"
            logging.info(f"[TASK] Resultado recebido de {wid} (task {task_id})")
            return

        # |5.1| Servidor A → Servidor B – COMMAND_RELEASE
        if msg.get("TASK") == "COMMAND_RELEASE":
            requester_master = msg.get("MASTER")
            worker_list = msg.get("WORKERS", [])
            logging.info(f"[COMMAND_RELEASE] Recebido de {requester_master} para workers {worker_list}")
            
            # |5.2| Confirma recebimento
            ack = {
                "MASTER": MASTER_ID,
                "RESPONSE": "RELEASE_ACK",
                "WORKERS": worker_list
            }
            send_json(conn, ack)
            logging.info(f"[RELEASE_ACK] Enviado para {requester_master}")
            
            # Marca workers como pendentes de retorno
            with lock:
                for wid in worker_list:
                    pending_releases[wid] = requester_master
            
            # |5.3| Ordena retorno aos workers
            threading.Thread(
                target=order_workers_return, 
                args=(worker_list,), 
                daemon=True
            ).start()
            return

        # |5.5| Confirmação de retorno completo
        if msg.get("RESPONSE") == "WORKER_RETURNED":
            origin = msg.get("MASTER")
            worker_list = msg.get("WORKERS", [])
            logging.info(f"[WORKER_RETURNED] Recebido de {origin}: workers {worker_list} retornaram")
            
            # Remove workers emprestados da lista
            with lock:
                for wid in worker_list:
                    if wid in workers_emprestados:
                        logging.info(f"[CLEANUP] Removendo worker emprestado {wid}")
                        workers_emprestados.pop(wid, None)
            return

    except Exception as e:
        logging.error(f"[ERRO handle_client] {e}")
    finally:
        conn.close()


def start_master_server():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((HOST, PORT))
    s.listen(10)
    logging.info(f"[SERVER] Servidor iniciado em {HOST}:{PORT}")
    while True:
        conn, addr = s.accept()
        threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()


def heartbeat():
    while True:
        try:
            with socket.socket() as s:
                s.settimeout(5)
                s.connect(NEIGHBOR_MASTER)
                send_json(s, {"SERVER": "ALIVE", "TASK": "REQUEST"})
                response = recv_json(s)
                logging.info(f"[HEARTBEAT] Enviado REQUEST -> {NEIGHBOR_MASTER} | Resp: {response}")
        except Exception as e:
            logging.warning(f"[HEARTBEAT] Falha ao enviar heartbeat: {e}")
        time.sleep(HEARTBEAT_INTERVAL)


def monitor_masters():
    while True:
        now = time.time()
        with lock:
            inactive = [m for m, i in known_masters.items() if now - i["last_seen"] > HEARTBEAT_TIMEOUT]
            for m in inactive:
                logging.warning(f"[MASTER] {m} inativo, removendo da lista.")
                del known_masters[m]
        time.sleep(5)


def distribute_tasks():
    while True:
        with lock:
            all_workers = {}
            all_workers.update({wid: w for wid, w in workers_filhos.items()})
            all_workers.update({wid: w for wid, w in workers_emprestados.items()})
            idle = [wid for wid, w in all_workers.items() if w["status"] == "PARADO"]
            while idle and pending_tasks:
                wid = idle.pop(0)
                task = pending_tasks.pop(0)
                w = all_workers[wid]
                try:
                    with socket.socket() as s:
                        s.connect((w["host"], w["port"]))
                        send_json(s, {
                            "type": "assign_task",
                            "payload": task,
                            "MASTER_HOST": HOST,
                            "MASTER_PORT": PORT
                        })
                    if wid in workers_filhos:
                        workers_filhos[wid]["status"] = "OCUPADO"
                    elif wid in workers_emprestados:
                        workers_emprestados[wid]["status"] = "OCUPADO"
                    logging.info(f"[TASK] Enviado {task['task_id']} -> {wid}")
                except Exception as e:
                    logging.error(f"[TASK] Falha ao enviar tarefa para {wid}: {e}")
                    workers_filhos.pop(wid, None)
                    workers_emprestados.pop(wid, None)
        time.sleep(1)


def monitor_load():
    """Monitora carga e gerencia empréstimo/devolução de workers"""
    while True:
        with lock:
            count = len(pending_tasks)
            borrowed = list(workers_emprestados.keys())
        
        # Saturação: requisita suporte
        if count >= THRESHOLD:
            logging.warning(f"[LOAD] {count} tarefas pendentes - requisitando suporte")
            request_support()
        
        # Normalização: devolve workers emprestados
        elif count < THRESHOLD and borrowed:
            logging.info(f"[LOAD] Normalizado ({count} pendentes). Devolvendo {len(borrowed)} workers")
            release_borrowed_workers(borrowed)
        
        time.sleep(3)


def release_borrowed_workers(borrowed_ids):
    """Inicia devolução de workers emprestados (|5.1|)"""
    # Agrupa por master de origem
    by_origin = {}
    with lock:
        for wid in borrowed_ids:
            if wid in workers_emprestados:
                origin = workers_emprestados[wid].get("original_master")
                if origin:
                    by_origin.setdefault(origin, []).append(wid)
    
    # Envia COMMAND_RELEASE para cada master de origem
    for origin, wid_list in by_origin.items():
        # Libera em lotes
        for i in range(0, len(wid_list), RELEASE_BATCH):
            batch = wid_list[i:i+RELEASE_BATCH]
            threading.Thread(
                target=send_release_command,
                args=(origin, batch),
                daemon=True
            ).start()
            time.sleep(0.5)


def send_release_command(origin_master, worker_ids):
    """Envia COMMAND_RELEASE ao master original (|5.1|)"""
    # Parse origin_master (pode ser "host:port" ou só "host")
    try:
        if ":" in origin_master:
            host, port = origin_master.split(":")
            target = (host, int(port))
        else:
            target = (origin_master, NEIGHBOR_MASTER[1])
    except Exception:
        target = NEIGHBOR_MASTER
    
    payload = {
        "MASTER": f"{HOST}:{PORT}",
        "TASK": "COMMAND_RELEASE",
        "WORKERS": worker_ids
    }
    
    logging.info(f"[COMMAND_RELEASE] Enviando para {target}: {worker_ids}")
    
    try:
        with socket.socket() as s:
            s.settimeout(5)
            s.connect(target)
            send_json(s, payload)
            
            # Aguarda ACK (|5.2|)
            response = recv_json(s)
            if response and response.get("RESPONSE") == "RELEASE_ACK":
                logging.info(f"[RELEASE_ACK] Recebido de {target}")
            else:
                logging.warning(f"[RELEASE_ACK] Não recebido de {target}")
                
    except Exception as e:
        logging.error(f"[COMMAND_RELEASE] Erro ao enviar para {target}: {e}")


def order_workers_return(worker_ids):
    """Ordena workers a retornarem (|5.3|)"""
    for wid in worker_ids:
        with lock:
            w = workers_emprestados.get(wid) or workers_filhos.get(wid)
            if not w:
                logging.warning(f"[RETURN] Worker {wid} não encontrado")
                continue
        
        payload = {
            "MASTER": MASTER_ID,
            "TASK": "RETURN",
            "MASTER_RETURN": f"{HOST}:{PORT}",
            "MASTER_RETURN_HOST": HOST,
            "MASTER_RETURN_PORT": PORT
        }
        
        try:
            with socket.socket() as s:
                s.settimeout(5)
                s.connect((w["host"], w["port"]))
                send_json(s, payload)
                logging.info(f"[RETURN] Comando enviado para worker {wid}")
        except Exception as e:
            logging.error(f"[RETURN] Erro ao enviar para {wid}: {e}")


def notify_worker_returned(requester_master, worker_id):
    """Notifica master que solicitou release que worker retornou (|5.5|)"""
    # Parse requester_master
    try:
        if ":" in requester_master:
            host, port = requester_master.split(":")
            target = (host, int(port))
        else:
            target = (requester_master, NEIGHBOR_MASTER[1])
    except Exception:
        target = NEIGHBOR_MASTER
    
    payload = {
        "MASTER": MASTER_ID,
        "RESPONSE": "WORKER_RETURNED",
        "WORKERS": [worker_id]
    }
    
    logging.info(f"[WORKER_RETURNED] Notificando {target} sobre retorno de {worker_id}")
    
    try:
        with socket.socket() as s:
            s.settimeout(5)
            s.connect(target)
            send_json(s, payload)
            logging.info(f"[WORKER_RETURNED] Enviado para {target}")
    except Exception as e:
        logging.error(f"[WORKER_RETURNED] Erro ao notificar {target}: {e}")


def request_support():
    try:
        with socket.socket() as s:
            s.settimeout(5)
            s.connect(NEIGHBOR_MASTER)
            send_json(s, {"TASK": "WORKER_REQUEST", "WORKERS_NEEDED": 5})
            response = recv_json(s)
            if response and response.get("TASK") == "WORKER_RESPONSE" and response.get("STATUS") == "ACK":
                for w in response["WORKERS"]:
                    wid = w["WORKER_UUID"]
                    with lock:
                        workers_emprestados[wid] = {
                            "host": w["host"],
                            "port": w["port"],
                            "status": "PARADO",
                            "original_master": w.get("MASTER_ORIGIN")
                        }
                logging.info(f"[SUPORTE] Recebidos {len(response['WORKERS'])} workers de apoio")
            else:
                logging.warning("[SUPORTE] Pedido negado (NACK)")
    except Exception as e:
        logging.error(f"[SUPORTE] Erro ao solicitar suporte: {e}")


def generate_tasks():
    i = 1
    while True:
        task = {"task_id": f"T{i}", "workload": [random.randint(1, 100)]}
        with lock:
            pending_tasks.append(task)
        logging.info(f"[GERADOR] Nova tarefa criada: {task['task_id']}")
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
            logging.info(f"[STATUS] Pendentes: {len(pending_tasks)} | Filhos: {len(workers_filhos)} | Emprestados: {len(workers_emprestados)} | Masters ativos: {len(known_masters)}")

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

        # 1.1 | Heartbeat REQUEST (A → B)
        if msg.get("TASK") == "HEARTBEAT" and "RESPONSE" not in msg:
            response = {
                "SERVER_UUID": MASTER_ID,
                "TASK": "HEARTBEAT",
                "RESPONSE": "ALIVE"
            }
            send_json(conn, response)
            with lock:
                known_masters[addr[0]] = {"last_seen": time.time()}
            logging.info(f"[HEARTBEAT] Recebido REQUEST de {addr[0]}, respondido ALIVE")
            return

        # 1.2 | Heartbeat RESPONSE (B → A)
        if msg.get("TASK") == "HEARTBEAT" and msg.get("RESPONSE") == "ALIVE":
            with lock:
                known_masters[addr[0]] = {"last_seen": time.time()}
            logging.info(f"[HEARTBEAT] Recebido RESPONSE ALIVE de {addr[0]}")
            return

        # 2.1 | Pedir Tarefa (Worker → Servidor) - Worker normal
        if msg.get("WORKER") == "ALIVE" and "SERVER_UUID" not in msg:
            wid = msg.get("WORKER_UUID")
            with lock:
                if wid in workers_filhos:
                    workers_filhos[wid]["status"] = "PARADO"
                    idle_workers = [w for w in workers_filhos.values() if w["status"] == "PARADO"]
                    logging.info(f"[WORKER FILHO] {wid} pede tarefa. Workers ociosos: {len(idle_workers)}")
                    
                    # Verifica se há tarefas pendentes
                    if pending_tasks:
                        task = pending_tasks.pop(0)
                        workers_filhos[wid]["status"] = "OCUPADO"
                        # 2.2 | Entregar Tarefa
                        response = {
                            "TASK": "QUERY",
                            "USER": task.get("workload")
                        }
                        send_json(conn, response)
                        logging.info(f"[TASK] Entregue {task['task_id']} para worker {wid}")
                    else:
                        # 2.3 | Sem Tarefa
                        send_json(conn, {"TASK": "NO_TASK"})
                        logging.info(f"[NO_TASK] Sem tarefas para worker {wid}")
                else:
                    # Novo worker se registrando
                    port = msg.get("port", 6000)
                    workers_filhos[wid] = {
                        "host": addr[0],
                        "port": port,
                        "status": "PARADO"
                    }
                    send_json(conn, {"TASK": "NO_TASK"})
                    logging.info(f"[WORKER FILHO] Registrado {wid} de {addr[0]}:{port}")
            return

        # 2.1b | Pedir Tarefa - Emprestado (Worker → Servidor)
        if msg.get("WORKER") == "ALIVE" and "SERVER_UUID" in msg:
            wid = msg.get("WORKER_UUID")
            origin_server = msg.get("SERVER_UUID")
            port = msg.get("port", 6000)
            
            with lock:
                if wid not in workers_emprestados:
                    # Novo worker emprestado se registrando
                    workers_emprestados[wid] = {
                        "host": addr[0],
                        "port": port,
                        "status": "PARADO",
                        "original_master": origin_server
                    }
                    logging.info(f"[WORKER EMPRESTADO] Registrado {wid} de servidor {origin_server}")
                    
                    # Notifica master de origem que worker chegou (4.3)
                    threading.Thread(
                        target=notify_worker_returned,
                        args=(origin_server, wid),
                        daemon=True
                    ).start()
                else:
                    workers_emprestados[wid]["status"] = "PARADO"
                
                # Verifica se há tarefas pendentes
                if pending_tasks:
                    task = pending_tasks.pop(0)
                    workers_emprestados[wid]["status"] = "OCUPADO"
                    # 2.2 | Entregar Tarefa
                    response = {
                        "TASK": "QUERY",
                        "USER": task.get("workload")
                    }
                    send_json(conn, response)
                    logging.info(f"[TASK] Entregue {task['task_id']} para worker emprestado {wid}")
                else:
                    # 2.3 | Sem Tarefa
                    send_json(conn, {"TASK": "NO_TASK"})
                    logging.info(f"[NO_TASK] Sem tarefas para worker emprestado {wid}")
            return

        # 3.1 | Pedido de Worker (A → B)
        if msg.get("TASK") == "WORKER_REQUEST":
            requestor_info = msg.get("REQUESTOR_INFO", {})
            requestor_ip = requestor_info.get("ip", addr[0])
            requestor_port = requestor_info.get("port", 5001)
            
            logging.info(f"[WORKER_REQUEST] Pedido de workers de {requestor_ip}:{requestor_port}")
            
            with lock:
                available = [(wid, w) for wid, w in workers_filhos.items() if w["status"] == "PARADO"]

            if available and len(available) >= 1:  # Empresta se tiver pelo menos 1 disponível
                # Oferece até 2 workers (ou quantos tiver disponível)
                to_offer = available[:min(2, len(available))]
                offered_uuids = []
                
                logging.info(f"[EMPRÉSTIMO] Iniciando empréstimo de {len(to_offer)} workers para {requestor_ip}:{requestor_port}")
                
                for wid, w in to_offer:
                    offered_uuids.append(wid)
                    with lock:
                        # Marcar como transferido
                        workers_filhos[wid]["status"] = "TRANSFERIDO"
                    
                    logging.info(f"[EMPRÉSTIMO] Redirecionando worker {wid} para {requestor_ip}:{requestor_port}")
                    
                    # 2.6 | Comando: Redirecionar (Servidor Dono → Worker)
                    threading.Thread(
                        target=send_redirect_to_worker,
                        args=(wid, w, requestor_ip, requestor_port),
                        daemon=True
                    ).start()
                
                # 3.2 | Resposta Disponível (B → A)
                response = {
                    "SERVER_UUID": MASTER_ID,
                    "RESPONSE": "AVAILABLE",
                    "WORKERS_UUID": offered_uuids
                }
                send_json(conn, response)
                logging.info(f"[WORKER_RESPONSE] Enviando AVAILABLE com {len(offered_uuids)} workers")
            else:
                # 3.3 | Resposta Indisponível (B → A)
                response = {
                    "SERVER_UUID": MASTER_ID,
                    "RESPONSE": "UNAVAILABLE"
                }
                send_json(conn, response)
                logging.warning(f"[WORKER_RESPONSE] UNAVAILABLE - sem workers disponíveis para emprestar")
            return

        # 2.4 | Reportar Status (Worker → Servidor)
        if msg.get("STATUS") in ["OK", "NOK"]:
            wid = msg.get("WORKER_UUID")
            task_status = msg.get("STATUS")
            task_type = msg.get("TASK")
            
            with lock:
                if wid in workers_filhos:
                    workers_filhos[wid]["status"] = "PARADO"
                elif wid in workers_emprestados:
                    workers_emprestados[wid]["status"] = "PARADO"
            
            # 2.5 | Confirmar Status (Servidor → Worker)
            send_json(conn, {"STATUS": "ACK"})
            logging.info(f"[STATUS] Worker {wid} reportou {task_status} para tarefa {task_type}")
            return

        # 4.1 | Notificar Liberação (A → B)
        if msg.get("TASK") == "COMMAND_RELEASE":
            requester_uuid = msg.get("SERVER_UUID")
            requester_info = msg.get("REQUESTOR_INFO", {})
            requester_port = requester_info.get("port", NEIGHBOR_MASTER[1])
            worker_list = msg.get("WORKERS_UUID", [])
            logging.info(f"[COMMAND_RELEASE] Recebido de {requester_uuid} para workers {worker_list}")
            
            # 4.2 | Confirmar Liberação (B → A)
            ack = {
                "SERVER_UUID": MASTER_ID,
                "RESPONSE": "RELEASE_ACK",
                "WORKERS_UUID": worker_list
            }
            send_json(conn, ack)
            logging.info(f"[RELEASE_ACK] Enviado para {requester_uuid}")
            
            # Marca workers como pendentes de retorno
            with lock:
                for wid in worker_list:
                    pending_releases[wid] = addr[0]
            
            # Envia comando RETURN aos workers (2.7)
            threading.Thread(
                target=order_workers_return, 
                args=(worker_list, addr[0], requester_port), 
                daemon=True
            ).start()
            return

        # 4.3 | Confirmar Recebimento (B → A) - Worker disponível/retornou
        if msg.get("RESPONSE") == "RELEASE_COMPLETED":
            origin_uuid = msg.get("SERVER_UUID")
            worker_list = msg.get("WORKERS_UUID", [])
            logging.info(f"[RELEASE_COMPLETED] Recebido de {origin_uuid}: workers {worker_list} estão disponíveis/retornaram")
            
            # Marca workers como disponíveis novamente (caso tenham sido TRANSFERIDO)
            with lock:
                for wid in worker_list:
                    if wid in workers_filhos and workers_filhos[wid]["status"] == "TRANSFERIDO":
                        workers_filhos[wid]["status"] = "PARADO"
                        logging.info(f"[DISPONÍVEL] Worker {wid} retornou e está PARADO")
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
                # 1.1 | Pergunta (A → B)
                send_json(s, {
                    "SERVER_UUID": MASTER_ID,
                    "TASK": "HEARTBEAT"
                })
                response = recv_json(s)
                logging.info(f"[HEARTBEAT] Enviado para {NEIGHBOR_MASTER} | Resp: {response}")
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


def send_redirect_to_worker(worker_id, worker_info, target_ip, target_port):
    """Envia comando REDIRECT para worker (2.6)"""
    payload = {
        "TASK": "REDIRECT",
        "SERVER_REDIRECT": {
            "ip": target_ip,
            "port": target_port
        }
    }
    
    try:
        with socket.socket() as s:
            s.settimeout(5)
            s.connect((worker_info["host"], worker_info["port"]))
            send_json(s, payload)
            logging.info(f"[REDIRECT] Comando enviado para worker {worker_id} -> {target_ip}:{target_port}")
    except Exception as e:
        logging.error(f"[REDIRECT] Erro ao enviar comando para worker {worker_id}: {e}")


def order_workers_return(worker_ids, return_ip, return_port):
    """Envia comando RETURN para workers (2.7)"""
    for wid in worker_ids:
        with lock:
            w = workers_emprestados.get(wid) or workers_filhos.get(wid)
            if not w:
                logging.warning(f"[RETURN] Worker {wid} não encontrado")
                continue
        
        # 2.7 | Comando: Retornar (Servidor Temporário → Worker)
        payload = {
            "TASK": "RETURN",
            "SERVER_RETURN": {
                "ip": return_ip,
                "port": return_port
            }
        }
        
        try:
            with socket.socket() as s:
                s.settimeout(5)
                s.connect((w["host"], w["port"]))
                send_json(s, payload)
                logging.info(f"[RETURN] Comando enviado para worker {wid} -> {return_ip}:{return_port}")
                
                # Remove worker emprestado da lista local imediatamente
                with lock:
                    if wid in workers_emprestados:
                        workers_emprestados.pop(wid)
                        logging.info(f"[RETURN] Worker {wid} removido da lista de emprestados")
                        
        except Exception as e:
            logging.error(f"[RETURN] Erro ao enviar comando para {wid}: {e}")


def notify_worker_returned(origin_master_info, worker_id):
    """Notifica master de origem que worker está disponível/retornou (4.3)"""
    # Parse origin_master_info (pode ser "ip:port" ou só "ip")
    try:
        if ":" in str(origin_master_info):
            host, port = origin_master_info.split(":")
            target = (host, int(port))
        else:
            target = (origin_master_info, NEIGHBOR_MASTER[1])
    except Exception:
        target = NEIGHBOR_MASTER
    
    # 4.3 | Confirmar Recebimento (B → A)
    payload = {
        "SERVER_UUID": MASTER_ID,
        "RESPONSE": "RELEASE_COMPLETED",
        "WORKERS_UUID": [worker_id]
    }
    
    logging.info(f"[RELEASE_COMPLETED] Notificando {target} sobre disponibilidade de {worker_id}")
    
    try:
        with socket.socket() as s:
            s.settimeout(5)
            s.connect(target)
            send_json(s, payload)
            logging.info(f"[RELEASE_COMPLETED] Enviado para {target}")
    except Exception as e:
        logging.error(f"[RELEASE_COMPLETED] Erro ao notificar {target}: {e}")


def distribute_tasks():
    """Distribui tarefas para workers ociosos"""
    while True:
        with lock:
            all_workers = {}
            all_workers.update({wid: w for wid, w in workers_filhos.items()})
            all_workers.update({wid: w for wid, w in workers_emprestados.items()})
            idle = [wid for wid, w in all_workers.items() if w["status"] == "PARADO"]
        
        # Nota: A distribuição agora é feita quando o worker pede tarefa (pull model)
        # Esta função pode ser removida ou usada para monitoramento
        time.sleep(1)


def monitor_load():
    """Monitora carga e gerencia empréstimo/devolução de workers"""
    while True:
        with lock:
            count = len(pending_tasks)
            borrowed = list(workers_emprestados.keys())
            # Conta workers filhos ociosos
            idle_children = [wid for wid, w in workers_filhos.items() if w["status"] == "PARADO"]
        
        # Saturação: requisita suporte
        if count >= THRESHOLD:
            logging.warning(f"[LOAD] {count} tarefas pendentes - requisitando suporte")
            request_support()
        
        # Normalização: devolve workers emprestados (< 10 tarefas)
        elif count < THRESHOLD and borrowed:
            logging.info(f"[LOAD] Normalizado ({count} pendentes). Devolvendo {len(borrowed)} workers")
            release_borrowed_workers(borrowed)
        
        time.sleep(3)


def release_borrowed_workers(borrowed_ids):
    """Inicia devolução de workers emprestados (4.1)"""
    # Agrupa por master de origem
    by_origin = {}
    with lock:
        for wid in borrowed_ids:
            if wid in workers_emprestados:
                origin = workers_emprestados[wid].get("original_master")
                if origin:
                    by_origin.setdefault(origin, []).append(wid)
    
    # Envia COMMAND_RELEASE para cada master de origem
    for origin_uuid, wid_list in by_origin.items():
        # Libera em lotes
        for i in range(0, len(wid_list), RELEASE_BATCH):
            batch = wid_list[i:i+RELEASE_BATCH]
            threading.Thread(
                target=send_release_command,
                args=(origin_uuid, batch),
                daemon=True
            ).start()
            time.sleep(0.5)


def send_release_command(origin_uuid, worker_ids):
    """Envia COMMAND_RELEASE ao master original (4.1)"""
    target = NEIGHBOR_MASTER
    
    # 4.1 | Notificar Liberação (A → B)
    payload = {
        "SERVER_UUID": MASTER_ID,
        "TASK": "COMMAND_RELEASE",
        "WORKERS_UUID": worker_ids,
        "REQUESTOR_INFO": {
            "ip": HOST,
            "port": PORT
        }
    }
    
    logging.info(f"[COMMAND_RELEASE] Enviando para {target}: {worker_ids}")
    
    try:
        with socket.socket() as s:
            s.settimeout(5)
            s.connect(target)
            send_json(s, payload)
            
            # Aguarda ACK (4.2)
            response = recv_json(s)
            if response and response.get("RESPONSE") == "RELEASE_ACK":
                logging.info(f"[RELEASE_ACK] Recebido de {target}")
            else:
                logging.warning(f"[RELEASE_ACK] Não recebido de {target}")
                
    except Exception as e:
        logging.error(f"[COMMAND_RELEASE] Erro ao enviar para {target}: {e}")


def request_support():
    """Solicita workers emprestados quando saturado (3.1)"""
    try:
        with socket.socket() as s:
            s.settimeout(5)
            s.connect(NEIGHBOR_MASTER)
            
            # 3.1 | Pedido de Worker (A → B)
            send_json(s, {
                "TASK": "WORKER_REQUEST",
                "REQUESTOR_INFO": {
                    "ip": HOST,
                    "port": PORT
                }
            })
            
            response = recv_json(s)
            
            # 3.2 | Resposta Disponível
            if response and response.get("RESPONSE") == "AVAILABLE":
                workers_uuids = response.get("WORKERS_UUID", [])
                logging.info(f"[SUPORTE] AVAILABLE: {len(workers_uuids)} workers serão redirecionados")
                # Os workers se conectarão automaticamente usando payload 2.1b
            
            # 3.3 | Resposta Indisponível
            elif response and response.get("RESPONSE") == "UNAVAILABLE":
                logging.warning("[SUPORTE] UNAVAILABLE - vizinho sem workers disponíveis")
            else:
                logging.warning("[SUPORTE] Resposta inesperada ou timeout")
                
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
import socket
import json
import threading
import time
import uuid
import random
import logging
import psutil
import sys
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)


NEIGHBOR_MASTER = ("10.62.217.22", 5000)
NEIGHBOR_MASTER = ("10.62.217.199", 5000)
NEIGHBOR_MASTER = ("10.62.217.212", 5000)
PORT = 5000
MASTER_ID = 'SERVER_3'
HOST = "10.62.217.16"
THRESHOLD = 10  
HEARTBEAT_INTERVAL = 5
HEARTBEAT_TIMEOUT = 15
RELEASE_BATCH = 2 

# Separação das listas de workers
workers_filhos = {}         # wid -> {host, port, status}
workers_emprestados = {}    # wid -> {host, port, status, original_master}
pending_tasks = []
known_masters = {}
pending_releases = {} 
lock = threading.Lock()

def send_json(sock, obj):
    data = json.dumps(obj) + "\n"
    sock.sendall(data.encode())

def recv_json(conn, timeout=5):
    conn.settimeout(timeout)
    buf = b""
    try:
        while b"\n" not in buf:
            chunk = conn.recv(4096)
            if not chunk: break
            buf += chunk
    except socket.timeout: pass
    except Exception as e: logging.debug(f"[recv_json] erro: {e}")
    if not buf: return None
    try:
        raw = buf.decode(errors="ignore")
        return json.loads(raw.split("\n")[0])
    except Exception as e:
        pass

# logging.error(f"[recv_json] erro parse: {e}")
# return None

def handle_client(conn, addr):
    try:
        msg = recv_json(conn)
        if not msg: return
        # logging.info(f"[RECV] {addr} -> {msg}") # Comentei para limpar o log

        if msg.get("TASK") == "HEARTBEAT" and "RESPONSE" not in msg:
            response = {"SERVER_UUID": MASTER_ID, "TASK": "HEARTBEAT", "RESPONSE": "ALIVE"}
            send_json(conn, response)
            with lock: known_masters[addr[0]] = {"last_seen": time.time(), "status": "available"}
            return

        if msg.get("TASK") == "HEARTBEAT" and msg.get("RESPONSE") == "ALIVE":
            with lock: known_masters[addr[0]] = {"last_seen": time.time(), "status": "available"}
            return

        if msg.get("WORKER") == "ALIVE" and "SERVER_UUID" not in msg:
            wid = msg.get("WORKER_UUID")
            with lock:
                if wid in workers_filhos:
                    if workers_filhos[wid]["status"] == "TRANSFERIDO": workers_filhos[wid]["status"] = "PARADO"
                    workers_filhos[wid]["status"] = "PARADO"
                    if pending_tasks:
                        task = pending_tasks.pop(0)
                        workers_filhos[wid]["status"] = "OCUPADO"
                        send_json(conn, {"TASK": "QUERY", "USER": task.get("workload")})
                        logging.info(f"[TASK] Entregue {task['task_id']} para worker {wid}")
                    else:
                        send_json(conn, {"TASK": "NO_TASK"})
                else:
                    port = msg.get("port", 6000)
                    workers_filhos[wid] = {"host": addr[0], "port": port, "status": "PARADO"}
                    send_json(conn, {"TASK": "NO_TASK"})
                    logging.info(f"[WORKER FILHO] Registrado {wid}")
            return

        if msg.get("WORKER") == "ALIVE" and "SERVER_UUID" in msg:
            wid = msg.get("WORKER_UUID")
            origin_server = msg.get("SERVER_UUID")
            port = msg.get("port", 6000)
            with lock:
                if wid not in workers_emprestados:
                    workers_emprestados[wid] = {"host": addr[0], "port": port, "status": "PARADO", "original_master": origin_server}
                    logging.info(f"[WORKER EMPRESTADO] Registrado {wid} de {origin_server}")
                    threading.Thread(target=notify_worker_returned, args=(origin_server, wid), daemon=True).start()
                else:
                    workers_emprestados[wid]["status"] = "PARADO"
                
                if pending_tasks:
                    task = pending_tasks.pop(0)
                    workers_emprestados[wid]["status"] = "OCUPADO"
                    send_json(conn, {"TASK": "QUERY", "USER": task.get("workload")})
                else:
                    send_json(conn, {"TASK": "NO_TASK"})
            return

        if msg.get("TASK") == "WORKER_REQUEST":
            requestor_info = msg.get("REQUESTOR_INFO", {})
            req_ip = requestor_info.get("ip", addr[0])
            req_port = requestor_info.get("port", PORT)
            logging.info(f"[WORKER_REQUEST] Pedido de {req_ip}:{req_port}")
            with lock: available = [(wid, w) for wid, w in workers_filhos.items() if w["status"] == "PARADO"]
            if available:
                to_offer = available[:min(2, len(available))]
                offered_uuids = []
                for wid, w in to_offer:
                    offered_uuids.append(wid)
                    with lock: workers_filhos[wid]["status"] = "TRANSFERIDO"
                    threading.Thread(target=send_redirect_to_worker, args=(wid, w, req_ip, req_port), daemon=True).start()
                send_json(conn, {"SERVER_UUID": MASTER_ID, "RESPONSE": "AVAILABLE", "WORKERS_UUID": offered_uuids})
                logging.info(f"[WORKER_RESPONSE] Emprestando {len(offered_uuids)} workers")
            else:
                send_json(conn, {"SERVER_UUID": MASTER_ID, "RESPONSE": "UNAVAILABLE"})
            return

        if msg.get("STATUS") in ["OK", "NOK"]:
            wid = msg.get("WORKER_UUID")
            with lock:
                if wid in workers_filhos: workers_filhos[wid]["status"] = "PARADO"
                elif wid in workers_emprestados: workers_emprestados[wid]["status"] = "PARADO"
            send_json(conn, {"STATUS": "ACK"})
            return

        if msg.get("TASK") == "COMMAND_RELEASE":
            worker_list = msg.get("WORKERS_UUID", [])
            requester_info = msg.get("REQUESTOR_INFO", {})
            requester_port = requester_info.get("port", NEIGHBOR_MASTER[1])
            send_json(conn, {"SERVER_UUID": MASTER_ID, "RESPONSE": "RELEASE_ACK", "WORKERS_UUID": worker_list})
            logging.info(f"[RELEASE] Liberando workers: {worker_list}")
            threading.Thread(target=order_workers_return, args=(worker_list, addr[0], requester_port), daemon=True).start()
            return

        if msg.get("RESPONSE") == "RELEASE_COMPLETED":
            worker_list = msg.get("WORKERS_UUID", [])
            with lock:
                for wid in worker_list:
                    if wid in workers_filhos and workers_filhos[wid]["status"] == "TRANSFERIDO":
                        workers_filhos[wid]["status"] = "PARADO"
                        logging.info(f"[RETORNOU] Worker {wid} voltou para casa")
            return

    except Exception as e: logging.error(f"[ERRO handle_client] {e}")
    finally: conn.close()

def start_master_server():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind((HOST, PORT))
        s.listen(10)
        logging.info(f"[SERVER] Servidor iniciado em {HOST}:{PORT}")
        while True:
            conn, addr = s.accept()
            threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()
    except Exception as e:
        logging.error(f"[SERVER CRITICAL] Não foi possível iniciar na porta {PORT}: {e}")

def heartbeat():
    while True:
        try:
            with socket.socket() as s:
                s.settimeout(5)
                s.connect(NEIGHBOR_MASTER)
                send_json(s, {"SERVER_UUID": MASTER_ID, "TASK": "HEARTBEAT"})
                recv_json(s)
                with lock: known_masters[NEIGHBOR_MASTER[0]] = {"last_seen": time.time(), "status": "available"}
        except Exception:
            with lock: 
                if NEIGHBOR_MASTER[0] in known_masters: known_masters[NEIGHBOR_MASTER[0]]["status"] = "unavailable"
        time.sleep(HEARTBEAT_INTERVAL)

def monitor_masters():
    while True:
        now = time.time()
        with lock:
            inactive = [m for m, i in known_masters.items() if now - i["last_seen"] > HEARTBEAT_TIMEOUT]
            for m in inactive: del known_masters[m]
        time.sleep(5)

def send_redirect_to_worker(worker_id, worker_info, target_ip, target_port):
    payload = {"TASK": "REDIRECT", "SERVER_REDIRECT": {"ip": target_ip, "port": target_port}}
    try:
        with socket.socket() as s:
            s.settimeout(5)
            s.connect((worker_info["host"], worker_info["port"]))
            send_json(s, payload)
    except Exception as e: logging.error(f"[REDIRECT] Erro: {e}")

def order_workers_return(worker_ids, return_ip, return_port):
    for wid in worker_ids:
        with lock: w = workers_emprestados.get(wid) or workers_filhos.get(wid)
        if not w: continue
        payload = {"TASK": "RETURN", "SERVER_RETURN": {"ip": return_ip, "port": return_port}}
        try:
            with socket.socket() as s:
                s.settimeout(5)
                s.connect((w["host"], w["port"]))
                send_json(s, payload)
                with lock: 
                    if wid in workers_emprestados: workers_emprestados.pop(wid)
        except Exception as e: logging.error(f"[RETURN] Erro: {e}")

def notify_worker_returned(origin_master_info, worker_id):
    target = NEIGHBOR_MASTER # Simplificado para vizinho direto
    payload = {"SERVER_UUID": MASTER_ID, "RESPONSE": "RELEASE_COMPLETED", "WORKERS_UUID": [worker_id]}
    try:
        with socket.socket() as s:
            s.settimeout(5)
            s.connect(target)
            send_json(s, payload)
    except Exception: pass

def distribute_tasks():
    while True: time.sleep(1)

def monitor_load():
    while True:
        with lock:
            count = len(pending_tasks)
            borrowed = list(workers_emprestados.keys())
        if count >= THRESHOLD:
            logging.warning(f"[LOAD] {count} tarefas - PEDINDO AJUDA")
            request_support()
        elif count < THRESHOLD and borrowed:
            logging.info(f"[LOAD] Normalizado. Devolvendo workers.")
            release_borrowed_workers(borrowed)
        time.sleep(3)

def release_borrowed_workers(borrowed_ids):
    by_origin = {}
    with lock:
        for wid in borrowed_ids:
            if wid in workers_emprestados:
                origin = workers_emprestados[wid].get("original_master")
                if origin: by_origin.setdefault(origin, []).append(wid)
    for origin_uuid, wid_list in by_origin.items():
        for i in range(0, len(wid_list), RELEASE_BATCH):
            batch = wid_list[i:i+RELEASE_BATCH]
            threading.Thread(target=send_release_command, args=(origin_uuid, batch), daemon=True).start()
            time.sleep(0.5)

def send_release_command(origin_uuid, worker_ids):
    target = NEIGHBOR_MASTER
    payload = {"SERVER_UUID": MASTER_ID, "TASK": "COMMAND_RELEASE", "WORKERS_UUID": worker_ids, "REQUESTOR_INFO": {"ip": HOST, "port": PORT}}
    try:
        with socket.socket() as s:
            s.settimeout(5)
            s.connect(target)
            send_json(s, payload)
            recv_json(s)
    except Exception: pass

def request_support():
    try:
        with socket.socket() as s:
            s.settimeout(5)
            s.connect(NEIGHBOR_MASTER)
            send_json(s, {"TASK": "WORKER_REQUEST", "REQUESTOR_INFO": {"ip": HOST, "port": PORT}})
            resp = recv_json(s)
            if resp and resp.get("RESPONSE") == "AVAILABLE":
                logging.info(f"[SUPORTE] Consegui ajuda!")
    except Exception: pass

def generate_tasks():
    i = 1
    while True:
        task = {"task_id": f"T{i}", "workload": [random.randint(1, 100)]}
        with lock: pending_tasks.append(task)
        logging.info(f"[GERADOR] Tarefa {task['task_id']}")
        i += 1
        time.sleep(16)

def send_metrics_loop():
    DASHBOARD_HOST = "10.62.217.32"
    DASHBOARD_PORT = 8000
    logging.info(f"[DASHBOARD] Iniciando reporte para {DASHBOARD_HOST}:{DASHBOARD_PORT}")
    while True:
        try:
            mem = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            cpu_percent = psutil.cpu_percent(interval=None)
            boot_time = datetime.fromtimestamp(psutil.boot_time())
            uptime_seconds = (datetime.now() - boot_time).total_seconds()

            with lock:
                total_registered = len(workers_filhos)
                received_workers = len(workers_emprestados)
                idle_count = sum(1 for w in workers_filhos.values() if w["status"] == "PARADO") + \
                             sum(1 for w in workers_emprestados.values() if w["status"] == "PARADO")
                running_count = sum(1 for w in workers_filhos.values() if w["status"] == "OCUPADO") + \
                                sum(1 for w in workers_emprestados.values() if w["status"] == "OCUPADO")
                borrowed_count = sum(1 for w in workers_filhos.values() if w["status"] == "TRANSFERIDO")
                workers_alive = total_registered + received_workers
                pending_count = len(pending_tasks)
                neighbors_list = []
                for ip, data in known_masters.items():
                    neighbors_list.append({
                        "server_uuid": ip, 
                        "status": data.get("status", "unknown"),
                        "last_heartbeat": datetime.fromtimestamp(data["last_seen"]).isoformat() + "Z"
                    })
            total_registered = 1
            running_count = int(running_count)
            workers_alive = int(total_registered)
            idle_count = int(idle_count)
            borrowed_count = int(borrowed_count)
            received_workers = int(received_workers)
            payload = {
                "server_uuid": MASTER_ID, "task": "performance_report", "timestamp": datetime.utcnow().isoformat() + "Z", "mensage_id": str(uuid.uuid4()),
                "performance": {
                    "system": {
                        "uptime_seconds": int(uptime_seconds), "load_average_1m": 0.0, "load_average_5m": 0.0,
                        "cpu": {"usage_percent": cpu_percent, "count_logical": psutil.cpu_count(logical=True), "count_physical": psutil.cpu_count(logical=False)},
                        "memory": {"total_mb": int(mem.total / (1024*1024)), "available_mb": int(mem.available / (1024*1024)), "percent_used": mem.percent, "memory_used": int(mem.used / (1024*1024))},
                        "disk": {"total_gb": round(disk.total / (1024**3), 2), "free_gb": round(disk.free / (1024**3), 2), "percent_used": disk.percent}
                    },
                    "farm_state": {
                        "workers": {"total_registered": total_registered, "workers_utilization": running_count, "workers_alive": workers_alive, "workers_idle": idle_count, "workers_borrowed": borrowed_count, "workers_recieved": received_workers, "workers_failed": 0},
                        "tasks": {"tasks_pending": pending_count, "tasks_running": running_count}
                    },
                    "config_thresholds": {"max_task": THRESHOLD},
                    "neighbors": neighbors_list,
                }
            }
            print(payload)
            s_dash = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s_dash.settimeout(10)
            s_dash.connect((DASHBOARD_HOST, DASHBOARD_PORT))
            send_json(s_dash, payload)
            s_dash.close()
        except Exception as e: logging.error(f"[DASHBOARD] Erro: {e}")
        time.sleep(10)

if __name__ == "__main__":
    threading.Thread(target=start_master_server, daemon=True).start()
    threading.Thread(target=heartbeat, daemon=True).start()
    threading.Thread(target=monitor_masters, daemon=True).start()
    threading.Thread(target=distribute_tasks, daemon=True).start()
    threading.Thread(target=monitor_load, daemon=True).start()
    threading.Thread(target=generate_tasks, daemon=True).start()
    threading.Thread(target=send_metrics_loop, daemon=True).start()

    while True:
        time.sleep(10)
        with lock:
            logging.info(f"[STATUS] Pendentes: {len(pending_tasks)} | Filhos: {len(workers_filhos)} | Emprestados: {len(workers_emprestados)} | Masters ativos: {len(known_masters)}")
import docker
import logging
import threading
import time
import socket
import json
from collections import deque

client = docker.from_env()
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

running_services = dict()
running_services_lock = threading.Lock()

sched_queue = deque()
sched_queue_lock = threading.Lock()


def get_node_info():
    nodes = client.nodes.list()
    node_info = []

    for node in nodes:
        node_data = {
            "ID": node.id,
            "Name": node.attrs["Description"]["Hostname"],
            "Status": node.attrs["Status"]["State"],
            "Role": node.attrs["Spec"]["Role"],
            "Availability": node.attrs["Spec"]["Availability"],
            "Platform": node.attrs["Description"]["Platform"]["Architecture"],
        }

        # Retrieve memory and CPU usage if available
        if "Description" in node.attrs and "Resources" in node.attrs["Description"]:
            resources = node.attrs["Description"]["Resources"]
            if "NanoCPUs" in resources:
                node_data["CPU"] = (
                    resources["NanoCPUs"] / 1e9
                )  # Convert from nanocpus to CPUs
            if "MemoryBytes" in resources:
                node_data["Memory"] = resources["MemoryBytes"] / (
                    1024 * 1024
                )  # Convert from bytes to MB

        node_info.append(node_data)

    return node_info


def get_available_worker_nodes(worker_nodes):
    nodes = client.nodes.list()
    available_worker_nodes = []
    for node in nodes:
        node_name = node.attrs["Description"]["Hostname"]
        if node.attrs["Status"]["State"] == "ready" and node_name in worker_nodes:
            available_worker_nodes.append(node_name)

    return available_worker_nodes


def get_worker_nodes_load():
    services_associated = dict()
    for service in client.services.list():
        node_name = service.attrs["Spec"]["TaskTemplate"]["Placement"]["Constraints"][
            0
        ].replace("node.hostname == ", "")
        my_service_name = service.name

        if node_name not in services_associated:
            services_associated[node_name] = []

        services_associated[node_name].append(my_service_name)
    return services_associated


def load_balance(available_worker_nodes, services_associated, service_limit):
    least_work_node = ""
    max_services_associated = 255
    for node in available_worker_nodes:
        if node not in services_associated:
            least_work_node = node
            break
        else:
            if (
                len(services_associated[node]) < max_services_associated
                and len(services_associated[node]) < service_limit[node]
            ):
                least_work_node = node
                max_services_associated = len(services_associated[node])
    return least_work_node


def check_schedulability(worker_nodes, service_limit):
    available_worker_nodes = get_available_worker_nodes(worker_nodes)
    services_associated = get_worker_nodes_load()
    work_node = load_balance(available_worker_nodes, services_associated, service_limit)
    return work_node


def create_service(service_name, work_node, task_request, secrets=None):
    image_name = "rafaelcalai633/wait-for-value:1.1.0"
    constraints = ["node.hostname == rpi5-node01"]
    command = [
        "python",
        "wait_for_value.py",
        str(task_request["ecxecution_time"]),
        service_name,
    ]
    secrets = secrets or []
    command = command or []

    resources = {
        "Limits": {
            "NanoCPUs": int(1 * 1e9),  # Convert CPU to NanoCPUs
            "MemoryBytes": int(256 * 1e6),  # Convert MB to Bytes
        }
    }

    constraints = [f"node.hostname == {work_node}"]

    logging.info(f"Service {service_name} with Constraints: {constraints} created!")
    return client.services.create(
        name=service_name,
        image=image_name,
        constraints=constraints,
        secrets=secrets,
        command=command,
        restart_policy={"Condition": "none"},
        resources=resources,
    )


def remove_service(service_id):
    client = docker.from_env()

    try:
        service = client.services.get(service_id)
        service.remove()
        # logging.info(f"Service {service_id} removed.")
    except docker.errors.NotFound:
        logging.error(f"Service {service_id} not found.")


def pause_service_containers(service_id):
    service_containers = client.services.get(service_id).tasks()
    for container in service_containers:
        container_command = dict()
        container_command["id"] = container["Status"]["ContainerStatus"]["ContainerID"]
        container_command["command"] = "pause"
        __send_message(container_command)
    logging.info(f"Service {service_id} paused.")


def unpause_service_containers(service_id):
    service_containers = client.services.get(service_id).tasks()
    for container in service_containers:
        container_command = dict()
        container_command["id"] = container["Status"]["ContainerStatus"]["ContainerID"]
        container_command["command"] = "unpause"
        __send_message(container_command)

    logging.info(f"Service {service_id} unpaused.")


def __send_message(container_command, host, port):
    client_socket = socket.socket()
    client_socket.connect((host, port))

    message = json.dumps(container_command)

    client_socket.send(message.encode())
    client_socket.close()


def sched_service(worker_nodes, service_limit, task_request):
    service_name = task_request["task_name"]

    work_node = check_schedulability(worker_nodes, service_limit)

    if work_node:
        service = create_service(service_name, work_node, task_request)
    else:
        sched_queue_lock.acquire()
        sched_queue.append(task_request)
        sched_queue_lock.release()
        logging.info(f"Cluster is busy, Service {service_name} was added to the queue")


def remove_service_thread(thread, config_data):
    worker_nodes = config_data["nodes"]["worker_nodes"]
    service_limit = config_data["nodes"]["service_limit"]

    while True:
        for service in client.services.list():
            if service.tasks(filters={"desired-state": ["shutdown"]}):
                logging.info(f"service stoped: {service.name}")
                remove_service(service.id)
                if sched_queue:
                    sched_queue_lock.acquire()
                    task_request = sched_queue.popleft()
                    sched_queue_lock.release()

                    logging.info(
                        f"Creating a service from the sched queue: {task_request['task_name']}"
                    )
                    sched_service(worker_nodes, service_limit, task_request)
        time.sleep(0.1)


def listen_service_request(thread, config_data):
    PORT = 8767
    HOST = "0.0.0.0"
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((HOST, PORT))
    logging.info(f"socket binded to port: {PORT}")

    server.listen()
    logging.info("socket is listening")
    thread = 0

    worker_nodes = config_data["nodes"]["worker_nodes"]
    service_limit = config_data["nodes"]["service_limit"]

    while True:
        connection, addr = server.accept()
        logging.info(f"Connetion from {addr}")

        while True:
            data = connection.recv(1024)
            if data:
                task_request = eval(data)

                sched_service(worker_nodes, service_limit, task_request)
                break


def main():
    with open("config.json", encoding="utf-8") as f:
        config_data = json.load(f)

    monitor_thread = threading.Thread(
        target=remove_service_thread, args=(1, config_data)
    )
    server_thread = threading.Thread(
        target=listen_service_request, args=(1, config_data)
    )
    monitor_thread.start()
    server_thread.start()

    monitor_thread.join()
    server_thread.join()


if __name__ == "__main__":
    logging.info("Real Time Cluster Scheduler started!")
    main()

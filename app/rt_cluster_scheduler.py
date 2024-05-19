import docker
import logging
import threading
import time
import socket
import json
from copy import deepcopy
from docker.errors import NotFound, APIError

client = docker.from_env()
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

running_services = dict()
running_services_lock = threading.Lock()

sched_queue = list()
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


def get_worker_nodes_load(available_worker_nodes):
    services_associated = dict()
    for worker_node in available_worker_nodes:
        services_associated[worker_node] = list()

    running_services_lock.acquire()
    copy_running_services = deepcopy(running_services)
    running_services_lock.release()

    for service in copy_running_services:
        services_associated[copy_running_services[service]["work_node"]].append(service)

    return services_associated


def available_worker_node(available_worker_nodes, services_associated, service_limit):
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


def pause_lower_priority_service(task_priority, worker_node_addresses):
    running_services_lock.acquire()
    copy_running_services = deepcopy(running_services)
    running_services_lock.release()

    lowest_priority_service = ""
    available_worker_node = ""
    lowest_priority = task_priority
    for service in copy_running_services:
        if copy_running_services[service]["priority"] > lowest_priority:
            lowest_priority = copy_running_services[service]["priority"]
            lowest_priority_service = service
            available_worker_node = copy_running_services[service]["work_node"]

    if available_worker_node:
        pause_service_containers(
            lowest_priority_service, worker_node_addresses[available_worker_node]
        )

    return available_worker_node


def load_balance(
    available_worker_nodes,
    services_associated,
    service_limit,
    task_request,
    worker_node_addresses,
):

    worker_node = available_worker_node(
        available_worker_nodes, services_associated, service_limit
    )
    if worker_node == "":
        worker_node = pause_lower_priority_service(
            task_request["priority"], worker_node_addresses
        )

    return worker_node


def check_schedulability(
    worker_nodes, service_limit, task_request, worker_node_addresses
):
    available_worker_nodes = get_available_worker_nodes(worker_nodes)
    services_associated = get_worker_nodes_load(available_worker_nodes)

    work_node = load_balance(
        available_worker_nodes,
        services_associated,
        service_limit,
        task_request,
        worker_node_addresses,
    )
    return work_node


def create_service(
    service_name, work_node, task_request, worker_node_addresses, secrets=None
):
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
    task_request["work_node"] = work_node
    task_request["service_state"] = "new"
    task_request["work_node_ip"] = worker_node_addresses[work_node]

    running_services_lock.acquire()
    running_services[service_name] = task_request
    running_services_lock.release()

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
        logging.info(f"Service {service.name} removed from Swarm cluster")
    except docker.errors.NotFound:
        logging.error(f"Service {service_id} not found.")


def add_sched_waiting_queue(task):
    index = 0
    sched_queue_lock.acquire()
    for service in sched_queue:
        if service["priority"] > task["priority"]:
            break
        index += 1

    sched_queue.insert(index, task)
    sched_queue_lock.release()


def pause_service_containers(service_name, worker_node_address):
    service = client.services.list(filters={"name": service_name})[0]
    service_containers = service.tasks()
    service_state = ""

    for container in service_containers:
        if container["Status"]["State"] == "complete":
            return

        if container["Status"]["State"] != "running":
            remove_service(service.id)
            logging.info(f"Service {service_name} removed.")
            service_state = "new"
        else:
            container_command = dict()
            container_command["id"] = container["Status"]["ContainerStatus"][
                "ContainerID"
            ]
            container_command["command"] = "pause"
            __send_message(container_command, worker_node_address)
            service_state = "paused"
            logging.info(f"Service {service_name} paused.")

    running_services_lock.acquire()
    if service_name in running_services:
        running_services[service_name]["service_state"] = service_state
        add_sched_waiting_queue(running_services[service_name])
        del running_services[service_name]
    running_services_lock.release()


def unpause_service_containers(task_request):
    service_name = task_request["task_name"]
    worker_node_address = task_request["work_node_ip"]

    service = client.services.list(filters={"name": service_name})[0]
    service_containers = service.tasks()
    for container in service_containers:
        container_command = dict()
        container_command["id"] = container["Status"]["ContainerStatus"]["ContainerID"]
        container_command["command"] = "unpause"
        __send_message(container_command, worker_node_address)

    logging.info(f"Service {service_name} unpaused.")


def __send_message(container_command, host):
    client_socket = socket.socket()
    client_socket.connect((host, 8770))

    message = json.dumps(container_command)

    client_socket.send(message.encode())
    client_socket.close()


def sched_service(worker_nodes, service_limit, task_request, worker_node_addresses):
    service_name = task_request["task_name"]

    work_node = check_schedulability(
        worker_nodes, service_limit, task_request, worker_node_addresses
    )

    if work_node:
        service = create_service(
            service_name, work_node, task_request, worker_node_addresses
        )
    else:
        task_request["service_state"] = "new"
        add_sched_waiting_queue(task_request)

        logging.info(f"Cluster is busy, Service {service_name} was added to the queue")


def remove_service_thread(thread, config_data):
    worker_nodes = config_data["nodes"]["worker_nodes"]
    service_limit = config_data["nodes"]["service_limit"]
    worker_node_addresses = config_data["nodes"]["ip_address"]

    while True:
        swarm_services = client.services.list()
        for service in swarm_services:
            try:
                service.reload()
                if service.tasks(filters={"desired-state": ["shutdown"]}):
                    logging.info(f"Service stopped: {service.name}")

                    free_node = None
                    running_services_lock.acquire()
                    if service.name in running_services:
                        free_node = running_services[service.name]["work_node"]
                        del running_services[service.name]
                    running_services_lock.release()

                    sched_queue_lock.acquire()
                    for item in sched_queue:
                        if "task_name" in item and item["task_name"] == service.name:
                            free_node = item["work_node"]
                            sched_queue.remove(item)
                            logging.warning(
                                f"Service {service.name} removed from deque."
                            )
                            break
                    sched_queue_lock.release()

                    remove_service(service.id)

                    sched_queue_lock.acquire()
                    task_request = None
                    if sched_queue:
                        for index, task in enumerate(sched_queue):
                            if task["service_state"] == "new":
                                task_request = sched_queue.pop(index)
                                break
                            elif task["work_node"] == free_node:
                                task_request = sched_queue.pop(index)
                                break
                    sched_queue_lock.release()

                    if task_request:
                        if task_request["service_state"] == "new":
                            logging.info(
                                f"Creating a service from the sched queue: {task_request['task_name']}"
                            )
                            sched_service(
                                worker_nodes,
                                service_limit,
                                task_request,
                                worker_node_addresses,
                            )
                        else:
                            logging.info(
                                f"Unpause a service from the sched queue: {task_request['task_name']}"
                            )
                            unpause_service_containers(task_request)
            except NotFound:
                logging.warning(f"Service {service.name} no longer exists, skipping.")
            except APIError as api_err:
                logging.error(f"API error occurred: {api_err}")
            except Exception as error:
                logging.error(f"In remove service an exception occurred: {error}")

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
    worker_node_addresses = config_data["nodes"]["ip_address"]

    while True:
        connection, addr = server.accept()
        logging.info(f"Connetion from {addr}")

        while True:
            data = connection.recv(1024)
            if data:
                task_request = eval(data)

                sched_service(
                    worker_nodes, service_limit, task_request, worker_node_addresses
                )
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

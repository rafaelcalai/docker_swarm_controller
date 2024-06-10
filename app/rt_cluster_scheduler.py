import docker
import logging
import threading
import time
import socket
import json
from copy import deepcopy
from docker.errors import NotFound, APIError
from datetime import datetime, timedelta

OVERHEAD = 5
client = docker.from_env()
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

running_services = dict()
running_services_lock = threading.Lock()

pending_services = list()
pending_services_lock = threading.Lock()


sched_queue = list()
sched_queue_lock = threading.Lock()

sched_event = threading.Semaphore(0)

def remove_all_services():
    services = client.services.list()

    if not services:
        logging.info("No services to remove.")
        return
    
    for service in services:
        logging.info(f'Removing service: {service.name}')
        service.remove()

    logging.info("All services have been removed.")

def create_pause_unpause_services(worker_nodes):

    available_nodes = get_available_worker_nodes(worker_nodes)
    
    image = "rafaelcalai633/pause_unpause_container:1.0.0"
    replicas = 1
    entrypoint = ["python3", "/app/pause_unpause_container.py"]
    restart_policy = docker.types.RestartPolicy(condition='any')
    
    endpoint_spec = docker.types.EndpointSpec(
        mode='vip',
        ports={8770: (8770, 'tcp', 'host')}
    )

    for node in available_nodes:
        service_name = f"pause_unpause_containers_{node}"
        constraints = [f"node.hostname == {node}"]
        client.services.create(
            name=service_name,
            image=image,
            command=entrypoint,
            constraints=constraints,
            mode=docker.types.ServiceMode('replicated', replicas=replicas),
            restart_policy=restart_policy,
            mounts=[docker.types.Mount(target='/var/run/docker.sock', source='/var/run/docker.sock', type='bind')],
            endpoint_spec=endpoint_spec
        )
        logging.info(f'Service {service_name} created successfully.')

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
    for service in running_services:
        if running_services[service]["work_node"] in available_worker_nodes:
            services_associated[running_services[service]["work_node"]].append(
                service
            )
    running_services_lock.release()
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


def update_sched_deadline(service):
    old_sched_deadline = running_services[service]["sched_deadline"]
    executed_time = datetime.now() - running_services[service]["service_started"]
    new_sched_deadline = old_sched_deadline + executed_time
    running_services[service]["sched_deadline"] = new_sched_deadline


def pause_lower_priority_service(task_sched_deadline, worker_node_addresses):
    lowest_priority_service = ""
    available_worker_node = ""
    lowest_sched_deadline_priority = task_sched_deadline
    for service in running_services:
        if (
            running_services[service]["sched_deadline"]
            > lowest_sched_deadline_priority
            and running_services[service]["service_state"] == "running"
        ):
            lowest_sched_deadline_priority = running_services[service][
                "sched_deadline"
            ]
            lowest_priority_service = service
            available_worker_node = running_services[service]["work_node"]
    return available_worker_node, lowest_priority_service


def load_balance(
    available_worker_nodes,
    services_associated,
    service_limit,
    task_request,
    worker_node_addresses,
):
    pause_service = None
    worker_node = available_worker_node(
        available_worker_nodes, services_associated, service_limit
    )
    if worker_node == "":
        worker_node, pause_service  = pause_lower_priority_service(
            task_request["sched_deadline"], worker_node_addresses
        )
    return worker_node, pause_service


def check_schedulability(
    worker_nodes, service_limit, task_request, worker_node_addresses
):
    available_worker_nodes = get_available_worker_nodes(worker_nodes)
    services_associated = get_worker_nodes_load(available_worker_nodes)
    work_node, pause_service = load_balance(
        available_worker_nodes,
        services_associated,
        service_limit,
        task_request,
        worker_node_addresses,
    )
    return work_node, pause_service


def create_service(
    service_name, work_node, task_request, worker_node_addresses, secrets=None
):
    image_name = "rafaelcalai633/wait-for-value:1.1.0"
    constraints = ["node.hostname == rpi5-node01"]
    command = [
        "python",
        "wait_for_value.py",
        str(task_request["execution_time"]),
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
    task_request["service_state"] = "pending"
    task_request["work_node_ip"] = worker_node_addresses[work_node]
    task_request["service_started"] = datetime.now()

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
        if service["sched_deadline"] >= task["sched_deadline"]:
            break
        index += 1
    sched_queue.insert(index, task)
    queue_size = len(sched_queue)
    sched_queue_lock.release()
    return index + 1, queue_size


def pause_service_containers(service_name, worker_node_address, available_worker_node):
    try:
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
                service_state = "paused"
                send_message(container_command, worker_node_address)
                logging.info(f"Service {service_name} paused.")
        
        running_services_lock.acquire()
        if service_name in running_services:
            running_services[service_name]["service_state"] = service_state
            update_sched_deadline(service_name)
            add_sched_waiting_queue(running_services[service_name])
            del running_services[service_name]
        running_services_lock.release()

        
        return available_worker_node
    except:
        logging.error("unpause container.")
        return ""
    


def unpause_service_containers(task_request):
    service_name = task_request["task_name"]
    worker_node_address = task_request["work_node_ip"]
    task_request["service_state"] = "unpause pending "

    service = client.services.list(filters={"name": service_name})[0]
    service_containers = service.tasks()
    for container in service_containers:
        container_command = dict()
        container_command["id"] = container["Status"]["ContainerStatus"]["ContainerID"]
        container_command["command"] = "unpause"
        send_message(container_command, worker_node_address)

    task_request["service_started"] = datetime.now()
    
    pending_services_lock.acquire()
    pending_services.append(service)
    pending_services_lock.release()
    
    running_services_lock.acquire()
    running_services[service_name] = task_request
    running_services_lock.release()
    
    logging.info(f"Service {service_name} unpaused.")


def send_message(container_command, host):
    client_socket = socket.socket()
    client_socket.connect((host, 8770))

    message = json.dumps(container_command)

    client_socket.send(message.encode())
    client_socket.close()


def sched_service(worker_nodes, service_limit, task_request, worker_node_addresses, work_node):
    service_name = task_request["task_name"]

    if work_node:
        service = create_service(
            service_name, work_node, task_request, worker_node_addresses
        )
        pending_services_lock.acquire()
        pending_services.append(service)
        pending_services_lock.release()
    else:
        task_request["service_state"] = "new"
        add_sched_waiting_queue(task_request)


def service_monitor_thread(thread):
    while True:
        if pending_services:
            pending_services_lock.acquire()
            pending_services_copy = pending_services
            pending_services_lock.release()

            for service in pending_services_copy:
                tasks = service.tasks()
                for task in tasks:
                    if task["Status"]["State"] == "running":
                        pending_services_lock.acquire()
                        pending_services.remove(service)
                        pending_services_lock.release()

                        running_services_lock.acquire()
                        running_services[service.name]["service_state"] = "running"
                        running_services_lock.release()

                        logging.info(
                            f"Service {service.name} container is {task['Status']['State']}"
                        )
                        sched_event.release()

        time.sleep(0.1)


def remove_service_thread(thread):
    while True:
        swarm_services = client.services.list()
        for service in swarm_services:
            try:
                service.reload()
                if service.tasks(filters={"desired-state": ["shutdown"]}):
                    logging.debug(f"Service stopped: {service.name}")

                    running_services_lock.acquire()
                    if service.name in running_services:
                        del running_services[service.name]
                    running_services_lock.release()

                    sched_queue_lock.acquire()
                    for item in sched_queue:
                        if "task_name" in item and item["task_name"] == service.name:
                            sched_queue.remove(item)
                            logging.warning(
                                f"Service {service.name} removed from queue."
                            )
                            break
                    sched_queue_lock.release()

                    remove_service(service.id)
                    sched_event.release()

            except NotFound:
                logging.warning(f"Service {service.name} no longer exists, skipping.")
            except APIError as api_err:
                logging.error(f"API error occurred: {api_err}")
            except Exception as error:
                logging.error(f"In remove service an exception occurred: {error}")
            

        time.sleep(0.1)


def service_request_thread(thread):
    PORT = 8767
    HOST = "0.0.0.0"
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((HOST, PORT))
    logging.info(f"socket binded to port: {PORT}")

    server.listen()
    logging.info("socket is listening")

    while True:
        connection, addr = server.accept()
        logging.debug(f"Connetion from {addr}")

        while True:
            data = connection.recv(1024)
            if data:
                task_request = eval(data)

                task_request["date_deadline"] = datetime.now() + timedelta(
                    seconds=task_request["deadline"]
                )
                task_request["sched_deadline"] = task_request[
                    "date_deadline"
                ] - timedelta(seconds=(OVERHEAD+task_request["execution_time"]))
                task_request["service_started"] = 0
                task_request["service_state"] = "new"
                
                index, queue_size = add_sched_waiting_queue(task_request)
                logging.info(f"Service {task_request['task_name']} was added to the queue at position {index}/{queue_size}")
                sched_event.release()
                
                break


def cluster_scheduler_thread(thread, config_data):
    worker_nodes = config_data["nodes"]["worker_nodes"]
    service_limit = config_data["nodes"]["service_limit"]
    worker_node_addresses = config_data["nodes"]["ip_address"]

    while True:

        sched_event.acquire()
        if sched_queue:
            sched_queue_lock.acquire()
            task_request = None
            if sched_queue:
                for index, task in enumerate(sched_queue):
                    work_node, pause_service = check_schedulability(worker_nodes, service_limit, task, worker_node_addresses)
                    if work_node:
                        task_request = sched_queue.pop(index)
                        break
            sched_queue_lock.release()

            if task_request: 
                if pause_service:
                    pause_service_containers(
                        pause_service, worker_node_addresses[work_node], work_node
                    )
                if task_request["service_state"] == "new":
                    sched_service(
                        worker_nodes,
                        service_limit,
                        task_request,
                        worker_node_addresses,
                        work_node
                    )
                else:
                    logging.info(
                        f"Unpause a service from the sched queue: {task_request['task_name']}"
                    )
                    unpause_service_containers(task_request)


def main():
    with open("config.json", encoding="utf-8") as f:
        config_data = json.load(f)

    remove_all_services()
    create_pause_unpause_services(config_data["nodes"]["worker_nodes"])

    remove_services_thread = threading.Thread(target=remove_service_thread, args=(1,))
    request_server_thread = threading.Thread(target=service_request_thread, args=(2,))
    monitor_serices_thread = threading.Thread(target=service_monitor_thread, args=(3,))
    scheduler_thread = threading.Thread(
        target=cluster_scheduler_thread, args=(4, config_data)
    )

    remove_services_thread.start()
    request_server_thread.start()
    monitor_serices_thread.start()
    scheduler_thread.start()

    remove_services_thread.join()
    request_server_thread.join()
    monitor_serices_thread.join()
    scheduler_thread.join()


if __name__ == "__main__":
    logging.info("Real Time Cluster Scheduler started!")
    main()

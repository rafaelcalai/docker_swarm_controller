import docker
import logging
import threading
import time
import socket
import json


client = docker.from_env()
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


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


def load_balance(available_worker_nodes, services_associated):
    least_work_node = ""
    max_services_associated = 255
    for node in available_worker_nodes:
        if node not in services_associated:
            least_work_node = node
            max_services_associated = 0
        else:
            if len(services_associated[node]) < max_services_associated:
                least_work_node = node
                max_services_associated = len(services_associated[node])
    return least_work_node


def create_service(
    service_name, image, constraints=None, secrets=None, command=None, worker_nodes=[]
):
    constraints = constraints or []
    secrets = secrets or []
    command = command or []

    resources = {
        "Limits": {
            "NanoCPUs": int(1 * 1e9),  # Convert CPU to NanoCPUs
            "MemoryBytes": int(256 * 1e6),  # Convert MB to Bytes
        }
    }

    available_worker_nodes = get_available_worker_nodes(worker_nodes)
    services_associated = get_worker_nodes_load()
    work_node = load_balance(available_worker_nodes, services_associated)
    constraints = [f"node.hostname == {work_node}"]

    logging.info(f"Service {service_name} with Constraints: {constraints} created!")
    return client.services.create(
        name=service_name,
        image=image,
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
        logging.info(f"Service {service_id} removed.")
    except docker.errors.NotFound:
        logging.error(f"Service {service_id} not found.")


def remove_service_thread(thread):
    while True:
        for service in client.services.list():
            if service.tasks(filters={"desired-state": ["shutdown"]}):
                logging.info(f"service stoped: {service.name}")
                remove_service(service.id)
        time.sleep(0.1)


def create_service_thread(thread):
    image_name = "rafaelcalai633/wait-for-value:1.1.0"
    constraints = ["node.hostname == rpi5-node01"]

    PORT = 8767
    HOST = "0.0.0.0"
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((HOST, PORT))
    logging.info(f"socket binded to port: {PORT}")

    server.listen()
    logging.info("socket is listening")
    thread = 0

    with open("config.json", encoding="utf-8") as f:
        config_data = json.load(f)

    WORKER_NODES = config_data["nodes"]["worker_nodes"]
    MANAGER_NODES = config_data["nodes"]["manager_nodes"]

    while True:
        connection, addr = server.accept()
        logging.info(f"Connetion from {addr}")

        while True:
            data = connection.recv(1024)
            if data:
                task_request = eval(data)
                service_name = task_request["task_name"]
                command = [
                    "python",
                    "wait_for_value.py",
                    str(task_request["ecxecution_time"]),
                    service_name
                ]

                service = create_service(
                    service_name,
                    image_name,
                    constraints=constraints,
                    command=command,
                    worker_nodes=WORKER_NODES,
                )
                break


def main():
    monitor_thread = threading.Thread(target=remove_service_thread, args=(1,))
    server_thread = threading.Thread(target=create_service_thread, args=(1,))
    monitor_thread.start()
    server_thread.start()


if __name__ == "__main__":
    logging.info("Real Time Cluster Scheduler started!")
    main()

import docker
import logging
import threading
import time
import socket


client = docker.from_env()
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def create_service(service_name, image, constraints=None, secrets=None, command=None):
    constraints = constraints or []
    secrets = secrets or []
    command = command or []
    return client.services.create(
        name=service_name,
        image=image,
        constraints=constraints,
        secrets=secrets,
        command=command,
        restart_policy={"Condition": "none"},
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
    image_name = "rafaelcalai633/wait-for-value"
    command = ["python", "wait_for_value.py", "5"]
    constraints = ["node.hostname == rpi5-node01"]

    PORT = 8767
    HOST = "0.0.0.0"
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((HOST, PORT))
    logging.info(f"socket binded to port: {PORT}")

    server.listen()
    logging.info("socket is listening")
    thread = 0

    while True:
        connection, addr = server.accept()
        logging.info(f"Connetion from {addr}")

        while True:
            data = connection.recv(1024)
            if data:
                task_request = eval(data)
                service_name = task_request["task_name"]

                service = create_service(
                    service_name, image_name, constraints=constraints, command=command
                )
                logging.info(f"Service {service_name} created!")
                break


def main():
    monitor_thread = threading.Thread(target=remove_service_thread, args=(1,))
    server_thread = threading.Thread(target=create_service_thread, args=(1,))
    monitor_thread.start()
    server_thread.start()


if __name__ == "__main__":
    logging.info("Real Time Cluster Scheduler started!")
    main()

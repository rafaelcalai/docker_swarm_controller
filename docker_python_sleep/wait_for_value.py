import sys
import time
import socket

IP_DISTRIBUTED_MANAGER = "192.168.1.111"
PORT_DISTRIBUTED_MANAGER = 8768


def wait_for_value(value):
    print(f"Waiting for: {value}s")
    time.sleep(int(value))


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python wait_for_value.py <value> <task_name>")
        sys.exit(1)

    value = sys.argv[1]
    wait_for_value(value)

    task_response = {"task_name": sys.argv[2], "task_response": 100}
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((IP_DISTRIBUTED_MANAGER, PORT_DISTRIBUTED_MANAGER))
    sock.send(str(task_response).encode())
    sock.close()

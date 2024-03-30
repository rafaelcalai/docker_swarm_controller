import docker
from time import sleep


def get_node_info():
    client = docker.from_env()

    # Get list of nodes in the swarm
    nodes = client.nodes.list()

    node_info = []

    for node in nodes:
        node_data = {
                     'ID': node.id,
                     'Name': node.attrs['Description']['Hostname'],
                     'Status': node.attrs['Status']['State'],
                     'Role': node.attrs['Spec']['Role'],
                     'Availability': node.attrs['Spec']['Availability'],
                     'Platform': node.attrs['Description']['Platform']['Architecture']
         # Add more information as needed
        }

          # Retrieve memory and CPU usage if available
        if 'Description' in node.attrs and 'Resources' in node.attrs['Description']:
            resources = node.attrs['Description']['Resources']
            if 'NanoCPUs' in resources:
                node_data['CPU'] = resources['NanoCPUs'] / 1e9  # Convert from nanocpus to CPUs
            if 'MemoryBytes' in resources:
                node_data['Memory'] = resources['MemoryBytes'] / (1024*1024)  # Convert from bytes to MB

        node_info.append(node_data)

    return node_info




def main():
    print("Docker Scann script")
    client = docker.from_env()

    print("\nCreate a ubuntu container and list all running containers")
    print(client.containers.run("ubuntu:latest", "sleep 25", detach=True))
    print(client.containers.list())


    print("Create wait-for-value container")
    sleep_container = client.containers.run("wait-for-value", "python wait_for_value.py 20", detach=True)
    sleep(2)
    print(client.containers.list())

    if sleep_container in client.containers.list():
        print("Pause wait-for-value container!")
        sleep_container.pause()
        print(client.containers.list())

        sleep(15)
        sleep_container.unpause()
        print(client.containers.list())


    print("Pull NGINX image and print all docker images")
    client.images.pull('nginx')
    print(client.images.list())

    node_info = get_node_info()
    for node in node_info:
        print(node)

if __name__ == "__main__":
    main()


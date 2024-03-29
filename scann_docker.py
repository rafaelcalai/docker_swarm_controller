import docker

def main():
    print("Docker Scann script")
    client = docker.from_env()

    print("\nCreate a ubuntu container and list all running containers")
    print(client.containers.run("ubuntu:latest", "sleep 25", detach=True))
    print(client.containers.list())


    print("Pull NGINX image and print all docker imges")
    client.images.pull('nginx')
    print(client.images.list())


if __name__ == "__main__":
    main()

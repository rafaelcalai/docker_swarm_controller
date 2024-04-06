import docker


client = docker.from_env()


def create_service(service_name, image, constraints=None, secrets=None, command=None):
    constraints = constraints or []
    secrets = secrets or []
    command = command or []
    print(secrets)
    return client.services.create(
        name=service_name,
        image=image,
        constraints=constraints,
        secrets=secrets,
        command=command,
        restart_policy={"Condition": "none"},
    )


service_name = "test_service_python"
image_name = "rafaelcalai633/wait-for-value"
command = ["python", "wait_for_value.py", "5"]

constraints = ["node.hostname == rpi5-node01"]

service = create_service(
    service_name, image_name, constraints=constraints, command=command
)
print(service)

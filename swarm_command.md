
# Create Docker swarm service to run wait-for-value
docker service create --replicas 5 --secret source=dockerhub_username,target=username --secret source=dockerhub_password,target=password --name test_service rafaelcalai633/wait-for-value  python wait_for_value.py 200

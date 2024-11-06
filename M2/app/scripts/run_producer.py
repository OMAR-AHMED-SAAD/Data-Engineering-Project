import docker
import time

def start_producer(id, kafka_url='localhost:9092', topic_name='fintech'):
  docker_client = docker.from_env()
  container = docker_client.containers.run(
    "mmedhat1910/dew24_streaming_producer",
    detach=True,
    name=f"m2_producer_container_{int(time.time())}",
    environment={
      "ID": id,
      "KAFKA_URL":kafka_url,
      "TOPIC":topic_name,
      'debug': 'True'
    },
    network='host'
  )

  print('Container initialized:', container.id)
  return container.id

def stop_container(container_id):
  docker_client = docker.from_env()
  container = docker_client.containers.get(container_id)
  container.stop()
  container.remove()
  print('Container stopped:', container_id)
  return True


if __name__ == "__main__":
    producer_id = "test_producer"
    kafka_url = "localhost:9092"  # Kafka URL
    topic_name = "fintech"  # Kafka topic name

    # Start the producer
    container_id = start_producer(producer_id, kafka_url, topic_name)
    print(f"Started producer with container ID: {container_id}")

    # Wait for a while to observe the producer in action
    time.sleep(10)
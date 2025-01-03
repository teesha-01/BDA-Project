import socket

def check_kafka_connection(host, port):
    try:
        # Try to establish a connection
        sock = socket.create_connection((host, port), timeout=5)  # Set timeout to 5 seconds
        print(f"Connection to {host}:{port} successful!")
        sock.close()
    except (socket.timeout, socket.error) as e:
        print(f"Connection to {host}:{port} failed. Error: {e}")

# Example usage
kafka_host = "kafka"  # Kafka container hostname or IP
kafka_port = 9092     # Kafka port

check_kafka_connection(kafka_host, kafka_port)

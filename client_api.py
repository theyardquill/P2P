import socket
import threading
import json
import signal
import sys

class ClientAPI:
    def __init__(self, client_port=None):
        self.peer_host = None
        self.peer_port = None
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        # Bind the client socket to a random port for receiving messages
        if client_port is None:
            self.client_socket.bind(('localhost', 0))  # Bind to any available port
        else:
            self.client_socket.bind(('localhost', client_port))

        self.received_messages = []
        print(f"Client started, listening on {self.client_socket.getsockname()}")

        # Setup signal handler for graceful exit
        signal.signal(signal.SIGINT, self.shutdown)

    def shutdown(self, signum, frame):
        print("Shutting down client...")
        self.client_socket.close()
        sys.exit(0)

    def select_peer_node(self, indexing_server_host='localhost', indexing_server_port=9000):
        """Query the indexing server to retrieve available peer nodes."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((indexing_server_host, indexing_server_port))
                request = {'action': 'get_peers'}
                s.send(json.dumps(request).encode('utf-8'))
                response = json.loads(s.recv(1024).decode('utf-8'))

                if response['status'] == 'success' and response['peers']:
                    # Display the list of available peers and allow the user to choose
                    print("Available peer nodes:")
                    for idx, (peer_id, (host, port)) in enumerate(response['peers'].items()):
                        print(f"{idx + 1}: {peer_id} at {host}:{port}")

                    while True:
                        choice = input("Select a peer node to connect to (enter the number): ")
                        try:
                            choice = int(choice)
                            if 1 <= choice <= len(response['peers']):
                                break
                            else:
                                print("Please select a valid number.")
                        except ValueError:
                            print("Invalid input. Please enter a number.")

                    peer_id = list(response['peers'].keys())[choice - 1]
                    self.peer_host, self.peer_port = response['peers'][peer_id]
                    print(f"Selected peer node {peer_id} at {self.peer_host}:{self.peer_port}")
                else:
                    print("No available peer nodes.")
        except Exception as e:
            print(f"Error querying indexing server: {e}")

    def create_topic(self, topic, indexing_server_host='localhost', indexing_server_port=9000):
        """Create a topic and register it with the indexing server."""
        if self.peer_host and self.peer_port:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((indexing_server_host, indexing_server_port))
                    create_topic_message = {
                        'action': 'add_topic',  # Use 'add_topic' instead of 'create_topic'
                        'topic': topic,
                        'peer_id': self.client_socket.getsockname()[1]  # Use client's port as peer_id
                    }
                    s.send(json.dumps(create_topic_message).encode('utf-8'))
                    response = json.loads(s.recv(1024).decode('utf-8'))
                    print(f"Create topic response: {response}")

                    # If topic exists, subscribe to it
                    if response['status'] == 'error':
                        print(f"Topic '{topic}' already exists. Subscribing to the existing topic.")
                        self.subscribe(topic)
                    return response['status'] == 'success'
            except Exception as e:
                print(f"Error creating topic: {e}")
        else:
            print("Peer node is not selected.")
        return False

    def delete_topic(self, topic, indexing_server_host='localhost', indexing_server_port=9000):
        """Delete a topic from the indexing server."""
        if self.peer_host and self.peer_port:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((indexing_server_host, indexing_server_port))
                    delete_topic_message = {
                        'action': 'delete_topic',
                        'topic': topic,
                        'peer_id': self.client_socket.getsockname()[1]  # Use client's port as peer_id
                    }
                    s.send(json.dumps(delete_topic_message).encode('utf-8'))
                    response = json.loads(s.recv(1024).decode('utf-8'))
                    print(f"Delete topic response: {response}")
                    return response['status'] == 'success'
            except Exception as e:
                print(f"Error deleting topic: {e}")
        else:
            print("Peer node is not selected.")
        return False

    def publish(self, topic, message):
        """Publish a message to a topic."""
        if self.peer_host and self.peer_port:
            msg = {
                'type': 'publish',
                'topic': topic,
                'message': message
            }
            try:
                self.client_socket.sendto(json.dumps(msg).encode(), (self.peer_host, self.peer_port))
                print(f"Sent publish request to {self.peer_host}:{self.peer_port}: {msg}")  # Log the sent message
            except Exception as e:
                print(f"Error sending publish request: {e}")
        else:
            print("Peer node is not selected.")

    def subscribe(self, topic):
        """Subscribe to a topic."""
        if self.peer_host and self.peer_port:
            msg = {
                'type': 'subscribe',
                'topic': topic
            }
            try:
                self.client_socket.sendto(json.dumps(msg).encode(), (self.peer_host, self.peer_port))
                print(f"Sent subscribe request to {self.peer_host}:{self.peer_port}: {msg}")  # Log the sent message
            except Exception as e:
                print(f"Error sending subscribe request: {e}")
        else:
            print("Peer node is not selected.")

    def start_receiving(self):
        threading.Thread(target=self.receive_messages, daemon=True).start()

    def receive_messages(self):
        while True:
            try:
                data, _ = self.client_socket.recvfrom(1024)
                message = json.loads(data.decode())
                self.received_messages.append(message)
                print(f"Received message: {message}")  # Log the received message
            except Exception as e:
                print(f"Error receiving message: {e}")
                break

if __name__ == "__main__":
    client = ClientAPI(client_port=6000)
    client.select_peer_node()

    # Start receiving messages
    client.start_receiving()

    # Test by creating, subscribing, and publishing
    topic = "Global Warming"
    if client.create_topic(topic):  # Create the topic
        client.subscribe(topic)
        client.publish(topic, "Climate change is a pressing issue affecting the entire planet.")  # Real-world info

    # Test deleting a topic
    if client.delete_topic(topic):  # Delete the topic
        print(f"Topic '{topic}' deleted successfully.")

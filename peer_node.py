import socket
import threading
import json
import random
import logging
import signal
import sys

# Configure logging
logging.basicConfig(filename='peer_node.log', level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')

class PeerNode:
    def __init__(self, host='localhost', port=None, indexing_server_host='localhost', indexing_server_port=9000):
        self.host = host
        self.port = port if port is not None else random.randint(5000, 6000)
        self.indexing_server = (indexing_server_host, indexing_server_port)  # Indexing server address
        self.topics = []  # Tracks topics hosted by this peer
        self.subscribers = {}  # Dictionary to store subscribers by topic

        # Set up a UDP socket and bind to the provided host and port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind((self.host, self.port))
        print(f"Node started at {self.host}:{self.port}")

        # Setup signal handler for graceful exit
        signal.signal(signal.SIGINT, self.shutdown)

        # Register the peer node with the indexing server
        self.register_with_indexing_server()

        # Start a thread to listen for incoming messages from peers
        threading.Thread(target=self.listen, daemon=True).start()

    def shutdown(self, signum, frame):
        print("Shutting down peer node...")
        self.socket.close()
        sys.exit(0)

    # ... (rest of the code remains the same)


    def register_with_indexing_server(self):
        """Register this peer node with the indexing server."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect(self.indexing_server)
                register_message = {
                    'action': 'register',
                    'peer_id': self.port,  # Use port as the peer ID
                    'peer_port': self.port
                }
                s.send(json.dumps(register_message).encode('utf-8'))
                response = s.recv(1024).decode('utf-8')
                print(f"Registration response: {response}")
        except Exception as e:
            print(f"Error registering with indexing server: {e}")

    def listen(self):
        """Listening thread that waits for incoming UDP messages."""
        while True:
            try:
                data, addr = self.socket.recvfrom(1024)  # Receive up to 1024 bytes of data
                message = json.loads(data.decode())  # Decode and parse the incoming JSON message
                print(f"Received message from {addr}: {message}")
                self.handle_message(message, addr)   # Handle the message based on its type
            except Exception as e:
                print(f"Error receiving message: {e}")

    def handle_message(self, message, addr):
        """Handle incoming messages from peers."""
        print(f"Handling message: {message} from {addr}")  # Log incoming messages
        if message['type'] == 'publish':
            print(f"Handling publish message: {message}")
            self.distribute_message(message)  # Distribute the published message to subscribers
        elif message['type'] == 'subscribe':
            print(f"Handling subscribe request: {message}")
            if self.subscribe(message['topic'], addr):  # Subscribe the peer to a specific topic
                # After subscribing, acknowledge the subscription
                ack_message = {'type': 'ack', 'topic': message['topic']}
                self.socket.sendto(json.dumps(ack_message).encode(), addr)
        else:
            print(f"Unknown message type: {message['type']}")

    def subscribe(self, topic, addr):
        """Subscribe a peer (identified by addr) to a specific topic."""
        if topic not in self.subscribers:
            self.subscribers[topic] = []  # Initialize the list of subscribers for this topic
        if addr not in self.subscribers[topic]:
            self.subscribers[topic].append(addr)  # Add the peer's address to the list of subscribers
            print(f"Subscribed {addr} to topic '{topic}'")
            return True
        else:
            print(f"{addr} is already subscribed to '{topic}'")
            return False

    def distribute_message(self, message):
        """Distribute a published message to all subscribers of the topic."""
        topic = message['topic']
        if topic in self.subscribers:
            for subscriber in self.subscribers[topic]:
                try:
                    self.socket.sendto(json.dumps(message).encode(), subscriber)
                    print(f"Sent message to {subscriber}: {message}")  # Log the sent message
                except Exception as e:
                    print(f"Error sending message to {subscriber}: {e}")

    def publish(self, topic, content):
        """Publish a message to a topic, distributing it to all subscribers."""
        msg = {
            'type': 'publish',
            'topic': topic,
            'message': content
        }
        # Send the message to all subscribers
        print(f"Publishing message: {msg}")  # Log the published message
        self.distribute_message(msg)

if __name__ == "__main__":
    # If running this script, set a specific port, or choose a random one for additional nodes
    port = input("Enter a port number (or leave blank to use a random port): ")
    if port:
        node = PeerNode(port=int(port))
    else:
        node = PeerNode()

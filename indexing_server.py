import socket
import threading
import json
import logging

# Configure logging
logging.basicConfig(filename='indexing_server.log', level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')

class IndexingServer:
    def __init__(self, host='localhost', port=9000):
        self.host = host
        self.port = port
        self.peers = {}  # Tracks peer nodes by peer_id as {peer_id: (host, port)}
        self.topics = {}  # Tracks topics by topic name as {topic: peer_id}

    def handle_peer(self, peer_socket, addr):
        """Handle incoming peer connections and process their requests."""
        try:
            logging.info(f"Peer connected from {addr}")
            while True:
                request = peer_socket.recv(1024).decode('utf-8')
                if not request:
                    break
                request = json.loads(request)
                logging.info(f"Received request: {request} from {addr}")
                response = self.process_request(request, addr)
                peer_socket.send(json.dumps(response).encode('utf-8'))
                logging.info(f"Sent response: {response} to {addr}")
        except Exception as e:
            logging.error(f"Error handling peer {addr}: {e}")
        finally:
            peer_socket.close()
            logging.info(f"Peer {addr} disconnected")

    def process_request(self, request, addr):
        action = request.get('action')

        if action == 'register':
            return self.register_peer(request['peer_id'], addr[0], request['peer_port'])
        elif action == 'get_peers':
            return self.get_peers()
        elif action == 'create_topic':
            return self.create_topic(request['topic'], request['peer_id'])
        elif action == 'get_topics':
            return self.get_topics()
        else:
            logging.warning(f"Invalid action received: {action}")
            return {'status': 'error', 'message': 'Invalid action'}

    def register_peer(self, peer_id, host, port):
        self.peers[peer_id] = (host, port)
        logging.info(f"Registered peer {peer_id} at {host}:{port}")
        return {'status': 'success', 'message': f"Peer {peer_id} registered"}

    def get_peers(self):
        if self.peers:
            return {'status': 'success', 'peers': self.peers}
        else:
            logging.warning(f"No peers registered")
            return {'status': 'error', 'message': 'No peers registered'}

    def create_topic(self, topic, peer_id):
        if topic not in self.topics:  # Avoid duplicate topic creation
            self.topics[topic] = peer_id
            logging.info(f"Topic '{topic}' created by peer {peer_id}")
            return {'status': 'success', 'message': f"Topic '{topic}' created"}
        else:
            logging.warning(f"Topic '{topic}' already exists.")
            return {'status': 'error', 'message': f"Topic '{topic}' already exists."}

    def get_topics(self):
        """Return the list of existing topics."""
        return {'status': 'success', 'topics': list(self.topics.keys())}

    def start(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((self.host, self.port))
        server_socket.listen(5)
        logging.info(f"Indexing server listening on {self.host}:{self.port}")

        while True:
            peer_socket, addr = server_socket.accept()
            peer_handler = threading.Thread(target=self.handle_peer, args=(peer_socket, addr))
            peer_handler.start()

if __name__ == '__main__':
    indexing_server = IndexingServer('localhost', 9000)
    indexing_server.start()

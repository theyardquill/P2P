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
        self.topics = {}  # Tracks topics by topic name as {topic: [peer_id1, peer_id2]}

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
        elif action == 'unregister':
            return self.unregister_peer(request['peer_id'])
        elif action == 'add_topic':
            return self.add_topic(request['peer_id'], request['topic'])
        elif action == 'delete_topic':
            return self.delete_topic(request['peer_id'], request['topic'])
        elif action == 'query_topic':
            return self.query_topic(request['topic'])
        elif action == 'get_peers':
            return self.get_peers()
        else:
            logging.warning(f"Invalid action received: {action}")
            return {'status': 'error', 'message': 'Invalid action'}

    def register_peer(self, peer_id, host, port):
        self.peers[peer_id] = (host, port)
        logging.info(f"Registered peer {peer_id} at {host}:{port}")
        return {'status': 'success', 'message': f"Peer {peer_id} registered"}

    def unregister_peer(self, peer_id):
        if peer_id in self.peers:
            del self.peers[peer_id]
            # Remove peer from any topics it was associated with
            for topic in list(self.topics.keys()):
                if peer_id in self.topics[topic]:
                    self.topics[topic].remove(peer_id)
                    if not self.topics[topic]:
                        del self.topics[topic]  # Remove topic if no peers hold it
            logging.info(f"Unregistered peer {peer_id}")
            return {'status': 'success', 'message': f"Peer {peer_id} unregistered"}
        else:
            return {'status': 'error', 'message': f"Peer {peer_id} not found"}

    def add_topic(self, peer_id, topic):
        if peer_id in self.peers:
            if topic not in self.topics:
                self.topics[topic] = []
            if peer_id not in self.topics[topic]:
                self.topics[topic].append(peer_id)
            logging.info(f"Peer {peer_id} added topic '{topic}'")
            return {'status': 'success', 'message': f"Topic '{topic}' added for peer {peer_id}"}
        else:
            return {'status': 'error', 'message': f"Peer {peer_id} not registered"}

    def delete_topic(self, peer_id, topic):
        if topic in self.topics and peer_id in self.topics[topic]:
            self.topics[topic].remove(peer_id)
            if not self.topics[topic]:
                del self.topics[topic]
            logging.info(f"Peer {peer_id} deleted topic '{topic}'")
            return {'status': 'success', 'message': f"Topic '{topic}' deleted for peer {peer_id}"}
        else:
            return {'status': 'error', 'message': f"Topic '{topic}' not found for peer {peer_id}"}

    def query_topic(self, topic):
        if topic in self.topics and self.topics[topic]:
            peer_id = self.topics[topic][0]  # Return the first peer holding the topic
            logging.info(f"Query for topic '{topic}' found peer {peer_id}")
            return {'status': 'success', 'peer_id': peer_id, 'peer_info': self.peers[peer_id]}
        else:
            logging.warning(f"Query for topic '{topic}' failed")
            return {'status': 'error', 'message': f"Topic '{topic}' not found"}

    def get_peers(self):
        if self.peers:
            return {'status': 'success', 'peers': self.peers}
        else:
            logging.warning(f"No peers registered")
            return {'status': 'error', 'message': 'No peers registered'}

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

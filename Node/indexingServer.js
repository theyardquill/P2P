const net = require('net');
const fs = require('fs');

// Log settings
const logFile = 'indexing_server.log';

// Write logs
const log = (message) => {
  const timestamp = new Date().toISOString();
  const logMessage = `${timestamp} ${message}\n`;
  fs.appendFileSync(logFile, logMessage);
};

// Indexing Server class
class IndexingServer {
  constructor(host = 'localhost', port = 9000) {
    this.host = host;
    this.port = port;
    this.peers = {}; // {peer_id: {host, port}}
    this.topics = {}; // {topic: peer_id}
  }

  // Handle incoming peer connections
  handlePeer(socket) {
    socket.on('data', (data) => {
      const request = JSON.parse(data.toString());
      log(`Received request: ${JSON.stringify(request)}`);
      const response = this.processRequest(request, socket.remoteAddress); // Use socket.remoteAddress as host
      socket.write(JSON.stringify(response));
      log(`Sent response: ${JSON.stringify(response)}`);
    });

    socket.on('error', (err) => log(`Error handling peer: ${err.message}`));
  }

  // Process requests from peers
  processRequest(request, remoteAddress) {
    switch (request.action) {
      case 'register':
        return this.registerPeer(request.peer_id, remoteAddress, request.peer_port); // Use remoteAddress as the peer host
      case 'get_peers':
        return this.getPeers();
      case 'create_topic':
        return this.createTopic(request.topic, request.peer_id);
      default:
        return { status: 'error', message: 'Invalid action' };
    }
  }

  // Register peer node
  registerPeer(peer_id, host, port) {
    this.peers[peer_id] = { host, port };
    log(`Registered peer ${peer_id} at ${host}:${port}`);
    return { status: 'success', message: `Peer ${peer_id} registered at ${host}:${port}` };
  }

  // Return list of peers
  getPeers() {
    if (Object.keys(this.peers).length === 0) {
      return { status: 'error', message: 'No peers registered' };
    }
    return { status: 'success', peers: this.peers };
  }

  // Create a new topic
  createTopic(topic, peer_id) {
    if (this.topics[topic]) {
      return { status: 'error', message: `Topic '${topic}' already exists.` };
    }
    this.topics[topic] = peer_id;
    log(`Created topic '${topic}' by peer ${peer_id}`);
    return { status: 'success', message: `Topic '${topic}' created` };
  }

  // Start the indexing server
  start() {
    const server = net.createServer((socket) => this.handlePeer(socket));
    server.listen(this.port, this.host, () => {
      log(`Indexing server started on ${this.host}:${this.port}`);
      console.log(`Indexing server listening on ${this.host}:${this.port}`);
    });
  }
}

// Start the indexing server
const indexingServer = new IndexingServer();
indexingServer.start();

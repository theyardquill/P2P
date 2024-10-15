const dgram = require('dgram');
const net = require('net');
const fs = require('fs');

// Log settings
const logFile = 'peer_node.log';
const log = (message) => {
  const timestamp = new Date().toISOString();
  const logMessage = `${timestamp} ${message}\n`;
  fs.appendFileSync(logFile, logMessage);
};

// Peer Node class
class PeerNode {
  constructor(host = 'localhost', port = null, indexingServerHost = 'localhost', indexingServerPort = 9000) {
    this.host = host;
    this.port = port || Math.floor(Math.random() * 10000 + 5000);
    this.indexingServer = { host: indexingServerHost, port: indexingServerPort };
    this.topics = [];
    this.subscribers = {}; // {topic: [subscribers]}

    this.socket = dgram.createSocket('udp4');
    this.socket.bind(this.port, () => {
      console.log(`Peer node started at ${this.host}:${this.port}`);
    });

    // Graceful shutdown
    process.on('SIGINT', () => this.shutdown());

    // Register with indexing server
    this.registerWithIndexingServer();

    // Listen for incoming messages
    this.listen();
  }

  // Graceful shutdown of the peer node
  shutdown() {
    console.log('Shutting down peer node...');
    this.socket.close();
    process.exit(0);
  }

  // Register this peer node with the indexing server
  registerWithIndexingServer() {
    const client = net.createConnection({ host: this.indexingServer.host, port: this.indexingServer.port }, () => {
      const message = {
        action: 'register',
        peer_id: this.port,
        peer_port: this.port,
      };
      client.write(JSON.stringify(message));
      log(`Registered with indexing server at ${this.indexingServer.host}:${this.indexingServer.port}`);
    });
  }

  // Listen for incoming UDP messages
  listen() {
    this.socket.on('message', (msg, rinfo) => {
      const message = JSON.parse(msg.toString());
      log(`Received message from ${rinfo.address}:${rinfo.port} - ${JSON.stringify(message)}`);
      this.handleMessage(message, rinfo);
    });

    this.socket.on('error', (err) => log(`Error receiving message: ${err.message}`));
  }

  // Handle incoming messages from peers
  handleMessage(message, rinfo) {
    if (message.type === 'publish') {
      this.distributeMessage(message);
    } else if (message.type === 'subscribe') {
      this.subscribe(message.topic, rinfo);
    } else {
      log(`Unknown message type: ${message.type}`);
    }
  }

  // Subscribe a peer to a topic
  subscribe(topic, rinfo) {
    if (!this.subscribers[topic]) {
      this.subscribers[topic] = [];
    }
    if (!this.subscribers[topic].includes(rinfo)) {
      this.subscribers[topic].push(rinfo);
      log(`Subscribed ${rinfo.address}:${rinfo.port} to topic '${topic}'`);
    }
  }

  // Distribute published messages to subscribers
  distributeMessage(message) {
    const subscribers = this.subscribers[message.topic];
    if (subscribers) {
      subscribers.forEach((subscriber) => {
        this.socket.send(Buffer.from(JSON.stringify(message)), subscriber.port, subscriber.address, (err) => {
          if (err) log(`Error sending message to ${subscriber.address}:${subscriber.port}`);
        });
        log(`Sent message to ${subscriber.address}:${subscriber.port}`);
      });
    }
  }

  // Publish a message to a topic
  publish(topic, content) {
    const message = {
      type: 'publish',
      topic,
      message: content,
    };
    this.distributeMessage(message);
  }
}

// Start the peer node
const port = process.argv[2] || null;
const peerNode = new PeerNode('localhost', port);

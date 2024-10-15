const dgram = require('dgram');
const net = require('net');
const readline = require('readline');

// Client class
class ClientAPI {
  constructor(clientPort = null) {
    this.peerHost = null;
    this.peerPort = null;
    this.clientSocket = dgram.createSocket('udp4');
    
    // Bind to an available port for receiving messages
    this.clientSocket.bind(clientPort || 0, () => {
      console.log(`Client started, listening on ${this.clientSocket.address().address}:${this.clientSocket.address().port}`);
    });

    // Graceful shutdown handling
    process.on('SIGINT', () => this.shutdown());
  }

  // Shutdown the client
  shutdown() {
    console.log('Shutting down client...');
    this.clientSocket.close();
    process.exit(0);
  }

  // Query the indexing server to get available peers
  async selectPeerNode(indexingServerHost = 'localhost', indexingServerPort = 9000) {
    return new Promise((resolve, reject) => {
      const client = net.createConnection({ host: indexingServerHost, port: indexingServerPort }, () => {
        const request = { action: 'get_peers' };
        client.write(JSON.stringify(request));
      });

      client.on('data', (data) => {
        const response = JSON.parse(data.toString());
        if (response.status === 'success' && response.peers) {
          console.log('Available peer nodes:');
          Object.keys(response.peers).forEach((peerId, idx) => {
            console.log(`${idx + 1}: ${peerId} at ${response.peers[peerId].host}:${response.peers[peerId].port}`);
          });

          readline.createInterface({
            input: process.stdin,
            output: process.stdout,
          }).question('Select a peer node to connect to (enter the number): ', (answer) => {
            const peerId = Object.keys(response.peers)[parseInt(answer) - 1];
            this.peerHost = response.peers[peerId].host;
            this.peerPort = response.peers[peerId].port;
            console.log(`Selected peer node ${peerId} at ${this.peerHost}:${this.peerPort}`);
            client.end();
            resolve();
          });
        } else {
          console.log('No available peer nodes.');
          reject();
        }
      });

      client.on('error', (err) => reject(err));
    });
  }

  // Create a topic
  async createTopic(topic, indexingServerHost = 'localhost', indexingServerPort = 9000) {
    if (!this.peerHost || !this.peerPort) {
      console.log('Peer node is not selected.');
      return false;
    }

    return new Promise((resolve, reject) => {
      const client = net.createConnection({ host: indexingServerHost, port: indexingServerPort }, () => {
        const createTopicMessage = {
          action: 'create_topic',
          topic,
          peer_id: this.clientSocket.address().port,
        };
        client.write(JSON.stringify(createTopicMessage));
      });

      client.on('data', (data) => {
        const response = JSON.parse(data.toString());
        console.log(`Create topic response: ${response.message}`);
        if (response.status === 'error') {
          console.log(`Topic '${topic}' already exists. Subscribing to the existing topic.`);
          this.subscribe(topic);
        }
        client.end();
        resolve(response.status === 'success');
      });

      client.on('error', (err) => reject(err));
    });
  }

  // Publish a message
  publish(topic, message) {
    if (!this.peerHost || !this.peerPort) {
      console.log('Peer node is not selected.');
      return;
    }

    const msg = { type: 'publish', topic, message };
    this.clientSocket.send(Buffer.from(JSON.stringify(msg)), this.peerPort, this.peerHost, (err) => {
      if (err) console.log('Error sending publish request:', err);
      else console.log(`Sent publish request to ${this.peerHost}:${this.peerPort}:`, msg);
    });
  }

  // Subscribe to a topic
  subscribe(topic) {
    if (!this.peerHost || !this.peerPort) {
      console.log('Peer node is not selected.');
      return;
    }

    const msg = { type: 'subscribe', topic };
    this.clientSocket.send(Buffer.from(JSON.stringify(msg)), this.peerPort, this.peerHost, (err) => {
      if (err) console.log('Error sending subscribe request:', err);
      else console.log(`Sent subscribe request to ${this.peerHost}:${this.peerPort}:`, msg);
    });
  }

  // Start receiving messages
  startReceiving() {
    this.clientSocket.on('message', (msg) => {
      const message = JSON.parse(msg.toString());
      console.log(`Received message: ${JSON.stringify(message)}`);
    });
  }
}

// Client script execution
(async () => {
  const client = new ClientAPI(6000);
  await client.selectPeerNode();

  // Start receiving messages
  client.startReceiving();

  // Test by creating, subscribing, and publishing
  const topic = 'Global Warming';
  const success = await client.createTopic(topic);
  if (success) {
    client.subscribe(topic);
    client.publish(topic, 'Climate change is a pressing issue affecting the entire planet.');
  }
})();

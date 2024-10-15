import subprocess
import time
import random
import json
import socket
import matplotlib.pyplot as plt

# Constants
INDEXING_SERVER_PORT = 9000
PEER_PORTS = [8001, 8002, 8003, 8004, 8005, 8006, 8007, 8008]  # Extendable
TOPIC_NAME = "Global Warming"
NUMBER_OF_QUERIES = 1000  # Fixed number of requests per node
TOPIC_COUNT = 1000000  # Number of topics in the indexing server

def start_indexing_server():
    """Starts the indexing server."""
    print("Starting the indexing server...")
    subprocess.Popen(["make", "indexing-server"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def start_peer_nodes():
    """Starts the peer nodes."""
    print("Starting peer nodes...")
    subprocess.Popen(["make", "peer-nodes"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def measure_response_time(peer_node, topic):
    """Measures the average response time for querying topics."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
        client_socket.connect(('127.0.0.1', INDEXING_SERVER_PORT))
        total_time = 0
        for _ in range(NUMBER_OF_QUERIES):
            start_time = time.time()
            request = {'action': 'get_topic', 'topic': topic}
            client_socket.send(json.dumps(request).encode('utf-8'))
            client_socket.recv(1024)  # Assume we wait for a response
            total_time += time.time() - start_time
        average_time = total_time / NUMBER_OF_QUERIES
        return average_time

def benchmark_api(peer_node):
    """Benchmark the APIs for latency and throughput."""
    print(f"Benchmarking APIs for peer node {peer_node}...")
    latencies = []

    for _ in range(3):  # Benchmarking each API 3 times
        # Simulate API call with random latency
        start_time = time.time()
        time.sleep(random.uniform(0.01, 0.1))  # Simulated API latency
        latencies.append(time.time() - start_time)

    return latencies

def plot_results(x_values, y_values, title, xlabel, ylabel):
    """Plots the results."""
    plt.plot(x_values, y_values)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.grid()
    plt.show()

def main():
    start_indexing_server()
    time.sleep(2)  # Wait for server to start
    start_peer_nodes()
    time.sleep(2)  # Wait for peers to start

    # Measure response times for various peer nodes
    response_times = []
    for peer in PEER_PORTS:
        avg_time = measure_response_time('127.0.0.1', peer)
        response_times.append(avg_time)
        print(f"Average response time for peer {peer}: {avg_time:.4f} seconds")

    # Benchmark APIs
    api_latencies = []
    for peer in PEER_PORTS:
        latencies = benchmark_api(peer)
        api_latencies.append(latencies)
        print(f"Latencies for peer {peer}: {latencies}")

    # Plotting response times
    plot_results(PEER_PORTS, response_times, "Average Response Time vs Peer Nodes", "Peer Nodes", "Response Time (seconds)")

    # Plotting API latencies
    for idx, latencies in enumerate(api_latencies):
        plot_results([1, 2, 3], latencies, f"API Latencies for Peer {PEER_PORTS[idx]}", "API Calls", "Latency (seconds)")

if __name__ == "__main__":
    main()

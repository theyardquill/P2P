# Makefile for running Peer Nodes, Indexing Server, and Client

# Variables
PYTHON := python  # You can also use python3 depending on your environment
PEER_PORTS := 8001 8002 8003  # List of ports for peer nodes
CLIENT_RUNS := 3  # Number of times to run the client for each peer node

# Default target
all: help

# Help command to show instructions
help:
	@echo "Available commands:"
	@echo "  make indexing-server   - Start the indexing server"
	@echo "  make peer-nodes        - Start all peer nodes"
	@echo "  make client            - Run client to interact with a peer node"
	@echo "  make run-clients       - Run clients for each peer node"
	@echo "  make clean             - Clean log files"

# Run the indexing server
indexing-server:
	@echo "Starting the indexing server on port 9000..."
	$(PYTHON) indexing_server.py

# Run all peer nodes
peer-nodes:
	@for port in $(PEER_PORTS); do \
		echo "Starting peer node on port $$port..."; \
		$(PYTHON) peer_node.py $$port & \
	done

# Run the client (connects to each peer node multiple times)
run-clients: peer-nodes
	@for port in $(PEER_PORTS); do \
		@for run in $(shell seq 1 $(CLIENT_RUNS)); do \
			echo "Running client connected to peer node on port $$port..."; \
			$(PYTHON) client_api.py; \
		done \
	done

# Clean the log files
clean:
	@echo "Cleaning log files..."
	rm -f *.log

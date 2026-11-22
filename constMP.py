"""Default network settings. All values can be overridden via command-line arguments."""
GROUPMNGR_ADDR = "127.0.0.1"
GROUPMNGR_TCP_PORT = 5680  # XML-RPC over HTTP/TCP, NOT raw TCP requests
SERVER_ADDR = "127.0.0.1"
SERVER_PORT = 5678           # XML-RPC coordinator
PEER_UDP_PORT = 6789        # socket-based peer-to-peer data plane
PEER_TCP_PORT = 5679        # XML-RPC peer control plane
N = 3                       # default expected peer count (configurable)

"""Membership registry provided through XML-RPC, replacing raw TCP sockets."""
import argparse
import threading
from constMP import GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT
from rpc_utils import ThreadedRPCServer


class GroupManager:
    def __init__(self, server):
        self.server = server
        self.members = {}
        self.lock = threading.RLock()

    def register_peer(self, peer_id, host, udp_port, rpc_port):
        if not peer_id or not host or not (0 < int(udp_port) < 65536) or not (0 < int(rpc_port) < 65536):
            raise ValueError("Invalid peer registration")
        with self.lock:
            if peer_id not in self.members and any(
                p['host'] == host and p['udp_port'] == int(udp_port)
                for p in self.members.values()
            ):
                raise ValueError("UDP endpoint already registered")
            self.members[peer_id] = dict(id=peer_id, host=host,
                                         udp_port=int(udp_port), rpc_port=int(rpc_port))
            print(f"[group] registered {peer_id}: {host}:{udp_port}", flush=True)
        return True

    def list_peers(self):
        with self.lock:
            return list(self.members.values())

    def unregister_peer(self, peer_id):
        with self.lock:
            return self.members.pop(peer_id, None) is not None

    def ping(self):
        return True

    def stop(self):
        threading.Thread(target=self.server.shutdown, daemon=True).start()
        return True


def main():
    parser = argparse.ArgumentParser(description="XML-RPC group membership server")
    parser.add_argument('--bind', default='0.0.0.0')
    parser.add_argument('--port', type=int, default=GROUPMNGR_TCP_PORT)
    args = parser.parse_args()
    with ThreadedRPCServer((args.bind, args.port), allow_none=True, logRequests=False) as server:
        server.register_instance(GroupManager(server))
        print(f"[group] XML-RPC listening on {args.bind}:{args.port}", flush=True)
        server.serve_forever()


if __name__ == '__main__':
    main()

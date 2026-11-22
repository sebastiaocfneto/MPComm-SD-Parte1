"""Tiny shared XML-RPC helpers (Python standard library only)."""
from socketserver import ThreadingMixIn
from xmlrpc.client import ServerProxy, Transport
from xmlrpc.server import SimpleXMLRPCServer
import http.client


class ThreadedRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    daemon_threads = True
    allow_reuse_address = True


class TimeoutTransport(Transport):
    def __init__(self, timeout=3):
        super().__init__()
        self.timeout = timeout

    def make_connection(self, host):
        return http.client.HTTPConnection(host, timeout=self.timeout)


def rpc_client(host, port, timeout=3):
    # One proxy per operation/thread: xmlrpc.client.ServerProxy is not thread-safe.
    return ServerProxy(f"http://{host}:{int(port)}/", allow_none=True,
                       transport=TimeoutTransport(timeout=timeout))

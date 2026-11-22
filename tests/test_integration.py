"""End-to-end test with real OS processes and real UDP / XML-RPC localhost sockets."""
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

from rpc_utils import rpc_client

ROOT = Path(__file__).resolve().parents[1]


def unused_tcp_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def unused_udp_port():
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def wait_for_ready(host, port, proc, timeout=10):
    until = time.monotonic() + timeout
    while time.monotonic() < until:
        if proc.poll() is not None:
            raise RuntimeError(f'Server died: {proc.args}')
        try:
            with rpc_client(host, port, timeout=0.35) as proxy:
                if proxy.ping():
                    return
        except Exception:
            time.sleep(0.05)
    raise TimeoutError(f'RPC server did not start on {port}')


class LiveSystemTests(unittest.TestCase):
    def test_three_peers_fifo_under_data_and_ack_loss(self):
        procs = []
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp)
            group_port, server_port = unused_tcp_port(), unused_tcp_port()
            def launch(file, *argv):
                log_file = (output / f'p{len(procs)}.log').open('w')
                proc = subprocess.Popen([sys.executable, '-u', str(ROOT / file), *map(str, argv)],
                     stdout=log_file,
                     stderr=subprocess.STDOUT, cwd=ROOT)
                log_file.close()
                procs.append(proc)
                return proc
            try:
                manager = launch('GroupMngr.py', '--bind', '127.0.0.1', '--port', group_port)
                wait_for_ready('127.0.0.1', group_port, manager)
                configs = []
                for index in range(3):
                    udp_port, rpc_port = unused_udp_port(), unused_tcp_port()
                    configs.append((udp_port, rpc_port))
                    p = launch('peerCommunicatorUDP.py', '--peer-id', f'peer{index}',
                        '--udp-port', udp_port, '--rpc-port', rpc_port,
                        '--group-port', group_port, '--server-port', server_port,
                        '--retry-ms', 65, '--interval-ms', 5,
                        '--drop-first-data-every', 2,
                        '--drop-first-ack-every', 3)
                    wait_for_ready('127.0.0.1', rpc_port, p)
                with rpc_client('127.0.0.1', group_port) as proxy:
                    deadline = time.monotonic() + 10
                    while time.monotonic() < deadline:
                        if len(proxy.list_peers()) == 3:
                            break
                        time.sleep(0.1)
                    self.assertEqual(3, len(proxy.list_peers()))
                run = launch('comparisonServer.py', '--bind', '127.0.0.1',
                             '--port', server_port, '--group-port', group_port,
                             '--peers', 3, '--messages', 8, '--timeout', 20,
                             '--log-dir', output / 'results')
                try:
                    self.assertEqual(0, run.wait(timeout=25))
                except (AssertionError, subprocess.TimeoutExpired):
                    for i, proc in enumerate(procs):
                        fp = output / f'p{i}.log'
                        print(f'--- PROCESS {i} ---\n{fp.read_text() if fp.exists() else ""}')
                    raise
                results = list((output / 'results').glob('round_*.json'))
                self.assertEqual(1, len(results))
                contents = json.loads(results[0].read_text(encoding='utf-8'))
                summary = contents['summary']
                self.assertTrue(summary['fifo_valid'])
                self.assertTrue(summary['complete'])
                self.assertGreater(summary['retransmissions'], 0)
                self.assertEqual(3, len(contents['logs']))
                for pid, report in contents['logs'].items():
                    self.assertEqual(24, len(report['log']))
            finally:
                for proc in reversed(procs):
                    if proc.poll() is None:
                        proc.terminate()
                for proc in reversed(procs):
                    try:
                        proc.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait(timeout=3)


if __name__ == '__main__':
    unittest.main()

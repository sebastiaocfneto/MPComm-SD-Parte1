"""Single-command live demonstration (Windows/macOS/Linux, no dependencies).

Starts group manager, peers and coordinator as independent OS processes.
Exercises real local UDP sockets and XML-RPC calls, then shuts them down.
"""
import argparse
from datetime import datetime
import json
import socket
import subprocess
import sys
import time
from pathlib import Path

from rpc_utils import rpc_client

ROOT = Path(__file__).resolve().parent


def free_port(udp=False):
    kind = socket.SOCK_DGRAM if udp else socket.SOCK_STREAM
    with socket.socket(socket.AF_INET, kind) as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def ready(port, process, seconds=10):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f'Process exited unexpectedly ({process.returncode})')
        try:
            with rpc_client('127.0.0.1', port, timeout=0.35) as rpc:
                if rpc.ping():
                    return
        except Exception:
            time.sleep(0.06)
    raise TimeoutError(f'RPC server on port {port} failed to start')


def main():
    ap = argparse.ArgumentParser(description='MPComm Parte 1 live local demonstration')
    ap.add_argument('--peers', type=int, default=3)
    ap.add_argument('--messages', type=int, default=8)
    ap.add_argument('--drop-first-data-every', type=int, default=2)
    ap.add_argument('--drop-first-ack-every', type=int, default=3)
    args = ap.parse_args()
    if args.peers < 2 or args.messages < 1:
        ap.error('Use at least 2 peers and 1 message')
    logfile_dir = ROOT / 'logs' / ('demo_' + datetime.now().strftime('%Y%m%d_%H%M%S'))
    logfile_dir.mkdir(parents=True, exist_ok=True)
    processes = []
    log_handles = []

    def launch(name, *params):
        file = logfile_dir / f'{name}.log'
        handle = file.open('w', encoding='utf-8')
        log_handles.append(handle)
        proc = subprocess.Popen([sys.executable, '-u', str(ROOT / params[0]),
                                 *map(str, params[1:])], cwd=ROOT, stdout=handle,
                                 stderr=subprocess.STDOUT)
        processes.append((name, proc, file))
        return proc

    try:
        group_port = free_port()
        coordinator_port = free_port()
        print('Starting XML-RPC group manager...', flush=True)
        group_proc = launch('group', 'GroupMngr.py', '--bind', '127.0.0.1',
                            '--port', group_port)
        ready(group_port, group_proc)

        print(f'Starting {args.peers} peers (UDP data plane + XML-RPC control)...', flush=True)
        for i in range(args.peers):
            udp_port, rpc_port = free_port(udp=True), free_port()
            proc = launch(f'peer{i}', 'peerCommunicatorUDP.py',
                '--peer-id', f'P{i}', '--udp-port', udp_port, '--rpc-port', rpc_port,
                '--group-port', group_port, '--server-port', coordinator_port,
                '--drop-first-data-every', args.drop_first_data_every,
                '--drop-first-ack-every', args.drop_first_ack_every)
            ready(rpc_port, proc)

        print(f'Running {args.peers} peers x {args.messages} messages, '
              f'DATA drop-every={args.drop_first_data_every}, '
              f'ACK drop-every={args.drop_first_ack_every}...', flush=True)
        command = [sys.executable, '-u', str(ROOT / 'comparisonServer.py'),
                   '--bind', '127.0.0.1', '--port', str(coordinator_port),
                   '--group-port', str(group_port), '--peers', str(args.peers),
                   '--messages', str(args.messages), '--timeout', '45',
                   '--log-dir', str(logfile_dir)]
        subprocess.run(command, check=True, cwd=ROOT)
        reports = sorted(logfile_dir.glob('round_*.json'))
        if len(reports) != 1:
            raise RuntimeError('Missing or ambiguous JSON result')
        result = json.loads(reports[0].read_text(encoding='utf-8'))['summary']
        if not result['fifo_valid'] or not result['complete']:
            raise RuntimeError('Experiment finished but validation failed')
        print('\nPASS: FIFO and complete delivery confirmed.', flush=True)
        print(f'UDP retransmissions: {result["retransmissions"]}', flush=True)
        print(f'JSON report: {reports[0]}', flush=True)
        print(f'Process logs: {logfile_dir}', flush=True)
    except Exception:
        print('\nError details in process logs:', file=sys.stderr)
        for name, proc, file in processes:
            print(f'  {name}: {file} (exit={proc.poll()})', file=sys.stderr)
        raise
    finally:
        for _, proc, _ in reversed(processes):
            if proc.poll() is None:
                proc.terminate()
        for _, proc, _ in reversed(processes):
            try:
                proc.wait(timeout=4)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=4)
        for handle in log_handles:
            handle.close()


if __name__ == '__main__':
    main()

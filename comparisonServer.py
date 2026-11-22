"""XML-RPC coordinator of multi-peer UDP reliability/FIFO experiments."""
import argparse
import json
import threading
import time
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from constMP import (GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT,
                     SERVER_PORT, N)
from rpc_utils import ThreadedRPCServer, rpc_client


class Coordinator:
    def __init__(self):
        self.round_id = None
        self.received = {}
        self.members = []
        self.condition = threading.Condition()

    def submit_log(self, round_id, peer_id, log, stats):
        with self.condition:
            if round_id != self.round_id or peer_id not in {p['id'] for p in self.members}:
                return False
            if peer_id not in self.received:
                self.received[peer_id] = {'log': log, 'stats': stats}
                self.condition.notify_all()
                print(f'[coordinator] log {len(self.received)}/{len(self.members)} '
                      f'from {peer_id} ({len(log)} messages)', flush=True)
        return True

    def ping(self):
        return True

    def start(self, members, n_msgs, timeout, log_dir):
        if not members:
            raise ValueError('No registered peers')
        round_id = uuid.uuid4().hex
        with self.condition:
            self.round_id = round_id
            self.members = members
            self.received = {}
        print(f'\n[coordinator] Round {round_id}: {len(members)} peers, '
              f'{n_msgs} messages per peer', flush=True)
        # Two-phase start: every receiver is armed before anyone begins sending.
        for member in members:
            with rpc_client(member['host'], member['rpc_port']) as rpc:
                rpc.prepare_round(round_id, n_msgs, members)
        for member in members:
            with rpc_client(member['host'], member['rpc_port']) as rpc:
                rpc.start_round(round_id)
        deadline = time.monotonic() + timeout
        with self.condition:
            while len(self.received) < len(members):
                remain = deadline - time.monotonic()
                if remain <= 0:
                    missing = [m['id'] for m in members if m['id'] not in self.received]
                    statuses = {}
                    for m in members:
                        try:
                            with rpc_client(m['host'], m['rpc_port']) as rpc:
                                statuses[m['id']] = rpc.get_status()
                        except Exception as exc:
                            statuses[m['id']] = {'error': str(exc)}
                    raise TimeoutError(f'Missing logs from {missing}; statuses={statuses}')
                self.condition.wait(timeout=min(remain, 0.5))
            received = dict(self.received)
        result = compare_logs(received, members, n_msgs)
        result['round_id'] = round_id
        result['timestamp_utc'] = datetime.now(timezone.utc).isoformat()
        output = Path(log_dir)
        output.mkdir(parents=True, exist_ok=True)
        outfile = output / f'round_{round_id}.json'
        outfile.write_text(json.dumps({'summary': result, 'logs': received}, indent=2),
                           encoding='utf-8')
        print(f'[coordinator] FIFO valid: {result["fifo_valid"]}; '
              f'complete: {result["complete"]}; '
              f'cross-peer differences (NOT FIFO violations): '
              f'{result["divergent_positions"]}', flush=True)
        print(f'[coordinator] total UDP retransmissions: {result["retransmissions"]}',
              flush=True)
        print(f'[coordinator] saved: {outfile}', flush=True)
        for member in members:
            try:
                with rpc_client(member['host'], member['rpc_port']) as rpc:
                    rpc.finish_round(round_id)
            except Exception as exc:
                print(f'[coordinator] finish notification failed: {exc}', flush=True)
        return result


def compare_logs(received, members, n_msgs):
    ids = [m['id'] for m in members]
    expected = Counter((pid, seq) for pid in ids for seq in range(n_msgs))
    fifo_violations = {}
    missing = {}
    duplicate = {}
    normalized = {}
    for pid in ids:
        log = received.get(pid, {}).get('log', [])
        next_seq = {sender: 0 for sender in ids}
        violations = []
        observed = []
        for index, item in enumerate(log):
            try:
                sender, seq = item['sender'], item['seq']
                if sender not in next_seq or not isinstance(seq, int):
                    raise ValueError('Invalid source/sequence')
                if seq != next_seq[sender]:
                    violations.append({'position': index, 'sender': sender,
                                       'expected': next_seq[sender], 'received': seq})
                next_seq[sender] = seq + 1
                observed.append((sender, seq))
            except (KeyError, TypeError, ValueError):
                violations.append({'position': index, 'invalid': item})
        count = Counter(observed)
        fifo_violations[pid] = violations
        missing[pid] = sum((expected - count).values())
        duplicate[pid] = sum((count - expected).values())
        normalized[pid] = observed
    ref = normalized[ids[0]]
    # Cross-sender interleavings may differ even with perfectly correct FIFO.
    divergent = sum(any(i >= len(normalized[pid]) or normalized[pid][i] != ref[i]
                        for pid in ids[1:]) for i in range(len(ref)))
    result = {'peers': ids, 'messages_per_peer': n_msgs,
              'messages_expected_per_log': len(ids) * n_msgs,
              'fifo_valid': all(not errs for errs in fifo_violations.values()),
              'complete': all(missing[p] == 0 and duplicate[p] == 0
                              and len(normalized[p]) == len(expected) for p in ids),
              'fifo_violations': fifo_violations, 'missing_messages': missing,
              'duplicate_messages': duplicate, 'divergent_positions': divergent,
              'retransmissions': sum(received[p]['stats'].get('retransmissions', 0)
                                     for p in received)}
    return result


def main():
    parser = argparse.ArgumentParser(description='Multicast experiment coordinator')
    parser.add_argument('--bind', default='0.0.0.0')
    parser.add_argument('--port', type=int, default=SERVER_PORT)
    parser.add_argument('--group-host', default=GROUPMNGR_ADDR)
    parser.add_argument('--group-port', type=int, default=GROUPMNGR_TCP_PORT)
    parser.add_argument('--peers', type=int, default=N)
    parser.add_argument('--messages', type=int, default=None,
                        help='Run one round automatically and exit')
    parser.add_argument('--timeout', type=float, default=40)
    parser.add_argument('--log-dir', default='logs')
    args = parser.parse_args()
    coord = Coordinator()
    with ThreadedRPCServer((args.bind, args.port), allow_none=True,
                           logRequests=False) as server:
        server.register_instance(coord)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        print(f'[coordinator] XML-RPC listening at {args.bind}:{args.port}', flush=True)
        try:
            while True:
                if args.messages is None:
                    n_msgs = int(input('Messages per peer (0 to exit): '))
                    if n_msgs == 0:
                        break
                else:
                    n_msgs = args.messages
                if n_msgs <= 0:
                    raise ValueError('Use --messages > 0')
                with rpc_client(args.group_host, args.group_port) as rpc:
                    members = rpc.list_peers()
                if len(members) != args.peers:
                    raise RuntimeError(f'Expected {args.peers} peers, got {len(members)}. '
                                       'Start all peers before starting the experiment.')
                result = coord.start(members, n_msgs, args.timeout, args.log_dir)
                if not result['fifo_valid'] or not result['complete']:
                    raise RuntimeError('FAIL: FIFO or completeness validation failed')
                if args.messages is not None:
                    break
        finally:
            server.shutdown()


if __name__ == '__main__':
    main()

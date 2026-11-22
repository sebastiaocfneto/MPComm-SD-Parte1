"""Reliable FIFO peer-to-peer multicast over UDP sockets.

Reliability model: retry+ACK handles *transient* UDP loss, duplicates and reordering.
FIFO: per-sender sequence numbers, hold-back queues and in-order delivery.
No total order, durability, crash recovery or Byzantine fault tolerance is claimed.
"""
import json
import random
import threading
import time


def encode_packet(packet):
    return json.dumps(packet, separators=(',', ':'), sort_keys=True).encode('utf-8')


def decode_packet(data):
    packet = json.loads(data.decode('utf-8'))
    if not isinstance(packet, dict) or packet.get('type') not in ('DATA', 'ACK', 'FIN'):
        raise ValueError('Invalid UDP protocol packet')
    return packet


class RoundState:
    def __init__(self, round_id, peer_id, members, n_msgs):
        self.round_id = round_id
        self.peer_id = peer_id
        self.members = {m['id']: dict(m) for m in members}
        if peer_id not in self.members:
            raise ValueError('Local peer is missing from membership snapshot')
        self.n_msgs = n_msgs
        self.lock = threading.RLock()
        self.next_seq = {pid: 0 for pid in self.members}
        self.holdback = {pid: {} for pid in self.members}
        self.finished_senders = set()
        self.delivered = []
        # (destination peer ID, packet kind, sequence number) -> pending ACK
        self.pending = {}
        self.tx_complete = False
        self.started = False
        self.error = None
        self.reported = False
        self.sent_udp = 0
        self.retransmissions = 0
        self.dropped_udp = 0
        self.drop_seen = set()

    def receive_data(self, sender, seq):
        """Receive any sequence order, deliver only contiguous sequences per sender.

        Returns False for malformed/out-of-range messages and True for data or
        duplicate data that is safe to acknowledge.
        """
        with self.lock:
            if sender not in self.members or not isinstance(seq, int) or isinstance(seq, bool):
                return False
            if seq < 0 or seq >= self.n_msgs:
                return False
            if seq >= self.next_seq[sender]:
                self.holdback[sender][seq] = True
            while self.next_seq[sender] in self.holdback[sender]:
                current = self.next_seq[sender]
                del self.holdback[sender][current]
                self.delivered.append({'sender': sender, 'seq': current})
                self.next_seq[sender] += 1
            return True

    def receive_fin(self, sender, count):
        with self.lock:
            if sender not in self.members or count != self.n_msgs:
                return False
            self.finished_senders.add(sender)
            return True

    def all_received(self):
        with self.lock:
            return (len(self.finished_senders) == len(self.members)
                    and all(n == self.n_msgs for n in self.next_seq.values()))

    def stats(self):
        with self.lock:
            return {'delivered': len(self.delivered), 'expected': len(self.members) * self.n_msgs,
                    'sent_udp': self.sent_udp, 'retransmissions': self.retransmissions,
                    'dropped_udp': self.dropped_udp, 'tx_complete': self.tx_complete,
                    'rx_complete': self.all_received(), 'reported': self.reported,
                    'error': self.error}

    def should_drop_first(self, kind, sender, seq, every):
        """Deterministic one-time packet loss used only for test demonstrations."""
        if every <= 0 or seq is None or (seq + 1) % every:
            return False
        key = (kind, sender, seq)
        with self.lock:
            if key in self.drop_seen:
                return False
            self.drop_seen.add(key)
            self.dropped_udp += 1
            return True


class MulticastSender:
    def __init__(self, state, sendto, retry_seconds=0.15, max_attempts=80,
                 interval_seconds=0.025):
        self.state = state
        self.sendto = sendto  # callable (bytes, (host,port))
        self.retry_seconds = retry_seconds
        self.max_attempts = max_attempts
        self.interval_seconds = interval_seconds

    def _send(self, dest, packet, retry=False):
        with self.state.lock:
            endpoint = self.state.members[dest]
            self.sendto(encode_packet(packet), (endpoint['host'], endpoint['udp_port']))
            self.state.sent_udp += 1
            if retry:
                self.state.retransmissions += 1

    def _queue(self, dest, packet):
        key = (dest, packet['type'], packet.get('seq', -1))
        now = time.monotonic()
        # Insert before sending: an ACK may arrive immediately.
        with self.state.lock:
            self.state.pending[key] = {'packet': packet, 'last': now, 'attempts': 1}
        self._send(dest, packet)

    def handle_ack(self, sender, kind, seq):
        with self.state.lock:
            self.state.pending.pop((sender, kind, seq), None)

    def run(self):
        try:
            myid = self.state.peer_id
            peers = [pid for pid in self.state.members if pid != myid]
            for seq in range(self.state.n_msgs):
                # Local delivery is not subject to UDP packet loss.
                self.state.receive_data(myid, seq)
                packet = {'type': 'DATA', 'round': self.state.round_id,
                          'src': myid, 'seq': seq}
                for pid in peers:
                    self._queue(pid, packet)
                if self.interval_seconds:
                    time.sleep(random.uniform(0, self.interval_seconds * 2))
            self.state.receive_fin(myid, self.state.n_msgs)
            fin = {'type': 'FIN', 'round': self.state.round_id, 'src': myid,
                   'count': self.state.n_msgs}
            for pid in peers:
                self._queue(pid, fin)
            # Keep retransmitting only the destination+sequence ACKs still missing.
            while True:
                with self.state.lock:
                    if not self.state.pending:
                        self.state.tx_complete = True
                        break
                    now = time.monotonic()
                    to_retry = []
                    for key, entry in list(self.state.pending.items()):
                        if now - entry['last'] >= self.retry_seconds:
                            if entry['attempts'] >= self.max_attempts:
                                raise TimeoutError(f'No ACK from {key[0]} for {key[1:]}')
                            entry['last'] = now
                            entry['attempts'] += 1
                            to_retry.append((key[0], entry['packet']))
                for pid, packet in to_retry:
                    # ACK may race with retry; harmless due to deduplication.
                    self._send(pid, packet, retry=True)
                time.sleep(min(self.retry_seconds / 3, 0.05))
        except Exception as exc:
            with self.state.lock:
                self.state.error = str(exc)

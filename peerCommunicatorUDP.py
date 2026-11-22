"""Peer with RPC control plane and reliable FIFO UDP data plane."""
import argparse
import json
import socket
import threading
import time
import uuid

from constMP import (GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT,
                     PEER_TCP_PORT, PEER_UDP_PORT, SERVER_ADDR, SERVER_PORT)
from reliable_multicast import MulticastSender, RoundState, decode_packet, encode_packet
from rpc_utils import ThreadedRPCServer, rpc_client


class Peer:
    def __init__(self, args):
        self.args = args
        self.peer_id = args.peer_id or str(uuid.uuid4())[:12]
        self.udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp.bind((args.bind, args.udp_port))
        self.udp.settimeout(0.3)
        self.lock = threading.RLock()
        self.round = None
        self.sender = None
        self.closed = threading.Event()
        self.worker = None
        self.reporter = None
        self.rpc_server = None
        self.drop_first_data_every = args.drop_first_data_every
        self.drop_first_ack_every = args.drop_first_ack_every
        threading.Thread(target=self.udp_loop, daemon=True).start()

    def udp_loop(self):
        while not self.closed.is_set():
            try:
                data, address = self.udp.recvfrom(65507)
            except socket.timeout:
                continue
            except OSError:
                break
            try:
                packet = decode_packet(data)
                with self.lock:
                    state = self.round
                    sender = self.sender
                if state is None or state.round_id != packet.get('round'):
                    continue
                source_id = packet.get('src')
                if source_id not in state.members or source_id == self.peer_id:
                    continue
                # Validate that UDP datagrams originate from the declared endpoint.
                endpoint = state.members[source_id]
                if address[1] != endpoint['udp_port']:
                    continue
                kind = packet['type']
                if kind == 'ACK':
                    if sender is None or packet.get('ack_type') not in ('DATA', 'FIN'):
                        continue
                    seq = packet.get('seq', -1)
                    if not isinstance(seq, int):
                        continue
                    if state.should_drop_first('ACK', source_id, seq,
                                               self.drop_first_ack_every):
                        continue
                    sender.handle_ack(source_id, packet['ack_type'], seq)
                elif kind == 'DATA':
                    seq = packet.get('seq')
                    if not isinstance(seq, int) or isinstance(seq, bool):
                        continue
                    if state.should_drop_first('DATA', source_id, seq,
                                               self.drop_first_data_every):
                        continue
                    if state.receive_data(source_id, seq):
                        self._ack(address, state, 'DATA', seq)
                elif kind == 'FIN':
                    if state.receive_fin(source_id, packet.get('count')):
                        self._ack(address, state, 'FIN', -1)
            except (ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
                print(f'[peer {self.peer_id}] ignored invalid UDP packet: {exc}', flush=True)

    def _ack(self, address, state, kind, seq):
        ack = {'type': 'ACK', 'round': state.round_id, 'src': self.peer_id,
               'ack_type': kind, 'seq': seq}
        self.udp.sendto(encode_packet(ack), address)

    def ping(self):
        return True

    def prepare_round(self, round_id, n_msgs, members):
        if not round_id or not isinstance(n_msgs, int) or n_msgs < 0:
            raise ValueError('Invalid round parameters')
        if not members or len({m['id'] for m in members}) != len(members):
            raise ValueError('Invalid membership snapshot')
        with self.lock:
            if self.round is not None and self.round.round_id == round_id:
                return {'peer_id': self.peer_id, 'prepared': True}
            if self.round is not None and not self.round.reported:
                raise RuntimeError('Previous round is still active')
            self.round = RoundState(round_id, self.peer_id, members, n_msgs)
            self.sender = MulticastSender(self.round, self.udp.sendto,
                retry_seconds=self.args.retry_ms/1000,
                max_attempts=self.args.max_attempts,
                interval_seconds=self.args.interval_ms/1000)
        return {'peer_id': self.peer_id, 'prepared': True}

    def start_round(self, round_id):
        with self.lock:
            if not self.round or self.round.round_id != round_id:
                raise ValueError('Round not prepared')
            if self.round.started:
                return {'peer_id': self.peer_id, 'started': True}
            self.round.started = True
            state = self.round
            sender = self.sender
            self.worker = threading.Thread(target=sender.run, daemon=True)
            self.reporter = threading.Thread(target=self._report_when_ready,
                                             args=(state,), daemon=True)
            self.worker.start()
            self.reporter.start()
        return {'peer_id': self.peer_id, 'started': True}

    def _report_when_ready(self, state):
        while not self.closed.is_set():
            with state.lock:
                if state.error:
                    print(f'[peer {self.peer_id}] failed: {state.error}', flush=True)
                    return
                ready = state.tx_complete and state.all_received()
                if ready:
                    data = list(state.delivered)
                    stats = state.stats()
            if ready:
                try:
                    with rpc_client(self.args.server_host, self.args.server_port, timeout=5) as rpc:
                        result = rpc.submit_log(state.round_id, self.peer_id, data, stats)
                    if result:
                        with state.lock:
                            state.reported = True
                        print(f'[peer {self.peer_id}] reported {len(data)} messages; '
                              f'retransmissions={stats["retransmissions"]}', flush=True)
                        return
                except Exception as exc:
                    print(f'[peer {self.peer_id}] retrying RPC log report: {exc}', flush=True)
            time.sleep(0.2)

    def get_status(self):
        with self.lock:
            state = self.round
        return {'peer_id': self.peer_id, **(state.stats() if state else {'round': None})}

    def finish_round(self, round_id):
        with self.lock:
            if not self.round or self.round.round_id != round_id:
                return False
            if not self.round.reported:
                return False
            # Retain completed round until next prepare: UDP listener continues
            # acknowledging retransmitted FIN/DATA until the next round starts.
            return True

    def shutdown(self):
        self.closed.set()
        if self.rpc_server:
            threading.Thread(target=self.rpc_server.shutdown, daemon=True).start()
        return True

    def run(self):
        advertise_host = self.args.advertise_host or self.args.bind
        if advertise_host == '0.0.0.0':
            raise ValueError('Specify --advertise-host when binding to 0.0.0.0')
        with rpc_client(self.args.group_host, self.args.group_port) as rpc:
            rpc.register_peer(self.peer_id, advertise_host,
                              self.args.udp_port, self.args.rpc_port)
        print(f'[peer {self.peer_id}] UDP {self.args.bind}:{self.args.udp_port}, '
              f'RPC {self.args.bind}:{self.args.rpc_port}', flush=True)
        try:
            with ThreadedRPCServer((self.args.bind, self.args.rpc_port), allow_none=True,
                                   logRequests=False) as server:
                self.rpc_server = server
                server.register_instance(self)
                server.serve_forever(poll_interval=0.1)
        finally:
            self.closed.set()
            self.udp.close()
            try:
                with rpc_client(self.args.group_host, self.args.group_port) as rpc:
                    rpc.unregister_peer(self.peer_id)
            except Exception:
                pass


def main():
    p = argparse.ArgumentParser(description='Reliable FIFO UDP peer + XML-RPC control')
    p.add_argument('--bind', default='127.0.0.1', help='Local bind address')
    p.add_argument('--advertise-host', default=None, help='Address accessible by peers')
    p.add_argument('--peer-id', default=None)
    p.add_argument('--udp-port', type=int, default=PEER_UDP_PORT)
    p.add_argument('--rpc-port', type=int, default=PEER_TCP_PORT)
    p.add_argument('--group-host', default=GROUPMNGR_ADDR)
    p.add_argument('--group-port', type=int, default=GROUPMNGR_TCP_PORT)
    p.add_argument('--server-host', default=SERVER_ADDR)
    p.add_argument('--server-port', type=int, default=SERVER_PORT)
    p.add_argument('--retry-ms', type=int, default=150)
    p.add_argument('--max-attempts', type=int, default=80)
    p.add_argument('--interval-ms', type=int, default=25)
    p.add_argument('--drop-first-data-every', type=int, default=0,
                   help='Test only: discard first DATA reception of every Nth sequence')
    p.add_argument('--drop-first-ack-every', type=int, default=0,
                   help='Test only: discard first incoming ACK of every Nth sequence')
    args = p.parse_args()
    Peer(args).run()


if __name__ == '__main__':
    main()

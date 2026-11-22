import unittest
from collections import Counter
from reliable_multicast import RoundState, decode_packet, encode_packet
from comparisonServer import compare_logs


MEMBERS = [
    dict(id='A', host='127.0.0.1', udp_port=6001, rpc_port=7001),
    dict(id='B', host='127.0.0.1', udp_port=6002, rpc_port=7002),
    dict(id='C', host='127.0.0.1', udp_port=6003, rpc_port=7003),
]


class ProtocolUnitTests(unittest.TestCase):
    def test_fifo_buffering_out_of_order(self):
        state = RoundState('r1', 'A', MEMBERS, 4)
        for seq in (2, 3, 0):
            self.assertTrue(state.receive_data('B', seq))
        self.assertEqual([0], [m['seq'] for m in state.delivered])
        self.assertTrue(state.receive_data('B', 1))
        self.assertEqual([0, 1, 2, 3], [m['seq'] for m in state.delivered])

    def test_duplicate_suppression(self):
        state = RoundState('r1', 'A', MEMBERS, 2)
        for seq in (0, 0, 1, 0, 1):
            state.receive_data('B', seq)
        self.assertEqual(2, len(state.delivered))

    def test_fin_must_not_end_before_all_data(self):
        state = RoundState('r1', 'A', MEMBERS, 2)
        for member in ('A', 'B', 'C'):
            state.receive_fin(member, 2)
            state.receive_data(member, 1)
        self.assertFalse(state.all_received())
        for member in ('A', 'B', 'C'):
            state.receive_data(member, 0)
        self.assertTrue(state.all_received())

    def test_deterministic_loss_once(self):
        state = RoundState('r1', 'A', MEMBERS, 4)
        self.assertTrue(state.should_drop_first('DATA', 'B', 1, 2))
        self.assertFalse(state.should_drop_first('DATA', 'B', 1, 2))
        self.assertFalse(state.should_drop_first('DATA', 'B', 0, 2))

    def test_invalid_packets(self):
        state = RoundState('r1', 'A', MEMBERS, 4)
        self.assertFalse(state.receive_data('unknown', 0))
        self.assertFalse(state.receive_data('B', 4))
        self.assertFalse(state.receive_fin('B', 10))
        packet = {'type': 'DATA', 'round': 'r1', 'src': 'B', 'seq': 0}
        self.assertEqual(packet, decode_packet(encode_packet(packet)))

    def test_fifo_is_not_total_order(self):
        logs = {
            'A': {'log': [{'sender': 'A', 'seq': 0}, {'sender': 'B', 'seq': 0},
                          {'sender': 'C', 'seq': 0}], 'stats': {}},
            'B': {'log': [{'sender': 'C', 'seq': 0}, {'sender': 'A', 'seq': 0},
                          {'sender': 'B', 'seq': 0}], 'stats': {}},
            'C': {'log': [{'sender': 'B', 'seq': 0}, {'sender': 'C', 'seq': 0},
                          {'sender': 'A', 'seq': 0}], 'stats': {}},
        }
        report = compare_logs(logs, MEMBERS, 1)
        self.assertTrue(report['fifo_valid'])
        self.assertTrue(report['complete'])
        self.assertGreater(report['divergent_positions'], 0)

    def test_fifo_failure_detected(self):
        log = [{'sender': id, 'seq': seq} for id in ('A', 'B', 'C') for seq in (0, 1)]
        received = {id: {'log': list(log), 'stats': {}} for id in ('A', 'B', 'C')}
        received['C']['log'][0], received['C']['log'][1] = received['C']['log'][1], received['C']['log'][0]
        report = compare_logs(received, MEMBERS, 2)
        self.assertFalse(report['fifo_valid'])


if __name__ == '__main__':
    unittest.main()

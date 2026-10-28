"""Controlled checkpoint barrier, actual TCP BUSY response and correlated retry."""
import argparse
import concurrent.futures
import hashlib
import json
from pathlib import Path
import select
import socket
import struct
import sys
import threading
import time

sys.path.insert(0, '/private/workspace/source/tests')
import test_cluster as harness
import test_rpc_network as wire
from test_cluster_snapshot import wait_for

EVENTS = []
LOCK = threading.Lock()


def fields(data):
    result = {}
    offset = 0
    while offset < len(data):
        tag, offset = wire.read_varint(data, offset)
        if tag & 7 == 0:
            value, offset = wire.read_varint(data, offset)
        elif tag & 7 == 2:
            count, offset = wire.read_varint(data, offset)
            value, offset = data[offset:offset + count], offset + count
        else:
            raise AssertionError('unexpected wire type')
        result[tag >> 3] = value
    return result


class RecordingProxy(harness.Proxy):
    def relay(self, connection):
        buffers = {False: bytearray(), True: bytearray()}
        with connection:
            try:
                with socket.create_connection(('127.0.0.1', self.port), timeout=0.4) as remote:
                    connection.settimeout(0.4)
                    remote.settimeout(0.4)
                    while not self.stop.is_set():
                        if (self.source in self.blocked or self.target in self.blocked or
                            (self.source, self.target) in self.blocked):
                            return
                        readable, _, _ = select.select([connection, remote], [], [], 0.05)
                        for current in readable:
                            data = current.recv(65536)
                            if not data:
                                return
                            response = current is remote
                            buffer = buffers[response]
                            buffer.extend(data)
                            while len(buffer) >= 4:
                                length, = struct.unpack('!I', buffer[:4])
                                assert length <= 65536
                                if len(buffer) < length + 4:
                                    break
                                decoded = fields(bytes(buffer[4:length + 4]))
                                del buffer[:length + 4]
                                event = dict(mono_ns=time.monotonic_ns(), source=self.source,
                                             target=self.target, response=response,
                                             request_id=decoded.get(1, 0))
                                if response:
                                    event['code'] = decoded.get(2, 0)
                                if 7 in decoded:
                                    peer = fields(decoded[7])
                                    event.update(sequence=peer[3], kind=next(
                                        k for k in range(4, 10) if k in peer))
                                with LOCK:
                                    EVENTS.append(event)
                            (remote if response is False else connection).sendall(data)
            except OSError:
                return


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--node', type=Path, required=True)
    parser.add_argument('--fault-library', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir()
    harness.NODE = args.node.resolve()
    harness.CLIENT = Path('/private/workspace/source/build/bin/yewukv-client')
    harness.Proxy = RecordingProxy
    node_env = {i: dict(LD_PRELOAD=str(args.fault_library.resolve()),
                       YEWUKV_TEST_IO_FAULT=str(output / f'arm-{i}'),
                       YEWUKV_TEST_IO_MODE='gate-sync',
                       YEWUKV_TEST_IO_CHECKPOINT_ONLY='1',
                       YEWUKV_TEST_IO_ENTERED=str(output / f'entered-{i}'),
                       YEWUKV_TEST_IO_RELEASE=str(output / f'release-{i}'))
                for i in range(3)}
    cluster = harness.Cluster(output, request_timeout=3000, extra_args=[
        '--disk-executor', 'background', '--snapshot-encoder', 'background',
        '--snapshot-policy', 'fixed', '--snapshot-entries', '4',
        '--snapshot-bytes', '67108864'], node_env=node_env)
    result = dict(binary_sha256=hashlib.sha256(harness.NODE.read_bytes()).hexdigest())
    try:
        leader = cluster.leader()
        target, other = (leader + 1) % 3, (leader + 2) % 3
        result.update(leader=leader, checkpoint_follower=target, isolated_follower=other)
        assert cluster.call(leader, 'put', b'anchor', b'kept')['code'] == 0
        wait_for(cluster, lambda: all(cluster.status(i)['applied_index'] ==
                                     cluster.status(leader)['last_index'] for i in range(3)))
        result['before'] = [cluster.status(i) for i in range(3)]
        result['armed_ns'] = time.monotonic_ns()
        (output / f'arm-{target}').touch()
        for k in range(2):
            assert cluster.call(leader, 'put', f'trigger-{k}'.encode(), b'value')['code'] == 0
        wait_for(cluster, lambda: (output / f'entered-{target}').exists(), seconds=5)
        assert cluster.status(target)['persistence_pending'] == 1
        cluster.blocked.add(other)
        time.sleep(0.06)
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            submitted = time.monotonic_ns()
            future = pool.submit(cluster.call, leader, 'put', b'probe', b'after-barrier', timeout=5)
            try:
                def observed_wait():
                    state = cluster.status(target)
                    with LOCK:
                        busy = [e for e in EVENTS if e['mono_ns'] >= result['armed_ns'] and
                                e['source'] == leader and e['target'] == target and
                                e.get('code') == 4]
                    return (state, busy) if busy or state.get('peer_deferred_pending', 0) else None
                state, busy = wait_for(cluster, observed_wait, seconds=2)
                time.sleep(0.03)
                assert not future.done(), 'write acknowledged before the only available follower was durable'
                result['while_gated'] = state
                result['busy_while_gated'] = busy
                result['probe_submitted_ns'] = submitted
                result['released_ns'] = time.monotonic_ns()
            finally:
                (output / f'release-{target}').touch()
            reply = future.result(timeout=5)
            result['acknowledged_ns'] = time.monotonic_ns()
            assert reply['code'] == 0, reply
            result['reply'] = {k:v for k,v in reply.items() if not isinstance(v, bytes)}
        cluster.blocked.clear()
        wait_for(cluster, lambda: all(cluster.status(i)['applied_index'] ==
                                     cluster.status(leader)['last_index'] for i in range(3)))
        result['after'] = [cluster.status(i) for i in range(3)]
        for i in range(3):
            cluster.kill(i)
            (output / f'arm-{i}').unlink(missing_ok=True)
        for i in range(3):
            cluster.start(i)
        current = cluster.eventually_value(b'probe', b'after-barrier')
        assert cluster.call(current, 'get', b'anchor')['value'] == b'kept'
        result['restart_check'] = True
        result['release_to_ack_ms'] = (result['acknowledged_ns'] - result['released_ns']) / 1e6
        result['submitted_to_ack_ms'] = (result['acknowledged_ns'] - submitted) / 1e6
        with LOCK:
            result['events'] = EVENTS.copy()
        (output / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
        print(json.dumps({k:result[k] for k in ['binary_sha256','release_to_ack_ms',
                          'submitted_to_ack_ms','restart_check']}))
    finally:
        cluster.blocked.clear()
        for i in range(3):
            (output / f'release-{i}').touch()
        cluster.close()


if __name__ == '__main__':
    main()

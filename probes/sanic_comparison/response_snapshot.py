"""Owned response snapshots: complete backends, twenty windows then six profiles."""
import argparse
from dataclasses import asdict
import hashlib
import json
from pathlib import Path

from probes.sanic_comparison import native_request, run, server


def case_order(smoke=False, *, include_origin=False):
    if include_origin:
        raise ValueError('owned snapshots compare only baseline and candidate')
    order = native_request.case_order(smoke=smoke)
    if smoke:
        return order
    return order[:20] + [(f'{workload}-profile-{version}', version, workload, 'cprofile')
                        for workload in run.EXPECTED for version in ('baseline', 'candidate')]


def validate_response_path(row, version):
    if version not in ('baseline', 'candidate') or row['mode'] != 'candidate':
        raise ValueError('invalid snapshot comparison version')
    contract = row['ready']['workers'][0]['application_contract']
    file = run.ROOT / 'probes/sanic_boundary/application.py'
    if (contract['json_codec'] != 'stdlib' or contract['native_responses'] is not False or
            contract['bytes_headers'] is not True or contract['business'] != {
                'path': str(file), 'sha256': hashlib.sha256(file.read_bytes()).hexdigest()}):
        raise ValueError('snapshot business/response path mismatch')
    actual = json.loads(json.dumps(contract['server_config']))
    actual['http_port'] = 0
    expected = json.loads(json.dumps(asdict(server.native_config(0, [0, 1, 2], 'candidate').server)))
    if actual != expected:
        raise ValueError('snapshot server configuration mismatch')
    worker = row['server']['workers'][0]
    if worker['delta'].get('engine_native_response_queued', 0):
        raise ValueError('snapshot comparison requires the checked exit')
    if row.get('diagnostic') == 'cprofile':
        calls = worker['profile']['calls']
        count = lambda name: sum(c['calls'] for c in calls if name in c['function'])
        n = row['client']['requests']
        if (count('freeze_sanic_response') != (n if version == 'candidate' else 0) or
                count('processed_headers') != n or count('_sanitize_header_value') != 2*n or
                count('ApplicationResponseBatch.append_result')):
            raise ValueError('snapshot/header mechanism calls mismatch')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--baseline-record', type=Path, required=True)
    parser.add_argument('--candidate-commit', required=True)
    parser.add_argument('--smoke', action='store_true')
    args = parser.parse_args(argv)
    command = ['--out', str(args.out), '--baseline-record', str(args.baseline_record),
               '--candidate-commit', args.candidate_commit, '--include-engine']
    if args.smoke:
        command.append('--smoke')
    return native_request.main(command, order_factory=case_order, row_validator=validate_response_path)


if __name__ == '__main__':
    main()

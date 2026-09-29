"""Synthetic launch cleanup and source-binding check; never contacts AWS."""
import copy
import json
from pathlib import Path
import tempfile
from unittest.mock import Mock, patch

from scripts import launch_native_peer_1m_spot as controller


def main():
    config = json.loads(controller.CONFIG.read_text())
    with tempfile.TemporaryDirectory() as directory:
        proof_path = Path(directory) / 'verification.json'
        binary = dict(config['binary'])
        proof = dict(valid_check=True, state='terminated', no_corpus_query=True,
            qualification='private-listener HTTP example only', library_assurance_reused=2696,
            compiled_http_sha256=controller.sha(Path('crates/borsuk/examples/two_bit_http.rs').read_bytes()),
            binary=binary)
        proof_path.write_text(json.dumps(proof))
        qualified = copy.deepcopy(config)
        qualified['http_build_verification'] = dict(path=str(proof_path), sha256=controller.sha(proof_path.read_bytes()))
        assert controller.qualified_binary(qualified) == binary
        proof['state'] = 'running'
        proof_path.write_text(json.dumps(proof))
        qualified['http_build_verification']['sha256'] = controller.sha(proof_path.read_bytes())
        try:
            controller.qualified_binary(qualified)
        except AssertionError:
            pass
        else:
            raise AssertionError('live build accepted')
    historical = copy.deepcopy(config)
    historical.pop('http_build_verification', None)
    try:
        controller.qualified_binary(historical)
    except ValueError:
        pass
    else:
        raise AssertionError('historical loopback binary accepted')
    with patch.object(controller, 'qualified_binary', return_value=config['binary']):
        controller.preflight(config)
    for field in ('authority', 'inputs', 'indexes'):
        bad = copy.deepcopy(config)
        if field == 'authority':
            bad['items'][0][field]['root_sha256'] = '0' * 64
        elif field == 'inputs':
            bad['items'][0][field]['requests']['sha256'] = '0' * 64
        else:
            bad['items'][0][field]['10'] += '-wrong'
        try:
            controller.preflight(bad)
        except AssertionError:
            pass
        else:
            raise AssertionError('source mutation accepted: ' + field)
    for role in ('server', 'client'):
        body = controller.user_data(role, 'a' * 40, 'b' * 64, 'source', 'synthetic', config)
        assert '--on-active=1800s' in body and 'MemorySwapMax=0' in body
        assert ('MemoryMax=8G' if role == 'server' else 'MemoryMax=512M') in body
    ec2, s3 = Mock(), Mock()
    ec2.describe_instances.return_value = {'Reservations': []}
    ec2.describe_security_groups.return_value = {'SecurityGroups': [{'IpPermissions': [
        {'IpProtocol': 'tcp', 'FromPort': 8080, 'ToPort': 8080,
         'UserIdGroupPairs': [{'GroupId': controller.SECURITY_GROUP}]}]}]}
    ec2.describe_subnets.return_value = {'Subnets': [{'AvailabilityZone': 'synthetic'}]}
    timestamp = Mock()
    timestamp.isoformat.return_value = '2026-09-29T00:00:00Z'
    ec2.describe_spot_price_history.return_value = {'SpotPriceHistory': [
        {'SpotPrice': '0.18', 'Timestamp': timestamp}]}
    ec2.run_instances.side_effect = [
        {'Instances': [{'InstanceId': 'i-owned-server', 'PrivateIpAddress': '10.0.0.1'}]},
        RuntimeError('synthetic second launch failure')]
    session = Mock()
    session.client.side_effect = lambda name: ec2 if name == 'ec2' else s3
    def missing(client, key):
        return True
    def git_output(args, **kwargs):
        if args[1] == 'status':
            return ''
        if args[1] == 'rev-parse':
            return 'a' * 40
        if args[1] == 'archive':
            return b'synthetic archive'
        raise AssertionError(args)
    with tempfile.TemporaryDirectory() as directory, \
         patch.object(controller, 'ROOT', Path(directory)), \
         patch.object(controller, 'user_data', return_value='synthetic startup'), \
         patch.object(controller.boto3, 'Session', return_value=session), \
         patch.object(controller.subprocess, 'check_output', side_effect=git_output), \
         patch.object(controller.subprocess, 'run'), \
         patch.object(controller, 'missing', side_effect=missing), \
         patch.object(controller, 'put_if_absent'):
        # preflight above is real; temporary receipt root has no historical proofs.
        with patch.object(controller, 'preflight'):
            try:
                controller.main('a0001')
            except RuntimeError as error:
                assert str(error) == 'synthetic second launch failure'
            else:
                raise AssertionError('launch failure swallowed')
        progress = json.loads((Path(directory) / 'peer-1m/a0001/launch-progress.json').read_text())
        assert progress == {'server': {'instance_id': 'i-owned-server', 'private_ip': '10.0.0.1'}}
        close = json.loads((Path(directory) / 'peer-1m/a0001/aws-closeout.json').read_text())
        assert close['state'] == 'terminated' and close['nodes'] == progress
    ec2.terminate_instances.assert_called_once_with(InstanceIds=['i-owned-server'])
    ec2.get_waiter.return_value.wait.assert_called_once_with(InstanceIds=['i-owned-server'])
    print('PASS source binding, role startup syntax/limits, actual partial-launch cleanup')


if __name__ == '__main__':
    main()

"""Offline stdlib checks for the single-test On-Demand controller; never runs Cargo/AWS."""
import copy
import io
import json
import shutil
import tarfile
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import launch_native_cargo_pilot as pilot


def config():
    return dict(pilot.FIXED, controller_authority_pending=False, code_freeze_ns=1790980000000000000,
        controller_code_sha256={}, native_source_manifest={}, toolchain='1.98.0',
        target='x86_64-unknown-linux-gnu', machine_limit_seconds=3600, test_limit_seconds=2100,
        compute_cap_usd=.50, on_demand_usd_per_hour=.4074, price_observed_ns=1790980000000000000,
        price_evidence='root-frozen price observation', storage_cap_usd=1., gp3_gib_month_usd=.0952,
        root_gib=24, storage_ttl_seconds=86400, image_id='ami-0b8a830d6339a9758',
        subnet_id='subnet-034528fbd6977848f', availability_zone='eu-central-1a',
        security_group='sg-0123456789abcdef0', instance_profile_arn=pilot.peer.PROFILE_ARN,
        output_root='docs/research/cargo-pilot', s3_prefix='research/cargo-pilot-',
        cache=dict(volume_id=None, owned_volume='root-pilot-cache-001', size_gib=32,
                   availability_zone='eu-central-1a', expires_ns=1791066400000000000))


def counters():
    return dict(cgroup='/mock', observer_pid=1, process_ids=[1], pids_peak='2',
        **{'memory.max':str(8*1024**3), 'memory.peak':'100', 'memory.swap.max':'0',
           'memory.swap.peak':'0', 'memory.swap.events':'high 0\nmax 0\nfail 0',
           'memory.events':'low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\noom_group_kill 0',
           'cpu.max':'200000 100000', 'cpu.stat':'usage_usec 10', 'pids.max':'512',
           'pids.current':'1', 'pids.events':'max 0', 'host_memory_pressure':'some avg10=0.00\nfull avg10=0.00',
           'memory.pressure':'some avg10=0.00\nfull avg10=0.00'})


class PilotChecks(unittest.TestCase):
    def test_request_is_one_on_demand_with_disposable_root_only(self):
        request = pilot.launch_request(config(), 'a0001', '#!/bin/bash\n')
        self.assertNotIn('InstanceMarketOptions', request)
        self.assertEqual((request['MinCount'], request['MaxCount']), (1, 1))
        self.assertEqual(request['InstanceType'], 'c7i.2xlarge')
        self.assertEqual(len(request['BlockDeviceMappings']), 1)
        self.assertTrue(request['BlockDeviceMappings'][0]['Ebs']['DeleteOnTermination'])
        self.assertEqual(request['MetadataOptions']['HttpTokens'], 'required')

    def test_exact_actual_test_required(self):
        good = f'running 1 test\ntest {pilot.TEST} ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 42 filtered out; finished in 1.2s\n'
        self.assertEqual(pilot.test_result(good), dict(executed=1, passed=1, failed=0, ignored=0))
        for bad in ('', 'running 0 tests\ntest result: ok. 0 passed; 0 failed;', good+good,
                    good.replace(pilot.TEST, 'some_other_test'), good.replace('1 passed', '0 passed'),
                    good.replace('0 ignored', '1 ignored')):
            with self.assertRaises(AssertionError):
                pilot.test_result(bad)

    def test_volume_requires_owner_encryption_az_size_and_no_attachment(self):
        c = config()
        v = dict(VolumeId='vol-0123456789abcdef0', State='available', Encrypted=True,
                 VolumeType='gp3', Size=32, AvailabilityZone=c['availability_zone'], Attachments=[],
                 Tags=pilot.volume_tags(c))
        pilot.validate_volume(v, c)
        for key, value in (('Encrypted',False), ('State','in-use'), ('AvailabilityZone','eu-central-1b'),
                           ('Size',33), ('Tags',[]), ('Attachments',[dict(InstanceId='i-other')])):
            bad = dict(v, **{key:value})
            with self.assertRaises(AssertionError):
                pilot.validate_volume(bad, c)

    def test_ack_all_ids_cleanup_and_wait_before_collection(self):
        for failure in (None, RuntimeError('poll'), KeyboardInterrupt()):
            with self.subTest(failure=type(failure).__name__), tempfile.TemporaryDirectory() as tmp:
                out = Path(tmp)
                events = []
                ec2 = Mock()
                ec2.run_instances.return_value = dict(Instances=[dict(InstanceId='i-one'), dict(InstanceId='i-two')])
                def terminate(**kw):
                    self.assertEqual(kw['InstanceIds'], ['i-one','i-two'])
                    self.assertEqual(json.loads((out/'aws-launch.json').read_text())['nodes'],
                                     {'0':{'instance_id':'i-one'},'1':{'instance_id':'i-two'}})
                    events.append('terminate')
                ec2.terminate_instances.side_effect = terminate
                ec2.get_waiter.return_value.wait.side_effect = lambda **kw: events.append(('wait', kw['InstanceIds']))
                with patch.object(pilot, 'poll', side_effect=failure), patch.object(pilot, 'collect', side_effect=lambda *a: events.append('collect')):
                    with self.assertRaises(AssertionError):  # two IDs violates one original worker
                        pilot.execute_launch(ec2, Mock(), config(), {}, 'a0001', out, 'prefix', '', None)
                self.assertEqual(events, ['terminate', ('wait',['i-one','i-two'])])

    def test_one_id_cleanup_on_failure_and_interrupt(self):
        for failure in (None, RuntimeError('poll'), KeyboardInterrupt()):
            with tempfile.TemporaryDirectory() as tmp:
                ec2 = Mock()
                ec2.run_instances.return_value = dict(Instances=[dict(InstanceId='i-one')])
                events = []
                ec2.get_waiter.return_value.wait.side_effect = lambda **kw: events.append('wait')
                with patch.object(pilot, 'poll', side_effect=failure), patch.object(pilot, 'collect', side_effect=lambda *a: events.append('collect')):
                    if failure:
                        with self.assertRaises(type(failure)):
                            pilot.execute_launch(ec2, Mock(), config(), {}, 'a0001', Path(tmp), 'prefix', '', None)
                    else:
                        pilot.execute_launch(ec2, Mock(), config(), {}, 'a0001', Path(tmp), 'prefix', '', None)
                self.assertEqual(events, ['wait', 'collect'])
                ec2.terminate_instances.assert_called_once_with(InstanceIds=['i-one'])
                ec2.run_instances.assert_called_once()

    def test_userdata_has_only_pilot_command_and_valid_shell(self):
        c = config()
        proof = dict(config_path='docs/research/cargo-pilot/config.json', config_sha256='a'*64,
            code_identity_sha256='b'*64, campaign_schema=pilot.SCHEMA, source_identity_sha256=pilot.NATIVE_IDENTITY,
            source_file_count=399, artifact_roster_sha256=pilot.sha(pilot.encoded(pilot.ARTIFACTS)),
            source_commit='d'*40, source_archive_sha256='e'*64,
            awscli_version=pilot.workspace.semantic.AWSCLI_VERSION, awscli_sha256=pilot.workspace.semantic.AWSCLI_SHA256)
        v = dict(volume_id='vol-0123456789abcdef0',created=True,ownedVolume=c['cache']['owned_volume'],
            size_gib=32,expires_ns=c['cache']['expires_ns'],availability_zone=c['availability_zone'],encrypted=True,auto_delete=False)
        body = pilot.user_data(c, proof, c['s3_prefix']+'a0001',v)
        self.assertLess(len(body.encode()), 16384)
        subprocess.run(['bash','-n'],input=body,text=True,check=True)
        self.assertIn('--default-toolchain 1.98.0', body)
        self.assertIn('--worker', body)
        self.assertNotIn('--release', body)
        self.assertNotIn('git ', body)
        self.assertIn('stamp userdata-start', body)
        self.assertIn('btime', body)
        self.assertIn('stamp "$phase"', body)
        self.assertIn('umount /mnt/cargo-pilot-cache', body)

    def test_mount_guard_and_cache_lock(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(pilot,'MOUNT',Path(tmp)):
            with self.assertRaises(AssertionError):
                pilot.mounted_cache(dict(volume_id='vol-0123456789abcdef0'))
            with patch.object(pilot,'mounted_cache'):
                with pilot.cache_lock({}):
                    with self.assertRaises(BlockingIOError):
                        with pilot.cache_lock({}):
                            self.fail('concurrent cache use')
                with pilot.cache_lock({}):
                    pass

    def test_ack_receipt_write_failure_still_terminates(self):
        with tempfile.TemporaryDirectory() as tmp:
            ec2 = Mock()
            ec2.run_instances.return_value = dict(Instances=[dict(InstanceId='i-one'),dict(InstanceId='i-two')])
            actual = pilot.write
            def fail(path, value):
                if path.name == 'aws-launch.json':
                    raise OSError('fsync failed')
                actual(path,value)
            with patch.object(pilot,'write',side_effect=fail), patch.object(pilot,'collect') as collect:
                with self.assertRaises(OSError):
                    pilot.execute_launch(ec2, Mock(),config(),{},'a0001',Path(tmp),'prefix','',None)
                collect.assert_not_called()
            ec2.terminate_instances.assert_called_once_with(InstanceIds=['i-one','i-two'])
            ec2.get_waiter.return_value.wait.assert_called_once_with(InstanceIds=['i-one','i-two'])

    def test_source_config_and_archive_authentication(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)/'repo'
            repo.mkdir()
            original = Path(pilot.__file__).resolve().parents[1]
            native = pilot.source_hashes(original)
            for name in set(native) | set(pilot.CODE):
                dest = repo/name
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(original/name,dest)
            manifest = dict(source_sha256=native, source_file_count=399,
                source_identity_sha256=pilot.NATIVE_IDENTITY, native_source_commit='a'*40)
            pilot.write(repo/'manifest.json',manifest)
            c = config()
            c['controller_code_sha256'] = {n:pilot.artifact(repo/n)['sha256'] for n in pilot.CODE}
            c['native_source_manifest'] = dict(path='manifest.json',**pilot.artifact(repo/'manifest.json'))
            pilot.write(repo/'config.json',c)
            digest = pilot.artifact(repo/'config.json')['sha256']
            c2, proof = pilot.qualify(repo,'config.json',digest)
            self.assertEqual(c2,c)
            archive = Path(tmp)/'source.tar.gz'
            with tarfile.open(archive,'w:gz') as output:
                for path in sorted(repo.rglob('*')):
                    if path.is_file():
                        output.add(path,arcname=str(path.relative_to(repo)),recursive=False)
            archive_sha = pilot.artifact(archive)['sha256']
            pilot.preflight('config.json',digest,archive,archive_sha,'a'*40,repo=repo)
            with self.assertRaisesRegex(AssertionError,'config drift'):
                pilot.qualify(repo,'config.json','0'*64)
            name = next(iter(native))
            path = repo/name
            path.write_bytes(path.read_bytes()+b'\n')
            with self.assertRaisesRegex(AssertionError,'source drift'):
                pilot.qualify(repo,'config.json',digest)
            shutil.copyfile(original/name,path)
            name = pilot.CODE[0]
            path = repo/name
            path.write_bytes(path.read_bytes()+b'\n')
            with self.assertRaisesRegex(AssertionError,'controller drift'):
                pilot.qualify(repo,'config.json',digest)
            shutil.copyfile(original/name,path)
            with self.assertRaisesRegex(AssertionError,'archive drift'):
                pilot.preflight('config.json',digest,archive,'0'*64,'a'*40,repo=repo)
            with tarfile.open(archive,'w:gz') as output:
                evil = tarfile.TarInfo('../escape')
                evil.size = 1
                output.addfile(evil,io.BytesIO(b'x'))
            with self.assertRaises(AssertionError):
                pilot.preflight('config.json',digest,archive,pilot.artifact(archive)['sha256'],'a'*40,repo=repo)

    def test_existing_cache_never_formatted(self):
        v = dict(volume_id='vol-0123456789abcdef0', encrypted=True,auto_delete=False,created=False)
        with tempfile.TemporaryDirectory() as tmp, patch.object(pilot,'MOUNT',Path(tmp)), \
                patch.object(Path,'exists',return_value=True), patch.object(pilot.os.path,'ismount',return_value=False), \
                patch.object(pilot.subprocess,'run',return_value=Mock(returncode=2,stdout='')) as run:
            with self.assertRaisesRegex(AssertionError,'never format'):
                pilot.mount_cache(v)
            self.assertEqual([call.args[0][0] for call in run.call_args_list], ['blkid'])

    def test_termination_wait_failure_forbids_collection(self):
        with tempfile.TemporaryDirectory() as tmp:
            ec2 = Mock()
            ec2.run_instances.return_value = dict(Instances=[dict(InstanceId='i-one')])
            ec2.get_waiter.return_value.wait.side_effect = RuntimeError('unconfirmed termination')
            with patch.object(pilot,'poll'), patch.object(pilot,'collect') as collect:
                with self.assertRaises(RuntimeError):
                    pilot.execute_launch(ec2,Mock(),config(),{},'a0001',Path(tmp),'prefix','',None)
                collect.assert_not_called()
            self.assertFalse((Path(tmp)/'aws-closeout.json').exists())

    def test_collection_authenticates_terminal_before_body(self):
        proof = {k:'a'*64 for k in pilot.TERMINAL_IDENTITIES}
        proof.update(source_commit='b'*40, source_archive_sha256='c'*64)
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            pilot.write(out/'aws-closeout.json',dict(state='terminated',nodes={'0':dict(instance_id='i-one')}))
            terminal = dict(proof,schema=pilot.SCHEMA,instance_id='i-other',artifacts={})
            s3 = Mock()
            s3.get_object.return_value = dict(Body=io.BytesIO(pilot.encoded(terminal)))
            with self.assertRaises(AssertionError):
                pilot.collect(s3,'prefix',out,'i-one',proof)
            s3.get_object.assert_called_once()
            terminal['instance_id'] = 'i-one'
            terminal['artifacts'] = {'test.log':dict(bytes=3,sha256=pilot.sha(b'abc'))}
            s3.get_object.side_effect = [dict(Body=io.BytesIO(pilot.encoded(terminal))),dict(Body=io.BytesIO(b'bad'))]
            with self.assertRaisesRegex(AssertionError,'artifact authentication'):
                pilot.collect(s3,'prefix',out,'i-one',proof)

    def test_receipt_rejects_zero_tests_source_drift_and_incomplete_resources(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            c = config()
            native = pilot.source_hashes(Path(pilot.__file__).resolve().parents[1])
            manifest = dict(source_sha256=native,source_file_count=399,source_identity_sha256=pilot.NATIVE_IDENTITY)
            pilot.write(out/'native-source-manifest.json',manifest)
            c['native_source_manifest'] = dict(path='manifest.json',**pilot.artifact(out/'native-source-manifest.json'))
            pilot.write(out/'config.json',c)
            proof = dict(config_sha256=pilot.artifact(out/'config.json')['sha256'],
                native_source_manifest=c['native_source_manifest'],source_sha256=native,source_file_count=399,
                source_identity_sha256=pilot.NATIVE_IDENTITY)
            pilot.write(out/'source-qualification.json',proof)
            for name in ('source-before.json','source-after.json'):
                pilot.write(out/name,native)
            identity = pilot.cache_identity(c,proof)
            cache = dict(identity=identity,namespace=str(pilot.MOUNT/pilot.sha(pilot.encoded(identity))),
                source_identity_sha256=pilot.NATIVE_IDENTITY,source_touched_count=399,source_touched_ns=c['code_freeze_ns']+2,
                binary_provenance_inferred=False,registry_populated=False,target_populated=False)
            pilot.write(out/'cache.json',cache)
            good = f'running 1 test\ntest {pilot.TEST} ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 42 filtered out; finished in 1s\n'
            (out/'test.log').write_text(good)
            (out/'rustc-version.txt').write_text('release: 1.98.0\nhost: x86_64-unknown-linux-gnu\n')
            (out/'cargo-version.txt').write_text('cargo 1.98.0 (test)\n')
            receipt = dict(schema=pilot.SCHEMA,qualified=True,command_started=True,command_completed=True,
                config_sha256=proof['config_sha256'],exit_status=0,command=pilot.COMMAND,tests=pilot.test_result(good),
                full_workspace_execution=False,binary_provenance_inferred=False,
                environment=pilot.environment(c,cache,Path('/mnt/native-cargo-pilot')),
                **{k:c['code_freeze_ns']+i for i,k in enumerate(('code_freeze_ns','start_ns','cache_prepared_ns','build_start_ns','build_end_ns','end_ns'))})
            resources = dict(before=counters(),after=counters(),closed=True)
            receipt['observed_resources'] = pilot.sample_summary([counters(),resources['after']])
            (out/'resource-samples.jsonl').write_bytes(pilot.encoded(counters())+b'\n')
            pilot.write(out/'pilot-receipt.json',receipt)
            pilot.write(out/'workspace-cgroup.json',resources)
            pilot.validate_receipt(out)
            # Complete authenticated collection uses the same receipt verifier.
            proof.update({k:'a'*64 for k in pilot.TERMINAL_IDENTITIES if k not in proof})
            proof['campaign_schema']=pilot.SCHEMA
            (out/'source-qualification.json').write_bytes(pilot.encoded(proof))
            volume=dict(volume_id='vol-0123456789abcdef0',created=True)
            (out/'volume.json').write_bytes(pilot.encoded(volume))
            phases=('boot','userdata-start','source-download','download-end','setup-end','cache-preparation',
                'cache-mount-end','execution','cache-close','complete','worker-terminal')
            (out/'timings.jsonl').write_bytes(b''.join(pilot.encoded(dict(phase=p,time_ns=i+1))+b'\n' for i,p in enumerate(phases)))
            for name in pilot.ARTIFACTS:
                if not (out/name).exists():
                    (out/name).write_bytes(b'retained evidence')
            frozen=dict(proof,source_commit='b'*40,source_archive_sha256='c'*64)
            terminal=dict(frozen,schema=pilot.SCHEMA,instance_id='i-one',status='complete',phase='complete',
                exit_code=0,original_exit_code=0,artifacts={n:pilot.artifact(out/n) for n in pilot.ARTIFACTS},
                source_qualification_sha256=pilot.artifact(out/'source-qualification.json')['sha256'])
            pilot.write(out/'aws-closeout.json',dict(state='terminated',nodes={'0':dict(instance_id='i-one')},cache_volume=volume))
            bodies={n:(out/n).read_bytes() for n in pilot.ARTIFACTS}
            def get(**kw):
                name=kw['Key'].removeprefix('prefix/')
                return dict(Body=io.BytesIO(pilot.encoded(terminal) if name=='terminal.json' else bodies[name.removeprefix('artifacts/')]))
            pilot.collect(Mock(get_object=get),'prefix',out,'i-one',frozen)
            self.assertTrue(json.loads((out/'collection-replay.json').read_bytes())['qualified'])
            for change in ('zero','source','resource','incomplete'):
                if change == 'zero':
                    (out/'test.log').write_text(good.replace('running 1 test','running 0 tests'))
                elif change == 'source':
                    (out/'source-after.json').write_text('{}')
                elif change == 'resource':
                    bad = copy.deepcopy(resources)
                    bad['after']['memory.swap.peak'] = '1'
                    (out/'workspace-cgroup.json').write_bytes(pilot.encoded(bad))
                else:
                    (out/'pilot-receipt.json').write_bytes(pilot.encoded(dict(receipt,command_completed=False)))
                with self.assertRaises(AssertionError):
                    pilot.validate_receipt(out)
                (out/'test.log').write_text(good)
                (out/'source-after.json').write_bytes(pilot.encoded(native))
                (out/'workspace-cgroup.json').write_bytes(pilot.encoded(resources))
                (out/'pilot-receipt.json').write_bytes(pilot.encoded(receipt))

    def test_worker_executes_only_one_command_with_isolated_environment(self):
        # Mock external effects; exercise the worker orchestration and durable receipt.
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)/'out'; out.mkdir()
            repo = Path(tmp)/'repo'; repo.mkdir()
            c = config()
            c['native_source_manifest'] = dict(path='manifest.json')
            (repo/'config.json').write_bytes(pilot.encoded(c))
            (repo/'manifest.json').write_text('{}')
            native = {'fake.rs':'a'*64}
            proof = dict(source_sha256=native,source_identity_sha256=pilot.NATIVE_IDENTITY)
            volume = dict(ownedVolume=c['cache']['owned_volume'],size_gib=c['cache']['size_gib'])
            pilot.write(out/'volume.json',volume)
            state = dict(namespace=str(out/'cache'))
            versions = [b'release: 1.98.0\nhost: x86_64-unknown-linux-gnu\n',b'cargo 1.98.0 (test)\n',b'mock cpu']
            good = f'running 1 test\ntest {pilot.TEST} ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 42 filtered out; finished in 1s\n'
            def run(argv, **kwargs):
                self.assertEqual(argv[8:],pilot.COMMAND)
                self.assertEqual(kwargs['env']['CARGO_BUILD_JOBS'],'1')
                self.assertEqual(kwargs['env']['RUSTUP_TOOLCHAIN'],'1.98.0')
                self.assertNotIn('AWS_SECRET_ACCESS_KEY',kwargs['env'])
                kwargs['stdout'].write(good.encode())
                return Mock(returncode=0,pid=99999999)
            with patch.object(pilot,'qualify',return_value=(c,proof)), \
                 patch.object(pilot,'source_hashes',return_value=native), \
                 patch.object(pilot,'cache_lock',return_value=__import__('contextlib').nullcontext()), \
                 patch.object(pilot,'cache_state',return_value=state), \
                 patch.object(pilot,'capture_resources',side_effect=lambda:counters()), \
                 patch.object(pilot.subprocess,'check_output',side_effect=versions), \
                 patch.object(pilot.subprocess,'Popen',side_effect=run) as command, \
                 patch.object(pilot.os,'killpg') as kill:
                status = pilot.execute_worker(repo,out,'config.json','a'*64)
            self.assertEqual(status,0,(out/'pilot-receipt.json').read_text())
            command.assert_called_once()
            kill.assert_called_once()
            self.assertEqual(json.loads((out/'pilot-receipt.json').read_text())['tests']['passed'],1)

    def test_cache_namespace_reuses_dependencies_across_source_changes(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(pilot,'MOUNT',Path(tmp)/'mount'):
            pilot.MOUNT.mkdir()
            repo = Path(tmp)/'repo'; repo.mkdir()
            (repo/'Cargo.lock').write_text('locked')
            (repo/'lib.rs').write_text('old snapshot')
            proof = dict(source_sha256={'Cargo.lock':pilot.sha(b'locked'),'lib.rs':pilot.sha(b'old snapshot')},
                source_identity_sha256='a'*64)
            out1=Path(tmp)/'out1'; out1.mkdir()
            out2=Path(tmp)/'out2'; out2.mkdir()
            import os
            os.utime(repo/'lib.rs',ns=(1,1))
            first=pilot.cache_state(config(),proof,out1,repo)
            self.assertGreater((repo/'lib.rs').stat().st_mtime_ns,1)
            (Path(first['namespace'])/'target/cached-unit').write_text('not provenance')
            (repo/'lib.rs').write_text('new snapshot')
            os.utime(repo/'lib.rs',ns=(1,1))
            proof['source_sha256']['lib.rs']=pilot.sha(b'new snapshot')
            proof['source_identity_sha256']='b'*64
            second=pilot.cache_state(config(),proof,out2,repo)
            self.assertEqual(first['namespace'],second['namespace'])
            self.assertTrue(second['target_populated'])
            self.assertGreater((repo/'lib.rs').stat().st_mtime_ns,1)
            self.assertEqual((repo/'lib.rs').read_bytes(),b'new snapshot')
            self.assertNotEqual(first['source_identity_sha256'],second['source_identity_sha256'])

    def test_sample_summary_preserves_observed_psi_and_kernel_peaks(self):
        a,b=counters(),counters()
        b['host_memory_pressure']='some avg10=3.50 avg60=0 total=100\nfull avg10=2.00 avg60=0 total=50'
        b['memory.pressure']='some avg10=1.00 avg60=0 total=99\nfull avg10=0.25 avg60=0 total=30'
        b['memory.peak']='500'
        summary=pilot.sample_summary([a,b])
        self.assertEqual(summary['host_full_avg10_max'],2.)
        self.assertEqual(summary['cgroup_full_avg10_max'],.25)
        self.assertEqual(summary['kernel_memory_peak_bytes'],500)
        self.assertEqual(summary['sample_count'],2)

    def test_cache_attach_and_detach_preserve_volume(self):
        with tempfile.TemporaryDirectory() as tmp:
            c=config()
            volume=dict(volume_id='vol-0123456789abcdef0')
            ec2=Mock()
            ec2.run_instances.return_value=dict(Instances=[dict(InstanceId='i-one')])
            ec2.describe_volumes.return_value=dict(Volumes=[dict(VolumeId=volume['volume_id'],State='available',
                Encrypted=True,VolumeType='gp3',Size=c['cache']['size_gib'],AvailabilityZone=c['availability_zone'],
                Attachments=[],Tags=pilot.volume_tags(c))])
            with patch.object(pilot,'poll'), patch.object(pilot,'collect'):
                pilot.execute_launch(ec2,Mock(),c,{},'a0001',Path(tmp),'prefix','',volume)
            ec2.attach_volume.assert_called_once_with(VolumeId=volume['volume_id'],InstanceId='i-one',Device='/dev/sdf')
            ec2.modify_instance_attribute.assert_called_once_with(InstanceId='i-one',BlockDeviceMappings=[
                dict(DeviceName='/dev/sdf',Ebs={'DeleteOnTermination':False})])
            ec2.delete_volume.assert_not_called()
            self.assertEqual([call.args[0] for call in ec2.get_waiter.call_args_list],
                ['instance_running','volume_in_use','instance_terminated','volume_available'])

    def test_resource_failures(self):
        good = dict(before=counters(), after=counters(), closed=True)
        pilot.validate_resources(good)
        for key, value in (('memory.swap.peak','1'), ('memory.events','oom 1'),
                           ('cpu.max','400000 100000'), ('pids.max','1024'), ('process_ids',[1,2])):
            bad = copy.deepcopy(good)
            bad['after'][key] = value
            with self.assertRaises(AssertionError):
                pilot.validate_resources(bad)


def main():
    result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(PilotChecks))
    return 0 if result.wasSuccessful() else 1


if __name__ == '__main__':
    sys.exit(main())

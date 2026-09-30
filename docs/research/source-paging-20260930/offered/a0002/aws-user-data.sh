#!/bin/bash
set -euo pipefail
systemd-run --unit=native-paged-cold-offered-stop --on-active=2700s /usr/sbin/shutdown -h now
root=/mnt/native-paged-cold-offered
mkdir -p "$root" && cd "$root"
cat >artifact-roster.json <<'ROSTER'
["source-qualification.json","boundary-check.json","compiled-source.json","binaries/two_bit_http","cpu.txt","test.log","run-closed.log","profile.log","profile-resources.txt","profile-cgroup.json","screen/summary.json","screen/rate0-relaion-records.jsonl","screen/rate0-cohere-records.jsonl","screen/rate1-relaion-records.jsonl","screen/rate1-cohere-records.jsonl","screen/rate2-relaion-records.jsonl","screen/rate2-cohere-records.jsonl","screen/rate3-relaion-records.jsonl","screen/rate3-cohere-records.jsonl","screen/rate4-relaion-records.jsonl","screen/rate4-cohere-records.jsonl","screen/rate5-relaion-records.jsonl","screen/rate5-cohere-records.jsonl","frozen/binaries/two_bit_http","frozen/boundary-check.json","frozen/compiled-source.json","frozen/source-qualification.json","frozen/run-closed.log","frozen/rustc-version.txt","frozen/cargo-version.txt","frozen/cpuinfo.txt","frozen/arm-feature-tree.txt","frozen/x86-feature-tree.txt","frozen/compiled-source/crates/borsuk/examples/two_bit_http.rs","frozen/compiled-source/crates/borsuk/src/object_native_generation.rs","frozen/compiled-source/crates/borsuk/src/two_bit_generation.rs","frozen/compiled-source/crates/borsuk/tests/two_bit_generation.rs","frozen/compiled-source/Cargo.toml","frozen/compiled-source/Cargo.lock","frozen/compiled-source/crates/borsuk/Cargo.toml","frozen/compiled-source/crates/borsuk/tests/two_bit_source.rs","frozen/compiled-source/crates/borsuk/src/sq8_s3_range.rs","frozen/compiled-source/crates/borsuk/src/sq8_page_authority.rs","frozen/compiled-source/crates/borsuk/src/two_bit_source.rs","frozen/compiled-source/crates/borsuk/src/two_bit_build.rs","frozen/compiled-source/crates/borsuk/src/two_bit_index.rs","frozen/compiled-source/crates/borsuk/src/unit_centroid_graph.rs","frozen/compiled-source/crates/borsuk/src/bin/build_two_bit_graph_variant.rs","frozen/compiled-source/crates/borsuk/src/bin/two_bit_plan_demo.rs","frozen/compiled-source/crates/borsuk/src/bin/two_bit_union_nomination.rs","frozen/compiled-source/crates/borsuk/src/bin/two_bit_walk_nomination.rs","frozen/compiled-source/crates/borsuk/tests/two_bit_application_ids.rs","frozen/compiled-source/crates/borsuk/tests/two_bit_gc_delayed_delete.rs"]
ROSTER
phase=bootstrap
finish() {
  code=$?
  trap - EXIT TERM
  set +e
  cd "$root"
  cp run.log run-closed.log || code=96
  token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
  instance_id=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id)
  for name in $(python3 -c 'import json; print(" ".join(json.load(open("artifact-roster.json"))))'); do
    if [ -f "$name" ]; then
      aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/source-paging/20260930/offered-a0002/artifacts/$name" --only-show-errors || code=96
    fi
  done
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" PHASE="$phase" python3 - <<'PY' >terminal.json
import hashlib,json,os
from pathlib import Path
artifacts={}
for name in json.loads(Path("artifact-roster.json").read_text()):
    path=Path(name)
    if path.is_file():
        digest=hashlib.sha256()
        with path.open('rb') as source:
            for chunk in iter(lambda: source.read(1024*1024),b''):
                digest.update(chunk)
        artifacts[name]={'bytes':path.stat().st_size,'sha256':digest.hexdigest()}
code=int(os.environ['EXIT_CODE'])
print(json.dumps({'schema':'borsuk-native-paged-cold-offered-spot-v1','source_commit':'fb4ec1f39cdc12195bd6cacb16bc9a1caa1f29e5',
  'source_archive_sha256':'f0de9d4461369cfc5ac591947b5bff375871a44a75a63a62718156ed52ecb3ae',
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/source-paging/20260930/offered-a0002/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/f0de9d4461369cfc5ac591947b5bff375871a44a75a63a62718156ed52ecb3ae.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' 'f0de9d4461369cfc5ac591947b5bff375871a44a75a63a62718156ed52ecb3ae' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
dnf install -y -q tar gzip time python3.12
python3.12 -m ensurepip
python3.12 -m pip install -q boto3
python3.12 -c 'import base64,gzip; from pathlib import Path; Path("source-qualification.json").write_bytes(gzip.decompress(base64.b64decode("H4sIAAAAAAAC/62a2VYjRxKG34VrY3JffDsPopMryC0kXCq1u8fH7z5fSgKKBprmaG7cZqs/MiPiXyT9c5Wmed1TmVfTbj+3abW/S8q6qz+uXNPaCu1Kaz57l4K1RVUjskm1G59kDlHVEGyW0dVcUpRZSdmrDNG7pq5+u8qH9aau2td1bdvSVuv96m69n3fTuqTN1R/zdGi/XZV0/5DWt9vVvty1+wRw3k37w5frbZrXX9v1Q7pt9brsNvV613ub+GL/sJuvv0oAyq62p4r/udqXaf0w7294UvmyOj1gdd/mVNOcVlPa3rb9aj+nef/7w3eQdJG+aiVU9dV5ZfivFcqo0KVPocUgjTTdBuWkED5V10v30WjnU0uhayp4E/JY82q/O0yceoFnpZIx+tidUsWbZq2SyYXcu40xqCxrFrrJkL2PWghtsm5JyJ5y6k5K9x4eENN8eFgd7/sMlXJvFMrDbTTWBC+V90qHnJKsTUQhOVQSJsum+VnJWcRsnS4mtG7VR1DLU7lsYojdZh7bba+9NaNMcKpopb3iC+9U8klkn4OKornOyWxhvkxScQG1SYdtuXuvc2MIVqP5J9xaUi/ai+pVz6p254xOSpuUjZem1sZMJhNM6VTYU4glKKODSion7bJ+F/fUviPaeeQWqL1wWFc6QxJLlD07uqd7iqVr4X1VOkXjudPmtWwlFem0zVn744Ap9z5qY/vk/QJKGSFiMJ2VKokB1H1MT6g1ZmcsnTLZqVZMUyxmSb7InHMvtvYYQhblXajHJj5Mu77etAWkGTfTevTV2BaSVK0DH4KTKeTmm7G2W6l7qybaXnJrOYQUZPQ2c+76GvKrtB6g9X2avq96S/t1Xm/W8/cFaAhKy6iCGoMR3bgxU1OXLqquQwkumqJbrl1FGbzifFkX0YpNvckY8lug3qymtknrusrr7Wjm/cPLk+YaGpNaqwuWffbSjltTpjTTxhzFZGupzTqTuQpGykrP0Sk1cPM6LUCntp9BaN8gtyPRbXbpvIbCUCiX5UWs1kUrYqqB3VPems6GxuikF1m149F1CD4KbT3rnrzodXmf02H72L/jaPb1BOxfhzZ9P2HF0iqcEZVh46EXo7NtNhlta1Kqha6UMNLKnFwrxXOtvQt2PQfPsBj1E6zzGpxwkhJeOvaYyauB2WMogMoldK9kaSrTOOtE4+Gm09WSY5UtRYY5MiVv47y17Cc8qZUajc5e2ALxZi4vl+ZiSzBPil1RhikZ7qZlXKpAiKyGN2v2qNTbeIslf3WTTSdTJHPooZEkioqF+tl4IYOQxqqcbY29uZyCRwlEdB2mQUicbj7aDxFf3Ke0CI+ugUHpsgWtHG3jsMo4GhNyhky5vSCdUzZ5BEkEI0Wr2moDB76DdqaTv3fTlzadF60EWuZj7EUk1YRg/iSM0UMqQUp0sNUWCqMvbCqWMgozU0UsqUZd9U+QHonybp4fTmCexc0pByYtecsoqKxEt2MhokyMJnTYREYCcRMokutMh4dOazFIU34b7LBd77YLFKipc6jkdERblHOwEQpGqwJSjk4XqYIMTUtXRdCRpXPCB2OwKT0E8TOU12cKSRWkWlfIqujuPINH+2g9i6sZb2sFfFd1Kh7ulknyF66zMAb6qO7q3+FZjlRUn8j4yb78J023u983u/JlTIUPxXY9Gm4a/C5l46aEEQ1F9Qb5hoyFalYrzBcrAE03lazROmv8Ggc7PW/e3W/G8xp9LZbplZZBSqLrhPBnDIhFQTOLajFBXclQx9EKI287pkDZMZ/jeWVKc9vfnPzZzYune7oQsuoCrRC24zR8lYl5jsahWaa34DlLgl9Frr5jhLxNmj/CXBVjXz29fcMXbvh6/nsHhc+nJkz7gVVVjT5zBOtj7nijXhHJwNJo2xEnD+Ng6zQ6aTrkCh/C8VHC6bUn/wprP5UbVOLmZFcfAW+n9ICSpGmdtvMJWSkfumWOpCwwnVZWNTpfgvd0t2SNpYsW4ga4FtQjN/6JTjsBT/0E+RHzYZO2q9rud2c8TEsPosJzdEnAowifHqLEF4bzYdwrNyCSK7awz4VpVKpqeANiNx/ineZ8u7tfj2Hcbc8XHFu1xlJ/8zLoFhVXx76WbARGZ0xNlSxRDnwBO0KzFFBzxsriX+OHsH+nzZdXqDkdpdVp3LfCGHKj3DTiS+YgiODzuA1fYSyIUajhNNnGym8YLKZ++3J3+c9Gtjmv2m3btmkBqV1FtNipjlnNGnuB2o9z2oBOKvQGkrDF2TFmWWMAvB8UHYYL8rG8Cbn/KxypfpUO8x1ZZ/5+Amt4XgOz4QGgxJKsDUg+e9ilBK0WVtNYGo5LTDZKGSWqym7D/akr/S7YXp808wQThFAqmxycw2dYpr/C9tYiZIg2k9qHWlUlUEgMHOQoFRRi6LMRkOWbMI+NOwWMI05vlqSCK82j/8CIlH1EAoGWueEcEWOm1ZZBLTIKjslycGqP5ZH9pzivGoW1sV7XBLkVgY9P0WJ9AfRMTKIQNZy45DcQHOgvZpKN7Z0IBfebn4LhD9u3E46o1Bds1Mbj90Ilt4hEmi0SusIuJZSSgQ+Oc5SG4iCawsRSLP7UGfFTnFMePAHxd9IFjc8wZqwYWpR0yVILXa0ypvsMiSk33D98DeMR0AL7b7FU6M+bQGwzPrRt52mH7z3y13mhk/HOmojgy6ElWF3WzLRh1fAvpjjjkZCMvxJkwmaqjcPLOIYvn5zhSzT+d36m5vTwsCHSj3at1nV/PmHIpQvLZkLy3BX7Qg5kukNCjPF0NguykkCxg/UjebuU0pAzYpTV8gPM2wJPbtJ3dJR/2/x4r9FzGM5I4sPlNk5DOlEMgavE3tHI6Bs/xcfDG6ZqiqgWFolKa/Mh6g9zGSRCXxVLpAgpxmbHnRbmpiOpiGmCRQ1TiTyyGKkZgm+qNWPyB6ulD+CWE0M+1/Ce7HrsLI8kueDouyI2FCcEGZNsVKATHYvgWygegCKMOMzunazHtq9vYaf5buTnXTlml5amcndzwhovt6y3t9fDygrI/ebxNZfTn/7+5363vXp60NNrRUbByVY7r50r+PwsHDRZlZSJiQ6uSZGqIocPW6Rx8zokVhj3CjPpY2wsh2lidFf9sNms9of1PAzznhxAiLu/+qOnzb79dtWn3X/bdmS6EScfX7vaD/eUpvtr8uV8mNr1PLX2+/xtHt/P37nUqz/YgKgxe48lC+0w8CYrRkIXUiXS5vAHJKUofdHOIAaWSVI5GnrmSyskRoi5BU6Tx30ey1j/YFIWmEPHyPBhARsjbptu2YBkslyeZ8FaZAsCYOrJeqMdUaCXgFs9vWRiyPtQMxGktCPs7rCtnP/6+MrMqSfPoNhQuQAMnqXH8qJk5fgqjcmOfG0q5IVfxOBhxBPORY+ShLPcQfUCm6aYaKGPczOs3vXXNu3H5L+8V+0WYATzTkZpvTocBMxmFFYZsUwxcxn8VGDIRMYRM8IxkXUQpEgMKa4EDMvCH1+fp/+H4ylM2BKRLgW4DJ+dzdA1FZAfzoRoK6klkVd5U8geFZ7O+CO8Siy4f9sEF/IG4s3Shz/hRmGNWgBf7M/fBT5Z6idgp5btvNjGvwH7vq9/vnUXl7d+sd3/sIr3/f9zR4J+UdOlseDDmn41JzxVOERmUeHF8eGXK3w7TzzzkmEtloVdmjM+XdibwWOxa2rJmRfnkU+X91ZAeb49CcUuyrs4uPxSeT9NMk+1seNhqXIXJ5xfqu2dyLOYN+eX83ZxFPrlql5ko+dbUuqFF7g4M/1SPa9D1OKCXor2xeHqUwW9O0xMi10O06Up7FNVPceyZ17AUywt3KVp7VP1LMz4syzCOssbujjW/VJF7+W8hQU08oU0Xpr/Pizr40D47Gei8i8czaVB8ZPFvZ0cF+wuYlyWd2mi/Gx57yyj1tov67o4en6yrrfGX4pgluP/f8moD4f1tu9+SBmEiGXOiCyPhK5xt85hcqOOTkacHsKWvbUZZxW8SVYF43TscCImogzFC864455Nh+112ez2rWL1b5enCvGFOpQuK8xiMjEJa1/teBuGTCgJrtw5Ou9ty5J56EaiItiDON4QaMEGLaw7ge3n8k6Cki+sdSvaBxldw3SRdWBSQUtHdokVimX4I24nCBfzyGf8qB1tGVMavSzHXHrO8n8d0mbdz4v4Y46Siv1Zkin2RHJ3GNHYEuFIQpc8fzApITiyjuQdAk0twuDhvRdKVIPpGd9QR0v/LbgPIvgLWoIAWSE7uJjnc+kdMyl8FKFHVJ5K2CMuGIVNUlY17AFfdklcLzbB3/8+vSJw9kUvjjygP/Nax3hr7iYJNOKGTv1wc4u6XfdFkpBlwJgTrlRKglVIuIcCI9kkQ3AxNNKvSNgq9tTrBq2MlxRqHa9Gnj8bMqpZvgd0pVuSPbKjJClPXsOPQeVW98Kcj5ArC7aIdhBfYeeYjHMis3DGp/ERnOdHs9v36/n4KQJtFIl4JNWj/OEoRh8RzoQzhMgyo6bHi3xzm4bl3TzXU1qotWvhUCdFY0qjjuxidwxjamxFxGIGuFsVtlqhKZoAzF50UoI4TsV92q77eIv8M704f/rn/NPHRzy24umRz2vDvAQskqcJCT3xOCP8QXPsYYzMVRIRH9WVb7hu72ozBfWmKXIk+HJ86Fzuju/QDmXdrDZpvJv/fXXf0p6Jrk8vQj3+4te2rbvp9c/Pszi14fTmH7/9qj9ZBOU1udnZRuYy0hAoGk5Bj8/wdHQEksuL1h4/s1F2h+08pN4+fX98zGo+frTiaZwQ1lxxrgVO6nl4jJRt8RrE4jMOl/XVsjOeNWIFXCpKWyxUGqPncGz/A3b4o0ohJgAA")))'
phase=binary-qualification
lscpu >cpu.txt
PYTHONPATH="$root/repo" python3.12 -m scripts.launch_native_paged_cold_offered_spot --extract-frozen "$root/repo" "$root" >test.log 2>&1
phase=profile
systemd-run --unit=native-paged-cold-offered --wait --pipe -p MemoryMax=7G -p MemorySwapMax=0 -p RuntimeMaxSec=2430 -p WorkingDirectory="$root/repo" \
 --setenv=PYTHONPATH="$root/repo" --setenv=TOKIO_WORKER_THREADS=4 --setenv=AWS_MAX_ATTEMPTS=1 --setenv=BORSUK_NATIVE_MEMORY_BYTES=1073741824 \
 /usr/bin/time -v -o "$root/profile-resources.txt" timeout --signal=TERM --kill-after=30 2400 \
 bash -c 'ulimit -v 4194304 || exit 96; taskset -c 4-5 python3.12 -m scripts.run_native_paged_cold_offered docs/research/source-paging-20260930/offered-config.json 422465367366c909b061e8d211a30386e10ad2f56c70032fe38aa954ca05534d "$1/binaries/two_bit_http" "$1/boundary-check.json" "$1/screen"; code=$?; taskset -c 0-3 python3.12 scripts/check_native_startup_build.py --cgroup "$1/profile-cgroup.json" 7516192768 || exit 96; exit "$code"' _ "$root" >profile.log 2>&1
test -s "$root/screen/summary.json"
test -s "$root/screen/rate0-relaion-records.jsonl"
test -s "$root/screen/rate0-cohere-records.jsonl"
test -s "$root/screen/rate1-relaion-records.jsonl"
test -s "$root/screen/rate1-cohere-records.jsonl"
test -s "$root/screen/rate2-relaion-records.jsonl"
test -s "$root/screen/rate2-cohere-records.jsonl"
test -s "$root/screen/rate3-relaion-records.jsonl"
test -s "$root/screen/rate3-cohere-records.jsonl"
test -s "$root/screen/rate4-relaion-records.jsonl"
test -s "$root/screen/rate4-cohere-records.jsonl"
test -s "$root/screen/rate5-relaion-records.jsonl"
test -s "$root/screen/rate5-cohere-records.jsonl"
phase=complete

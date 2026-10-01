#!/bin/bash
set -euo pipefail
systemd-run --unit=native-semantic-router-cold-stop --on-active=3600s /usr/sbin/shutdown -h now
root=/mnt/native-semantic-router-cold
mkdir -p "$root" && cd "$root"
phase=bootstrap
finish() {
  code=$?
  trap - EXIT TERM
  set +e
  cd "$root"
  cp run.log run-closed.log || code=96
  token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
  instance_id=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id)
  for name in source-qualification.json binaries/two_bit_http binaries/two_bit_plan_demo cpu.txt run-closed.log profile.log profile-resources.txt profile-cgroup.json screen/records.jsonl screen/summary.json screen/config.json screen/qualification.json native-assurance.json boundary-check.json native-source-manifest.json native-source.tar.gz qualified-source.tar.gz publisher-proof.json asset-manifest.json assurance/affected-final.json assurance/affected-final.log assurance/release-final.json assurance/release-final.log assurance/clippy-final.json assurance/clippy-final.log assurance/test-build-final.json assurance/test-build-final.log assurance/full-workspace-final.json assurance/full-workspace-final.log publication/config.json publication/asset-manifest.json publication/publication-receipt.json publication/ReLAION/control/native.jsonl publication/ReLAION/control/stdout.log publication/ReLAION/control/stderr.log publication/ReLAION/control/resources.txt publication/ReLAION/control/head.json publication/ReLAION/candidate/native.jsonl publication/ReLAION/candidate/stdout.log publication/ReLAION/candidate/stderr.log publication/ReLAION/candidate/resources.txt publication/ReLAION/candidate/head.json publication/CoHere/control/native.jsonl publication/CoHere/control/stdout.log publication/CoHere/control/stderr.log publication/CoHere/control/resources.txt publication/CoHere/control/head.json publication/CoHere/candidate/native.jsonl publication/CoHere/candidate/stdout.log publication/CoHere/candidate/stderr.log publication/CoHere/candidate/resources.txt publication/CoHere/candidate/head.json; do
    if [ -f "$name" ]; then
      aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261001/cold-a0001/artifacts/$name" --only-show-errors || code=96
    fi
  done
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" PHASE="$phase" python3 - <<'PY' >terminal.json
import hashlib,json,os
from pathlib import Path
artifacts={}
for name in ('source-qualification.json', 'binaries/two_bit_http', 'binaries/two_bit_plan_demo', 'cpu.txt', 'run-closed.log', 'profile.log', 'profile-resources.txt', 'profile-cgroup.json', 'screen/records.jsonl', 'screen/summary.json', 'screen/config.json', 'screen/qualification.json', 'native-assurance.json', 'boundary-check.json', 'native-source-manifest.json', 'native-source.tar.gz', 'qualified-source.tar.gz', 'publisher-proof.json', 'asset-manifest.json', 'assurance/affected-final.json', 'assurance/affected-final.log', 'assurance/release-final.json', 'assurance/release-final.log', 'assurance/clippy-final.json', 'assurance/clippy-final.log', 'assurance/test-build-final.json', 'assurance/test-build-final.log', 'assurance/full-workspace-final.json', 'assurance/full-workspace-final.log', 'publication/config.json', 'publication/asset-manifest.json', 'publication/publication-receipt.json', 'publication/ReLAION/control/native.jsonl', 'publication/ReLAION/control/stdout.log', 'publication/ReLAION/control/stderr.log', 'publication/ReLAION/control/resources.txt', 'publication/ReLAION/control/head.json', 'publication/ReLAION/candidate/native.jsonl', 'publication/ReLAION/candidate/stdout.log', 'publication/ReLAION/candidate/stderr.log', 'publication/ReLAION/candidate/resources.txt', 'publication/ReLAION/candidate/head.json', 'publication/CoHere/control/native.jsonl', 'publication/CoHere/control/stdout.log', 'publication/CoHere/control/stderr.log', 'publication/CoHere/control/resources.txt', 'publication/CoHere/control/head.json', 'publication/CoHere/candidate/native.jsonl', 'publication/CoHere/candidate/stdout.log', 'publication/CoHere/candidate/stderr.log', 'publication/CoHere/candidate/resources.txt', 'publication/CoHere/candidate/head.json'):
    path=Path(name)
    if path.is_file():
        digest=hashlib.sha256()
        with path.open('rb') as source:
            for chunk in iter(lambda: source.read(1024*1024),b''):
                digest.update(chunk)
        artifacts[name]={'bytes':path.stat().st_size,'sha256':digest.hexdigest()}
code=int(os.environ['EXIT_CODE'])
print(json.dumps({'schema':'borsuk-native-semantic-router-cold-spot-v1','source_commit':'64fd30e7d7ab240e9320083c0b67bc1904cc717a',
  'source_archive_sha256':'2be209e9392c8c67ccff34a1bfc188abcfed6f73ec7e4f01b693d9d12f20ae48','config_sha256': '72cd42fde1b52f08f90bd0d6d55e17fdbc5a208b28656911a74f2ef37e2686fb', 'qualification_sha256': '335b9f0776a50c0a92afd37f4e6cff8a6fb402e8068d835073e1526bf300e107', 'binary_sha256': 'c00b766f65f8f0ae0adb5fcca786cb33c0daf046ff4b8f9a8bbcab39e1263533', 'binary_bytes': 16191384, 'native_source_commit': 'eeaafae3cd5d374b982b22914e1634dfce117dd7', 'source_identity_sha256': '92085e6e40ac9324ea7a4fc8daab58995dc84680a5c2391eb426dd430230e520', 'source_file_count': 399, 'artifact_roster_sha256': 'aa12fa43aeba096e9a58e7ba12c57c82067774c9788acde6112d3c7a328a29a1', 'native_source_archive_sha256': '033d196e982bca44c4adfe39b00a9353698f7e2d93494fbe94130e236430287a', 'native_source_manifest_sha256': '21b769e5d54a85251f128aee6f1653673457227724d33c2801cde93795de4ccf', 'native_assurance_sha256': '98b1bf6f091842470391635af1307380c8e4870176b0b8fa38006985303b4e35', 'publisher_sha256': '6647839e08bcd5d50beb7badc313274028e0e5a36d2c354454fcfc0e325d1813', 'publisher_bytes': 15578880, 'publisher_qualification_sha256': 'd6cc84a5f87d087d2dc4c2826effa3d9d9fbecf77cabd054e4d9c57215fbcdcf', 'asset_manifest_sha256': 'e87e21b333d360005f325634588e351196503f2fb10290b54d7f0768a73be338', 'publication_assets': {'key': 'research/semantic-router/20261001/publication-assets/1d30e5a5dbfaa8c7ce054a121b55ebda8de751ba37bd27274ec33bb7cd5ff973.tar.gz', 'bytes': 105632412, 'sha256': '1d30e5a5dbfaa8c7ce054a121b55ebda8de751ba37bd27274ec33bb7cd5ff973'}, 'asset_preparation_sha256': 'e13e18118b5313f4e8bf4427aab72eb00e7d1e16ae18113e91ce7fc5a0ecf6c1',
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261001/cold-a0001/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/2be209e9392c8c67ccff34a1bfc188abcfed6f73ec7e4f01b693d9d12f20ae48.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' '2be209e9392c8c67ccff34a1bfc188abcfed6f73ec7e4f01b693d9d12f20ae48' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
dnf install -y -q tar gzip time util-linux python3.12
python3.12 -m ensurepip
python3.12 -m pip install -q boto3
python3.12 -c 'import base64,gzip; from pathlib import Path; Path("source-qualification.json").write_bytes(gzip.decompress(base64.b64decode("H4sIAAAAAAAC/61Y7XJbtw58F/+OYn4TzMtoQBC0dSNL6tFR2txM3/0uJVs+duy0d5zpdKYjuVwSWOwu9OOGp3nTWeb1tD/OOq2P9+xiuvlyw2xd5+BZK5uStHAkzRWfSsxCzqScc5CSiViaJmtd85LZO2JX2N58uuHjUef1A+82XY/z89FKWZ2t3vvmkzEmdo8vfIhE6qO1JUXju+vVGldMjaHlbnIizr6q93Q9+jDpgSeeN/vd4nTr1ZK1VKO3vgel2kNwmblmp9UYzc2qTXz+K6/FiuYukY1KT3K++Gm+30+b+fv6qTzHmy8/LqCrp/d8/s9xvxsf1++z4vtsk/t08/se+ff5laeJd6K33LvKrG3VNzvevobGOxbIBh1hLaap+uBCbt3aVqM1zRmvwRUNFCuPa6FWplrJxqRUk7chG0q/RN7u7xbAycQQFtC9eAVhUvdV1Cqh+ng7jrbZ1OK9RdFDFKoBDMnkQmK0yPqcuzXJvnq0bDeHw/d3nuzyArf4bC2aXGoXouBIjbfdo5JZQNPSlcyZrCi8xORyzw5saM754kPmX+C+fLDLZP2yzdVzMd64YDo37lI7a3ROnISipTHIZchghkLOFDubXjAdsWfrJWrTl8j9tN2u/txPX48HFn375YFsLIsbWGOTcCrZp9C5ogW19ZRarBRCVs4mRq8mauDEg3nsqOJTtbaCiv5f3OBlDXzw5GhxgxaMOLYJlM45thATpAM1T/gPT9SaI8qd0OQeoRuNk0eZnAuKEYkUX95g0q3y8Z3Hk/fLGROMM6Yn2EgpW+fwQhDPqRRnY0AzHEsA9X1ypZA33SQjzRbcVrK2XwG/InpItJyxyORdis7jny65UhUbawHTtUqtQGsafErel5ZADOM4W4oBfcHA1/4SGRDzqp4223fmG2RZYKcYNAqH1sRVaJiwbYMCvUpSyK8k8K94DCBUGn8rkaTF4nuqYyrLP2C/fDgl4+ICnJ2xhDf1rKEkBu+NVQhu8x7jpChGiKVg1qo4bxg8752goJCEUnMzA7zuT7vG0/eV3Kt8ff1YqFBZzrb3KCykMSeORgyjeb35PKRFcDanXoNxGPBEjXw0GbeJLtXuIfXW5IG4g0d809X11a8xrceoLPWEqq09dVMsDQk1vtjkI0OqcD4ZIWhoNjanaiqeh49MKhS98TVA3heYx/1pwiC95xohRL+sLywjp6KxxcAUXbTdwlBVU7cp+pRhINm5nF1AxcWRsTBfyF+JIJxI/wn588zT57v/LgvsMKpmOcAGJmWHx5OrIFaQwK0rGmZQbg/YQui3a9DKEnrVElAHdRAcKB9d5PNwqtvN8V6n1WHa7/sbXU1L1WpJhAKEkEAK/OuaBDzHJe0oZyutAEd6zsK1DRaHVhA8MNagebu8848Tbzd9A296/6mxpJwWuGBlhEBLDaS1RFfQUhsY6cDG1sVAFSq5ghyAVOBaUtsD+epRcO5gws3fg8AYlOn7+ok8yRboXLh+/gxmDLqZesIzu2E1DB/uIpwpCaKBGFiGCan3ABYVJgwoV1/UOrAN0vzpRvjhwJs75BsMywMPx9lPx9PX1VOT8eFu3shq2p+Q31ayxyAfD/t59W0kGdk3vd7nx81Rps1hPt6eB299OWL9dMT6csT6OPN8/Hz4PrxFcUNYNNIJKEBSbfCeyWDEPaSYw2BNQxsZ7cpIUqiU8zYixDA8qeMGb0OiU/PpsD7rzgUqwroUBEdaiyVEjJcdNPfwKuibmgKbs4ZNqNAbfAeVNaUiUQk6CV/5J6jFq2JC/qDSY8WxPXZwHbEoUHKC2yMkIi0mCDaje4MMBtMXqo8CKQoItwuoLZ92cv+E9aAztH7mNUTmTo/r0Yz1aMYFtwXLVVlgA6BayJDFYBA/1EqBWQYEGB2KRYgRIDtEu5octZTIjPRg38U9KNpmHxZQQ5cLhd4b7MFAs7qFKsOKS01QHAmhJthkUCSVBj6KrRXZCSNQiKqRd6Fek+XVE7Mj35DDcmloI2baIP/AgqApJNSjRzWd4awREwZhkxLFQtW4snokhPdxH9sIcembrS4gA/tUtZeM4KGE/QQK0htRspgnJIwQY0cC6OBpwfBV1YqATBZhsHIJ7WfIbzZmAG0exjB3ZIJN3WzHNvAMSgSaF+jVoMZQCyR7zDKE1XWPl6YSxCuCmIODZIe6Vkw7rJg7HJjqW6A5rBFBeNPW0JGGyj4cXr60NlJwtbVEMWUEiji65YIo5NGPrSs2uEFMoaIUNSDtZjwdVyV03PMCFOHuK98t2vlY5jvd6WWhukBKheuiiWSNes7Rkx2hGRKF+NyNQRLEGGbHWP5gUpRBI9vQi8i2Yo6XkOdlTX8i0tk4ZIGJo2Dy2fTIjUIugCwN46pMyg2p2qGPOcB+SgdK6XCG7kfSB5vR7wXmNPZO2etfGyy38P31ds+PemMC+gFOZFNaTCWaAjSIjMsRFtcxdeelxem5w4ixGTk/Zu4J2bm3JW2m0+7pVedx6JsJsH+cdPp+wSqiDYNeHLJRkByCrzAhLNexIQZjTXJuZFhbOakIzKH0biBqFfU0EtzbWE9TP9K6To+8hKlqy6XAytgpElApSDKCnIQeYtVs2DlIwBQTIUS4hQC7mSKoLDbVXyDtsQhO2tb383x4HHbwvHIl3BjU0B5cdejaKGyB0kWFXypWWwcPzZDw1AkZENshdAFaXt8Ge0thHvGQPC3mB3sEiYuG0R2HgNY9ZFrRr+Z7xjvRRkqhCMypFoRCcBf3C+/gnXbj54PnV0E5OoqIJaVA/F3CSk6wGBg4ReMy6xAui83JJiQXD+PrCZodQrOwSDK/Qvm5hsROhptiR4L79JRBeawuARtWLh4uFaOBHGEkxpaOslr8H6k7rLOY7nZe1mW/65u79YHn++EyezkTX3mS+9uDTn0/PYzQuxofbGZs86dJV864ZIo3t9cEMQp9eznrkt6uJ18DTXYCde9NMdwYf2QWg3TWsGzG0ZZWIXDOUHWE3a9g1cyhQ489JjOhrKMDcpom3c3rsWiujyfcBxc/wiuhfA83X+bppJ9uzl9et9D12FHW+pfKaajE0x89VvYa6te/L75fD78kuuWmcA17X3X071roV0HsdtTXYjm6vZx0+9EseDv/ucd15jN3cMHHbt/OD4fbx0goqOu037TVYT/NXLc67rJrm93dc7dfnfL7oup171hfN4Fl2WLMhOH4v8uWUsgYMgWpkIdaNBXOU7mJt95lLH6kBlLqU8P4YG4g3gLtwx4cMY/2uWyHLe/WTR/2b9fueo/HXP28ziA+4TpvHfO8kX/wkoviTToy8XzzpfP2+Ezyy4KzPk/wtwXTP7y5vUZA6njYzOO3FWXuCGXjQT6HOhAc0kzQc9bpohb60/JPJ/z8e++Hd9rH7fISEtbnn2GPS3IZrAEuWPfv2bU4b3U57xbuMZoUW+3M8DQEV9zX4vIQt9qYmuZh0TCb5jLaqtgHas0oUEf69E/b5/KnuQ8e+WKvXv80Ss9fPW7C8uqn8A8v2UuM38f26+b++r4f/qkHtb+Q8LwjCNQPVPalXD/fNGjkOcpfvQJ+FTVpMFhGwCJEBcaFCXtvjYTdq6GEiQwjKcFGEKtdam2Mz+itMzd//w+ojmdMRRkAAA==")))'
phase=binary-qualification
lscpu >cpu.txt
test "$(uname -m)" = x86_64
mkdir binaries
aws s3 cp s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261001/native/c00b766f65f8f0ae0adb5fcca786cb33c0daf046ff4b8f9a8bbcab39e1263533/two_bit_http binaries/two_bit_http --only-show-errors
aws s3 cp s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261001/native/6647839e08bcd5d50beb7badc313274028e0e5a36d2c354454fcfc0e325d1813/two_bit_plan_demo binaries/two_bit_plan_demo --only-show-errors
PYTHONPATH="$root/repo" python3.12 -m scripts.launch_native_semantic_router_cold_spot --stage "$root/repo" "$root"
phase=publication
systemd-run --unit=native-semantic-publication --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p RuntimeMaxSec=3600 -p WorkingDirectory="$root/repo" \
 --setenv=PYTHONPATH="$root/repo" --setenv=TOKIO_WORKER_THREADS=4 --setenv=AWS_MAX_ATTEMPTS=1 --setenv=BORSUK_NATIVE_MEMORY_BYTES=536870912 \
 bash -c 'ulimit -v 4194304 || exit 96; taskset -c 4-5 python3.12 -m scripts.launch_native_semantic_router_cold_spot --publish "$1/repo" "$1"' _ "$root"
phase=profile
systemd-run --unit=native-semantic-router-cold --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p RuntimeMaxSec=3030 -p WorkingDirectory="$root/repo" \
 --setenv=PYTHONPATH="$root/repo" --setenv=TOKIO_WORKER_THREADS=4 --setenv=AWS_MAX_ATTEMPTS=1 --setenv=BORSUK_NATIVE_MEMORY_BYTES=536870912 \
 /usr/bin/time -v -o "$root/profile-resources.txt" timeout --signal=TERM --kill-after=30 3000 \
 bash -c 'ulimit -v 4194304 || exit 96; taskset -c 4-5 python3.12 -m scripts.run_native_semantic_router_cold docs/research/performance-architecture-20260930/semantic-cold/config.json 72cd42fde1b52f08f90bd0d6d55e17fdbc5a208b28656911a74f2ef37e2686fb "$1/binaries/two_bit_http" "$1/boundary-check.json" "$1/screen"; code=$?; taskset -c 0-3 python3.12 -m scripts.check_native_startup_build --cgroup "$1/profile-cgroup.json" 8589934592 || exit 96; exit "$code"' _ "$root" >profile.log 2>&1
test -s "$root/source-qualification.json"
test -s "$root/binaries/two_bit_http"
test -s "$root/binaries/two_bit_plan_demo"
test -s "$root/cpu.txt"
test -s "$root/run-closed.log"
test -s "$root/profile.log"
test -s "$root/profile-resources.txt"
test -s "$root/profile-cgroup.json"
test -s "$root/screen/records.jsonl"
test -s "$root/screen/summary.json"
test -s "$root/screen/config.json"
test -s "$root/screen/qualification.json"
test -s "$root/native-assurance.json"
test -s "$root/boundary-check.json"
test -s "$root/native-source-manifest.json"
test -s "$root/native-source.tar.gz"
test -s "$root/qualified-source.tar.gz"
test -s "$root/publisher-proof.json"
test -s "$root/asset-manifest.json"
test -s "$root/assurance/affected-final.json"
test -s "$root/assurance/affected-final.log"
test -s "$root/assurance/release-final.json"
test -s "$root/assurance/release-final.log"
test -s "$root/assurance/clippy-final.json"
test -s "$root/assurance/clippy-final.log"
test -s "$root/assurance/test-build-final.json"
test -s "$root/assurance/test-build-final.log"
test -s "$root/assurance/full-workspace-final.json"
test -s "$root/assurance/full-workspace-final.log"
test -s "$root/publication/config.json"
test -s "$root/publication/asset-manifest.json"
test -s "$root/publication/publication-receipt.json"
test -s "$root/publication/ReLAION/control/native.jsonl"
test -f "$root/publication/ReLAION/control/stdout.log"
test -f "$root/publication/ReLAION/control/stderr.log"
test -s "$root/publication/ReLAION/control/resources.txt"
test -s "$root/publication/ReLAION/control/head.json"
test -s "$root/publication/ReLAION/candidate/native.jsonl"
test -f "$root/publication/ReLAION/candidate/stdout.log"
test -f "$root/publication/ReLAION/candidate/stderr.log"
test -s "$root/publication/ReLAION/candidate/resources.txt"
test -s "$root/publication/ReLAION/candidate/head.json"
test -s "$root/publication/CoHere/control/native.jsonl"
test -f "$root/publication/CoHere/control/stdout.log"
test -f "$root/publication/CoHere/control/stderr.log"
test -s "$root/publication/CoHere/control/resources.txt"
test -s "$root/publication/CoHere/control/head.json"
test -s "$root/publication/CoHere/candidate/native.jsonl"
test -f "$root/publication/CoHere/candidate/stdout.log"
test -f "$root/publication/CoHere/candidate/stderr.log"
test -s "$root/publication/CoHere/candidate/resources.txt"
test -s "$root/publication/CoHere/candidate/head.json"
phase=complete

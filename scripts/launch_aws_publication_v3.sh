#!/usr/bin/env bash
set -euo pipefail
export PYTHONDONTWRITEBYTECODE=1

if [[ "$#" -eq 2 && "$1" == "--stage-dataset" && -n "$2" ]]; then
  mode="$1"
  stage_dataset="$2"
elif [[ "$#" -eq 2 && "$1" == "--build-lifecycle" && -n "$2" ]]; then
  mode="$1"
  lifecycle_dataset="$2"
elif [[ "$#" -eq 3 && "$1" == "--build-read" && -n "$2" && -n "$3" ]]; then
  mode="$1"
  read_workload="$2"
  read_dataset="$3"
elif [[ "$#" -eq 5 && "$1" == "--run-read" && -n "$2" && -n "$3" && "$4" =~ ^r[0-9]{2}$ && "$5" =~ ^[0-9]+$ ]]; then
  mode="$1"
  read_workload="$2"
  read_dataset="$3"
  read_repetition="$4"
  read_arm_index="$5"
elif [[ "$#" -eq 3 && "$1" == "--diagnose-read" && -n "$2" && -n "$3" ]]; then
  mode="$1"
  read_workload="$2"
  read_dataset="$3"
elif [[ "$#" -eq 2 && "$1" == "--diagnose-lifecycle" && -n "$2" ]]; then
  mode="$1"
  lifecycle_dataset="$2"
elif [[ "$#" -eq 3 && "$1" == "--run-lifecycle" && -n "$2" && "$3" =~ ^[0-9]+$ ]]; then
  mode="$1"
  lifecycle_dataset="$2"
  lifecycle_arm_index="$3"
elif [[ "$#" -eq 1 && ( "$1" == "--dry-run" || "$1" == "--build-sift" || "$1" == "--read-recall-sift" || "$1" == "--read-concurrency-sift" ) ]]; then
  mode="$1"
else
  printf 'Publication V3 paid launch is unavailable until the AWS execution plan is implemented and reviewed\n' >&2
  printf 'usage: %s --dry-run|--build-sift|--read-recall-sift|--read-concurrency-sift|--build-read <workload-id> <dataset-id>|--run-read <workload-id> <dataset-id> <repetition-id> <arm-index>|--diagnose-read <workload-id> <dataset-id>|--stage-dataset <manifest-dataset-id>|--build-lifecycle <manifest-dataset-id>|--run-lifecycle <manifest-dataset-id> <arm-index>|--diagnose-lifecycle <manifest-dataset-id>\n' "$0" >&2
  exit 2
fi

cd "$(dirname "$0")/.."

if ! git diff --quiet || ! git diff --cached --quiet || [[ -n "$(git ls-files --others --exclude-standard)" ]]; then
  printf 'Publication V3 preflight requires a clean worktree including untracked files\n' >&2
  exit 2
fi

git fetch --quiet origin main
if [[ "$mode" == "--build-sift" || "$mode" == "--build-read" || "$mode" == "--run-read" || "$mode" == "--diagnose-read" || "$mode" == "--build-lifecycle" || "$mode" == "--run-lifecycle" || "$mode" == "--diagnose-lifecycle" || "$mode" == "--read-recall-sift" || "$mode" == "--read-concurrency-sift" ]]; then
  if ! git merge-base --is-ancestor HEAD origin/main; then
    printf 'Publication V3 frozen source commit must be contained in origin/main\n' >&2
    exit 2
  fi
else
  if ! git merge-base --is-ancestor origin/main HEAD; then
    printf 'origin/main must be an ancestor of the frozen source commit\n' >&2
    exit 2
  fi
  if [[ "$(git rev-parse HEAD)" != "$(git rev-parse origin/main)" ]]; then
    printf 'Publication V3 source commit must already be delivered to origin/main\n' >&2
    exit 2
  fi
fi

temporary="$(mktemp -d)"
trap 'rm -rf "$temporary"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
archive="$temporary/source-archive.tar.gz"
manifest="$temporary/manifest.json"
schedule="$temporary/schedule.json"
staging_plan="$temporary/staging-plan.json"
replay="$temporary/replay"
commit="$(git rev-parse HEAD)"

git archive --format=tar HEAD -- \
  Cargo.toml Cargo.lock crates scripts docs/research \
  python/uv.lock packages/borsuk/package-lock.json \
  | gzip -n >"$archive"

python3 - \
  docs/research/publication-v3-manifest.json \
  "$manifest" "$archive" "$commit" <<'PY'
import hashlib
import json
import pathlib
import sys

from scripts.publication_v3_protocol import canonical_json_bytes, validate_manifest


def digest(path: pathlib.Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


source, output, archive = map(pathlib.Path, sys.argv[1:4])
commit = sys.argv[4]
value = json.loads(source.read_text(encoding="utf-8"))
value["source"] = {
    "state": "frozen",
    "git_commit": commit,
    "archive_sha256": digest(archive),
    "cargo_lock_sha256": digest(pathlib.Path("Cargo.lock")),
    "python_lock_sha256": digest(pathlib.Path("python/uv.lock")),
    "node_lock_sha256": digest(pathlib.Path("packages/borsuk/package-lock.json")),
}
validated = validate_manifest(value)
output.write_bytes(canonical_json_bytes(validated))
PY

validation="$(python3 scripts/publication_v3_protocol.py validate "$manifest")"
python3 scripts/publication_v3_aws.py plan-staging "$manifest" >"$staging_plan"
paid_ready="$(python3 -c 'import json,sys; print(str(json.loads(sys.argv[1])["paid_ready"]).lower())' "$validation")"
structural_replay="blocked-until-paid-ready"
schedule_sha256=""
if [[ "$paid_ready" == "true" ]]; then
  python3 scripts/publication_v3_protocol.py schedule "$manifest" --output "$schedule" >/dev/null
  python3 scripts/publication_v3_protocol.py replay \
    "$manifest" --source-archive "$archive" --output "$replay" >/dev/null
  python3 scripts/validate_publication_v3_results.py \
    "$replay" --structural-only >/dev/null
  schedule_sha256="$(sha256sum "$schedule" | cut -d' ' -f1)"
  structural_replay="structurally-valid"
fi

if [[ "$mode" == "--stage-dataset" ]]; then
  if ! python3 -c 'import json,sys; sys.exit(0 if sys.argv[2] in {job["dataset_id"] for job in json.load(open(sys.argv[1]))["jobs"]} else 1)' \
    "$staging_plan" "$stage_dataset"; then
    printf 'Publication V3 stage dataset %s is not an unstaged manifest dataset\n' "$stage_dataset" >&2
    exit 2
  fi
  controller="${BORSUK_PUBLICATION_V3_CONTROLLER:-scripts/publication_v3_controller.py}"
  python3 "$controller" stage \
    --manifest "$manifest" \
    --source-archive "$archive" \
    --dataset "$stage_dataset" \
    --profile "${AWS_PROFILE:-causality}" \
    --image-id "${BORSUK_PUBLICATION_V3_AMI_ID:-ami-07bcecd13a160173f}" \
    --subnet-id "${BORSUK_PUBLICATION_V3_SUBNET_ID:-subnet-034528fbd6977848f}" \
    --security-group-id "${BORSUK_PUBLICATION_V3_SECURITY_GROUP_ID:-sg-0b1fd3e4fbde4af0d}" \
    --instance-profile-arn "${BORSUK_PUBLICATION_V3_INSTANCE_PROFILE_ARN:-arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile}" \
    --start-attempt "${BORSUK_PUBLICATION_V3_START_ATTEMPT:-1}" \
    --max-attempts "${BORSUK_PUBLICATION_V3_MAX_ATTEMPTS:-6}"
  exit 0
fi

if [[ "$mode" == "--build-read" ]]; then
  controller="${BORSUK_PUBLICATION_V3_CONTROLLER:-scripts/publication_v3_controller.py}"
  python3 "$controller" build-read \
    --manifest "$manifest" \
    --source-archive "$archive" \
    --workload "$read_workload" \
    --dataset "$read_dataset" \
    --profile "${AWS_PROFILE:-causality}" \
    --image-id "${BORSUK_PUBLICATION_V3_AMI_ID:-ami-07bcecd13a160173f}" \
    --subnet-id "${BORSUK_PUBLICATION_V3_SUBNET_ID:-subnet-034528fbd6977848f}" \
    --security-group-id "${BORSUK_PUBLICATION_V3_SECURITY_GROUP_ID:-sg-0b1fd3e4fbde4af0d}" \
    --instance-profile-arn "${BORSUK_PUBLICATION_V3_INSTANCE_PROFILE_ARN:-arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile}" \
    --attempt "${BORSUK_PUBLICATION_V3_BUILD_ATTEMPT:-0}" \
    --max-attempts "${BORSUK_PUBLICATION_V3_MAX_ATTEMPTS:-6}"
  exit 0
fi

if [[ "$mode" == "--run-read" ]]; then
  controller="${BORSUK_PUBLICATION_V3_CONTROLLER:-scripts/publication_v3_controller.py}"
  python3 "$controller" run-read \
    --manifest "$manifest" \
    --source-archive "$archive" \
    --workload "$read_workload" \
    --dataset "$read_dataset" \
    --repetition "$read_repetition" \
    --profile "${AWS_PROFILE:-causality}" \
    --image-id "${BORSUK_PUBLICATION_V3_AMI_ID:-ami-07bcecd13a160173f}" \
    --subnet-id "${BORSUK_PUBLICATION_V3_SUBNET_ID:-subnet-034528fbd6977848f}" \
    --security-group-id "${BORSUK_PUBLICATION_V3_SECURITY_GROUP_ID:-sg-0b1fd3e4fbde4af0d}" \
    --instance-profile-arn "${BORSUK_PUBLICATION_V3_INSTANCE_PROFILE_ARN:-arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile}" \
    --attempt "${BORSUK_PUBLICATION_V3_RUNTIME_ATTEMPT:-0}" \
    --build-attempt "${BORSUK_PUBLICATION_V3_BUILD_ATTEMPT:-0}" \
    --max-attempts "${BORSUK_PUBLICATION_V3_MAX_ATTEMPTS:-6}" \
    --arm-index "$read_arm_index" \
    --purchase-option "${BORSUK_PUBLICATION_V3_PURCHASE_OPTION:-spot}"
  exit 0
fi

if [[ "$mode" == "--diagnose-read" ]]; then
  controller="${BORSUK_PUBLICATION_V3_CONTROLLER:-scripts/publication_v3_controller.py}"
  python3 "$controller" diagnose-read \
    --manifest "$manifest" \
    --source-archive "$archive" \
    --workload "$read_workload" \
    --dataset "$read_dataset" \
    --repetition "${BORSUK_PUBLICATION_V3_REPETITION:-r01}" \
    --profile "${AWS_PROFILE:-causality}" \
    --image-id "${BORSUK_PUBLICATION_V3_AMI_ID:-ami-07bcecd13a160173f}" \
    --subnet-id "${BORSUK_PUBLICATION_V3_SUBNET_ID:-subnet-034528fbd6977848f}" \
    --security-group-id "${BORSUK_PUBLICATION_V3_SECURITY_GROUP_ID:-sg-0b1fd3e4fbde4af0d}" \
    --instance-profile-arn "${BORSUK_PUBLICATION_V3_INSTANCE_PROFILE_ARN:-arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile}" \
    --attempt "${BORSUK_PUBLICATION_V3_RUNTIME_ATTEMPT:-0}" \
    --build-attempt "${BORSUK_PUBLICATION_V3_BUILD_ATTEMPT:-0}" \
    --max-attempts "${BORSUK_PUBLICATION_V3_MAX_ATTEMPTS:-6}" \
    --arm-index "${BORSUK_PUBLICATION_V3_ARM_INDEX:-0}" \
    --nprobes "${BORSUK_PUBLICATION_V3_DIAGNOSTIC_NPROBES:-32,64}" \
    --candidates "${BORSUK_PUBLICATION_V3_DIAGNOSTIC_CANDIDATES:-512,1024,2048,4096}" \
    --purchase-option "${BORSUK_PUBLICATION_V3_PURCHASE_OPTION:-spot}"
  exit 0
fi

if [[ "$mode" == "--build-sift" ]]; then
  controller="${BORSUK_PUBLICATION_V3_CONTROLLER:-scripts/publication_v3_controller.py}"
  python3 "$controller" build-sift \
    --manifest "$manifest" \
    --source-archive "$archive" \
    --profile "${AWS_PROFILE:-causality}" \
    --image-id "${BORSUK_PUBLICATION_V3_AMI_ID:-ami-07bcecd13a160173f}" \
    --subnet-id "${BORSUK_PUBLICATION_V3_SUBNET_ID:-subnet-034528fbd6977848f}" \
    --security-group-id "${BORSUK_PUBLICATION_V3_SECURITY_GROUP_ID:-sg-0b1fd3e4fbde4af0d}" \
    --instance-profile-arn "${BORSUK_PUBLICATION_V3_INSTANCE_PROFILE_ARN:-arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile}" \
    --attempt "${BORSUK_PUBLICATION_V3_BUILD_ATTEMPT:-1}"
  exit 0
fi

if [[ "$mode" == "--build-lifecycle" ]]; then
  controller="${BORSUK_PUBLICATION_V3_CONTROLLER:-scripts/publication_v3_controller.py}"
  python3 "$controller" build-lifecycle \
    --manifest "$manifest" \
    --source-archive "$archive" \
    --dataset "$lifecycle_dataset" \
    --profile "${AWS_PROFILE:-causality}" \
    --image-id "${BORSUK_PUBLICATION_V3_AMI_ID:-ami-07bcecd13a160173f}" \
    --subnet-id "${BORSUK_PUBLICATION_V3_SUBNET_ID:-subnet-034528fbd6977848f}" \
    --security-group-id "${BORSUK_PUBLICATION_V3_SECURITY_GROUP_ID:-sg-0b1fd3e4fbde4af0d}" \
    --instance-profile-arn "${BORSUK_PUBLICATION_V3_INSTANCE_PROFILE_ARN:-arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile}" \
    --attempt "${BORSUK_PUBLICATION_V3_BUILD_ATTEMPT:-1}"
  exit 0
fi

if [[ "$mode" == "--read-recall-sift" ]]; then
  controller="${BORSUK_PUBLICATION_V3_CONTROLLER:-scripts/publication_v3_controller.py}"
  python3 "$controller" read-recall-sift \
    --manifest "$manifest" \
    --source-archive "$archive" \
    --profile "${AWS_PROFILE:-causality}" \
    --image-id "${BORSUK_PUBLICATION_V3_AMI_ID:-ami-07bcecd13a160173f}" \
    --subnet-id "${BORSUK_PUBLICATION_V3_SUBNET_ID:-subnet-034528fbd6977848f}" \
    --security-group-id "${BORSUK_PUBLICATION_V3_SECURITY_GROUP_ID:-sg-0b1fd3e4fbde4af0d}" \
    --instance-profile-arn "${BORSUK_PUBLICATION_V3_INSTANCE_PROFILE_ARN:-arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile}" \
    --attempt "${BORSUK_PUBLICATION_V3_RUNTIME_ATTEMPT:-1}" \
    --build-attempt "${BORSUK_PUBLICATION_V3_BUILD_ATTEMPT:-1}" \
    --arm-index "${BORSUK_PUBLICATION_V3_ARM_INDEX:-0}" \
    --purchase-option "${BORSUK_PUBLICATION_V3_PURCHASE_OPTION:-spot}"
  exit 0
fi

if [[ "$mode" == "--read-concurrency-sift" ]]; then
  controller="${BORSUK_PUBLICATION_V3_CONTROLLER:-scripts/publication_v3_controller.py}"
  python3 "$controller" read-concurrency-sift \
    --manifest "$manifest" \
    --source-archive "$archive" \
    --profile "${AWS_PROFILE:-causality}" \
    --image-id "${BORSUK_PUBLICATION_V3_AMI_ID:-ami-07bcecd13a160173f}" \
    --subnet-id "${BORSUK_PUBLICATION_V3_SUBNET_ID:-subnet-034528fbd6977848f}" \
    --security-group-id "${BORSUK_PUBLICATION_V3_SECURITY_GROUP_ID:-sg-0b1fd3e4fbde4af0d}" \
    --instance-profile-arn "${BORSUK_PUBLICATION_V3_INSTANCE_PROFILE_ARN:-arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile}" \
    --attempt "${BORSUK_PUBLICATION_V3_CONCURRENCY_ATTEMPT:-1}" \
    --build-attempt "${BORSUK_PUBLICATION_V3_BUILD_ATTEMPT:-1}" \
    --arm-index "${BORSUK_PUBLICATION_V3_ARM_INDEX:-0}" \
    --purchase-option "${BORSUK_PUBLICATION_V3_PURCHASE_OPTION:-spot}"
  exit 0
fi

if [[ "$mode" == "--run-lifecycle" ]]; then
  controller="${BORSUK_PUBLICATION_V3_CONTROLLER:-scripts/publication_v3_controller.py}"
  python3 "$controller" run-lifecycle \
    --manifest "$manifest" \
    --source-archive "$archive" \
    --dataset "$lifecycle_dataset" \
    --profile "${AWS_PROFILE:-causality}" \
    --image-id "${BORSUK_PUBLICATION_V3_AMI_ID:-ami-07bcecd13a160173f}" \
    --subnet-id "${BORSUK_PUBLICATION_V3_SUBNET_ID:-subnet-034528fbd6977848f}" \
    --security-group-id "${BORSUK_PUBLICATION_V3_SECURITY_GROUP_ID:-sg-0b1fd3e4fbde4af0d}" \
    --instance-profile-arn "${BORSUK_PUBLICATION_V3_INSTANCE_PROFILE_ARN:-arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile}" \
    --attempt "${BORSUK_PUBLICATION_V3_RUNTIME_ATTEMPT:-1}" \
    --build-attempt "${BORSUK_PUBLICATION_V3_BUILD_ATTEMPT:-1}" \
    --arm-index "$lifecycle_arm_index" \
    --purchase-option "${BORSUK_PUBLICATION_V3_PURCHASE_OPTION:-spot}"
  exit 0
fi

if [[ "$mode" == "--diagnose-lifecycle" ]]; then
  controller="${BORSUK_PUBLICATION_V3_CONTROLLER:-scripts/publication_v3_controller.py}"
  python3 "$controller" diagnose-lifecycle \
    --manifest "$manifest" \
    --source-archive "$archive" \
    --dataset "$lifecycle_dataset" \
    --profile "${AWS_PROFILE:-causality}" \
    --image-id "${BORSUK_PUBLICATION_V3_AMI_ID:-ami-07bcecd13a160173f}" \
    --subnet-id "${BORSUK_PUBLICATION_V3_SUBNET_ID:-subnet-034528fbd6977848f}" \
    --security-group-id "${BORSUK_PUBLICATION_V3_SECURITY_GROUP_ID:-sg-0b1fd3e4fbde4af0d}" \
    --instance-profile-arn "${BORSUK_PUBLICATION_V3_INSTANCE_PROFILE_ARN:-arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile}" \
    --attempt "${BORSUK_PUBLICATION_V3_RUNTIME_ATTEMPT:-1}" \
    --build-attempt "${BORSUK_PUBLICATION_V3_BUILD_ATTEMPT:-1}" \
    --write-ops "${BORSUK_PUBLICATION_V3_DIAGNOSTIC_WRITE_OPS:-2560}" \
    --timeout-seconds "${BORSUK_PUBLICATION_V3_DIAGNOSTIC_TIMEOUT_SECONDS:-1200}" \
    --purchase-option "${BORSUK_PUBLICATION_V3_PURCHASE_OPTION:-spot}"
  exit 0
fi

python3 - "$manifest" "$archive" "$validation" "$structural_replay" "$schedule_sha256" "$staging_plan" <<'PY'
import hashlib
import json
import pathlib
import sys

from scripts.publication_v3_protocol import canonical_json_bytes

manifest_path = pathlib.Path(sys.argv[1])
archive_path = pathlib.Path(sys.argv[2])
validation = json.loads(sys.argv[3])
manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
staging_plan_path = pathlib.Path(sys.argv[6])
staging_plan = json.loads(staging_plan_path.read_text(encoding="utf-8"))
if staging_plan["job_count"] != validation["unstaged_datasets"]:
    raise SystemExit("staging plan job count differs from unstaged datasets")
if staging_plan["manifest_sha256"] != hashlib.sha256(
    canonical_json_bytes(manifest)
).hexdigest():
    raise SystemExit("staging plan manifest identity differs")
report = {
    "schema_version": 1,
    "source_commit": manifest["source"]["git_commit"],
    "source_archive_sha256": hashlib.sha256(archive_path.read_bytes()).hexdigest(),
    "manifest_sha256": hashlib.sha256(canonical_json_bytes(manifest)).hexdigest(),
    "schedule_sha256": sys.argv[5] or None,
    "staging_jobs": staging_plan["job_count"],
    "staging_plan_sha256": hashlib.sha256(
        canonical_json_bytes(staging_plan)
    ).hexdigest(),
    "staging_plan": staging_plan,
    "cargo_lock_sha256": manifest["source"]["cargo_lock_sha256"],
    "python_lock_sha256": manifest["source"]["python_lock_sha256"],
    "node_lock_sha256": manifest["source"]["node_lock_sha256"],
    "result_prefix": manifest["prefixes"]["result"],
    "index_prefix": manifest["prefixes"]["index"],
    "paid_ready": validation["paid_ready"],
    "unstaged_datasets": validation["unstaged_datasets"],
    "structural_replay": sys.argv[4],
}
print(json.dumps(report, sort_keys=True, separators=(",", ":")))
PY

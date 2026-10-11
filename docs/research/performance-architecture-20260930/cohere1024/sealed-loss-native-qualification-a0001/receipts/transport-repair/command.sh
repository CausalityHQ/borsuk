set -eu
root=/mnt/borsuk-sealed-loss-qualification
mkdir "$root/evidence/analysis-inputs-failed-a0001"
systemctl show borsuk-loss-analysis-inputs.service > "$root/evidence/analysis-inputs-failed-a0001/unit.show"
journalctl -u borsuk-loss-analysis-inputs.service --no-pager > "$root/evidence/analysis-inputs-failed-a0001/journal.txt"
state=$(systemctl show borsuk-loss-analysis-inputs.service -p ActiveState --value)
test "$state" = failed || test "$state" = inactive
cg=$(systemctl show borsuk-loss-analysis-inputs.service -p ControlGroup --value)
if test -n "$cg" && test -d "/sys/fs/cgroup$cg"; then
 cp "/sys/fs/cgroup$cg/memory.events" "$root/evidence/analysis-inputs-failed-a0001/memory.events"
 cp "/sys/fs/cgroup$cg/cgroup.events" "$root/evidence/analysis-inputs-failed-a0001/cgroup.events"
 grep -qx 'populated 0' "$root/evidence/analysis-inputs-failed-a0001/cgroup.events"
fi
AWS_MAX_ATTEMPTS=1 timeout -k 2 30 aws s3 cp "$root/evidence/analysis-inputs-failed-a0001" s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261011/sealed-loss-native-qualification-a0001/stages/analysis-inputs-failed-a0001/ --recursive --only-show-errors
mv "$root/analysis-inputs" "$root/analysis-inputs-failed-a0001"
AWS_MAX_ATTEMPTS=1 timeout -k 2 30 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key research/semantic-router/20261011/sealed-loss-native-qualification-a0001/inputs/transport-r2/analysis-inputs.sh "$root/analysis-inputs-r2.sh" > /dev/null
printf '%s  %s\n' f33a11b2f73bb557ea912d12f259b33e7eca25a009830866f33c9cd15913141d "$root/analysis-inputs-r2.sh" | sha256sum -c -
systemd-run --unit=borsuk-loss-analysis-inputs-r2 --wait --pipe -p CPUQuota=100% -p AllowedCPUs=0 -p MemoryMax=256M -p MemorySwapMax=0 -p TasksMax=128 -p KillMode=control-group -p TimeoutStopSec=10 -p RuntimeMaxSec=480s /bin/bash "$root/analysis-inputs-r2.sh"

set -eu
printf '%s  /tmp/leaf-data-stage.sh
' 497917ba9ac5d8d92c96951bef51346b24f4ef4d771c9cd3fea48840047acbc3 | sha256sum -c -
printf '%s  /tmp/leaf-data-inner.sh
' 698a80e49b779bd9997c60c26d6428328336efd0a2f76f429255735004c966f6 | sha256sum -c -
/bin/bash /tmp/leaf-data-stage.sh experiment

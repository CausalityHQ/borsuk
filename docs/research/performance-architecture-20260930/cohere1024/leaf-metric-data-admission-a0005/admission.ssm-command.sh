set -eu
printf '%s  /tmp/leaf-data-stage.sh
' 1f415874e5ecda2ef3be11897feb73eac76e96ca03bd08320dcd581c7462a932 | sha256sum -c -
printf '%s  /tmp/leaf-data-inner.sh
' 6b280f04ee57959cdf868d6c2fade8409afefbc06f76920fb9697a47e72e4aab | sha256sum -c -
/bin/bash /tmp/leaf-data-stage.sh admission

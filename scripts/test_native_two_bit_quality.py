"""Small config trust/split boundary check; no dataset needed."""
import hashlib
import json
from pathlib import Path
import tempfile
from scripts.native_two_bit_quality import evaluate


def check():
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / 'config.json'
        body = json.dumps(dict(first=1,count=64)).encode()
        path.write_bytes(body)
        for digest, message in [('0'*64,'identity'),(hashlib.sha256(body).hexdigest(),'unsupported frozen split')]:
            try:
                evaluate(path,digest,'not-read','not-read')
            except ValueError as error:
                assert message in str(error)
            else:
                raise AssertionError('invalid configuration accepted')
    print('wrong config digest and unsupported split rejected before data reads')


if __name__ == '__main__':
    check()

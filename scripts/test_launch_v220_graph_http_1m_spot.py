import base64
import unittest

import boto3

from scripts.launch_v220_graph_http_1m_spot import user_data


class LauncherTest(unittest.TestCase):
    def test_ec2_receives_shell_script_after_boto3_encoding(self):
        script = user_data("0" * 40, "1" * 64, "key", "prefix", (), ())
        self.assertTrue(script.startswith("#!/bin/bash\n"))
        self.assertLessEqual(len(script.encode()), 16 * 1024)
        ec2 = boto3.client("ec2", region_name="eu-central-1")
        operation = ec2.meta.service_model.operation_model("RunInstances")
        params = {"ImageId": "ami-06121aa3085b6f918", "MinCount": 1,
                  "MaxCount": 1, "UserData": script}
        ec2.meta.events.emit("before-parameter-build.ec2.RunInstances",
                             params=params, model=operation, context={})
        wire = ec2._serializer.serialize_to_request(params, operation)["body"]["UserData"]
        self.assertEqual(base64.b64decode(wire), script.encode())


if __name__ == "__main__":
    unittest.main()

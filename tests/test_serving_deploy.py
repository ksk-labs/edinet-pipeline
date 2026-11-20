"""pipeline.serving_deploy のテスト (P2-T6)。

実行: リポジトリのルートで `python -m unittest discover -s tests`
Cloudflare へは接続せず、call と raw_put はフェイクで置き換える。
"""
import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.serving_deploy import (
    DeployError, build_metadata, build_multipart, deploy,
    get_or_create_subdomain,
)


class FakeCall:
    def __init__(self, subdomain=None):
        self.subdomain = subdomain
        self.calls = []

    def __call__(self, method, path, body=None):
        self.calls.append((method, path, body))
        if method == "GET" and path.endswith("/workers/subdomain"):
            if self.subdomain:
                return 200, {"success": True,
                             "result": {"subdomain": self.subdomain}}
            return 404, {"success": False, "errors": [{"code": 10007}]}
        if method == "POST" and path.endswith("/workers/subdomain"):
            self.subdomain = body["subdomain"]
            return 200, {"success": True,
                         "result": {"subdomain": self.subdomain}}
        raise AssertionError(f"unexpected call: {method} {path}")


def parse_multipart(body, content_type):
    boundary = content_type.split("boundary=")[1]
    parts = {}
    for chunk in body.split(("--" + boundary).encode()):
        if b"Content-Disposition" not in chunk:
            continue
        header, _, payload = chunk.partition(b"\r\n\r\n")
        name = header.decode().split('name="')[1].split('"')[0]
        parts[name] = payload.rstrip(b"\r\n")
    return parts


class MetadataAndMultipartTest(unittest.TestCase):
    def test_metadata_binds_db(self):
        metadata = build_metadata("db-uuid")
        self.assertEqual(metadata["main_module"], "serving_api.js")
        self.assertEqual(metadata["bindings"],
                         [{"type": "d1", "name": "DB", "id": "db-uuid"}])

    def test_multipart_carries_metadata_and_module(self):
        body, content_type = build_multipart(
            build_metadata("db-uuid"), "serving_api.js",
            b"export default {}", "test-boundary")
        parts = parse_multipart(body, content_type)
        metadata = json.loads(parts["metadata"])
        self.assertEqual(metadata["bindings"][0]["id"], "db-uuid")
        self.assertEqual(parts["serving_api.js"], b"export default {}")


class SubdomainTest(unittest.TestCase):
    def test_existing_subdomain_is_used(self):
        call = FakeCall(subdomain="muse-ksk")
        self.assertEqual(get_or_create_subdomain(call, "acct"), "muse-ksk")
        self.assertEqual([c[0] for c in call.calls], ["GET"])

    def test_missing_subdomain_is_created_from_desired(self):
        call = FakeCall()
        self.assertEqual(
            get_or_create_subdomain(call, "acct", "edinet-serving"),
            "edinet-serving")
        self.assertEqual([c[0] for c in call.calls], ["GET", "POST"])

    def test_missing_subdomain_without_desired_raises(self):
        with self.assertRaises(DeployError):
            get_or_create_subdomain(FakeCall(), "acct")


class DeployTest(unittest.TestCase):
    def test_deploy_uploads_and_returns_url(self):
        call = FakeCall(subdomain="muse-ksk")
        captured = {}

        def raw_put(path, body, content_type):
            captured.update(path=path, body=body,
                            content_type=content_type)
            return 200, {"success": True, "result": {}}

        result = deploy(call, raw_put, "acct", "db-uuid",
                        b"export default {}")
        self.assertEqual(
            captured["path"],
            "/accounts/acct/workers/scripts/edinet-serving-api")
        self.assertEqual(
            result["url"], "https://edinet-serving-api.muse-ksk.workers.dev")

    def test_deploy_failure_raises(self):
        def raw_put(path, body, content_type):
            return 400, {"success": False, "errors": [{"code": 10021}]}

        with self.assertRaises(DeployError):
            deploy(FakeCall(subdomain="x"), raw_put, "acct", "db-uuid",
                   b"js")


if __name__ == "__main__":
    unittest.main()

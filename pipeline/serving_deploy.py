"""提供層 Worker のデプロイ (P2-T6、仕様 #15 §9)。

workers/serving_api.js を Workers Scripts API へ multipart で投入し、
D1 バインディング (DB -> edinet-serving) を付ける。workers.dev の
サブドメインが未設定なら API で設定し、公開 URL を返す。

資格情報は実行時に Cloudflare コネクタのヘルパー (cf_api.py) を
import して使う。JSON の呼び出しは cf_api.call、multipart の投入は
cf_api が取り込むサロゲート付与を同じく使う。HTTP 層は差し替え
可能で、単体テストは Cloudflare へ接続しない。

使い方:
  python -m pipeline.serving_deploy deploy --account-id ID \
      --database-id UUID [--script workers/serving_api.js] \
      [--worker-name edinet-serving-api] [--subdomain NAME] \
      [--cf-api-path PATH]
"""
import argparse
import json
import os
import sys
import urllib.error
import urllib.request
import uuid

from .serving_sync import load_cf_module

DEFAULT_WORKER_NAME = "edinet-serving-api"
DEFAULT_SUBDOMAIN = "edinet-serving"
COMPATIBILITY_DATE = "2026-11-20"
API_BASE = "https://api.cloudflare.com/client/v4"


class DeployError(RuntimeError):
    pass


def build_metadata(database_id, main_module="serving_api.js"):
    return {
        "main_module": main_module,
        "compatibility_date": COMPATIBILITY_DATE,
        "bindings": [{"type": "d1", "name": "DB", "id": database_id}],
    }


def build_multipart(metadata, module_name, module_bytes, boundary):
    """Workers Scripts API の multipart ボディを組み立てる。"""
    parts = []
    parts.append(f"--{boundary}\r\n".encode())
    parts.append(b'Content-Disposition: form-data; name="metadata"\r\n')
    parts.append(b"Content-Type: application/json\r\n\r\n")
    parts.append(json.dumps(metadata).encode())
    parts.append(b"\r\n")
    parts.append(f"--{boundary}\r\n".encode())
    parts.append(
        f'Content-Disposition: form-data; name="{module_name}"; '
        f'filename="{module_name}"\r\n'.encode())
    parts.append(b"Content-Type: application/javascript+module\r\n\r\n")
    parts.append(module_bytes)
    parts.append(b"\r\n")
    parts.append(f"--{boundary}--\r\n".encode())
    return b"".join(parts), f"multipart/form-data; boundary={boundary}"


def make_raw_put(cf_module):
    """cf_api のサロゲート付与を使う multipart PUT 関数を作る。"""
    def raw_put(path, body, content_type):
        request = urllib.request.Request(
            API_BASE + path, data=body, method="PUT",
            headers={"Content-Type": content_type})
        cf_module.add_surrogate_to_request(
            request, "custom.cloudflare", allowed_hosts=["api.cloudflare.com"])
        try:
            opener = urllib.request.build_opener(cf_module.NoRedirect())
            with opener.open(request, timeout=120) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            payload = e.read().decode("utf-8", errors="replace")
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                pass
            return e.code, payload
    return raw_put


def get_or_create_subdomain(call, account_id, desired=None):
    """workers.dev サブドメインを取得し、未設定なら desired で設定する。"""
    path = f"/accounts/{account_id}/workers/subdomain"
    status, resp = call("GET", path)
    if status == 200 and resp.get("success"):
        return resp["result"]["subdomain"]
    if not desired:
        raise DeployError(
            "workers.dev サブドメインが未設定で、設定する名前が無い")
    status, resp = call("POST", path, {"subdomain": desired})
    if status != 200 or not resp.get("success"):
        raise DeployError(
            f"サブドメインの設定に失敗した: status={status} resp={resp}")
    return resp["result"]["subdomain"]


def deploy(call, raw_put, account_id, database_id, script_bytes,
           worker_name=DEFAULT_WORKER_NAME, subdomain=None):
    """Worker を投入し、公開 URL 等の結果 (dict) を返す。"""
    metadata = build_metadata(database_id)
    body, content_type = build_multipart(
        metadata, "serving_api.js", script_bytes, uuid.uuid4().hex)
    status, resp = raw_put(
        f"/accounts/{account_id}/workers/scripts/{worker_name}",
        body, content_type)
    if status != 200 or not resp.get("success"):
        raise DeployError(
            f"Worker の投入に失敗した: status={status} resp={resp}")
    sub = get_or_create_subdomain(call, account_id, subdomain)
    return {
        "worker_name": worker_name,
        "subdomain": sub,
        "url": f"https://{worker_name}.{sub}.workers.dev",
    }


def main(argv=None):
    parser = argparse.ArgumentParser(prog="pipeline.serving_deploy")
    sub = parser.add_subparsers(dest="command", required=True)
    deploy_parser = sub.add_parser("deploy")
    deploy_parser.add_argument("--account-id", required=True)
    deploy_parser.add_argument("--database-id", required=True)
    deploy_parser.add_argument(
        "--script",
        default=os.path.join(os.path.dirname(os.path.dirname(
            os.path.abspath(__file__))), "workers", "serving_api.js"))
    deploy_parser.add_argument("--worker-name", default=DEFAULT_WORKER_NAME)
    deploy_parser.add_argument("--subdomain", default=DEFAULT_SUBDOMAIN)
    deploy_parser.add_argument("--cf-api-path")
    args = parser.parse_args(argv)
    with open(args.script, "rb") as f:
        script_bytes = f.read()
    module = load_cf_module(args.cf_api_path)
    result = deploy(module.call, make_raw_put(module), args.account_id,
                    args.database_id, script_bytes,
                    worker_name=args.worker_name, subdomain=args.subdomain)
    print(json.dumps(result, ensure_ascii=False, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Smoke test: authenticate to DXtrade and fetch account balance."""
import json
import os
import sys
from pathlib import Path
from urllib import request as urlrequest
from urllib.error import HTTPError, URLError


def load_env(path: Path) -> dict:
    env = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip()
    return env


def post_json(url: str, payload: dict, headers: dict | None = None) -> tuple[int, dict | str]:
    body = json.dumps(payload).encode("utf-8")
    hdrs = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    }
    if headers:
        hdrs.update(headers)
    req = urlrequest.Request(url, data=body, headers=hdrs, method="POST")
    try:
        with urlrequest.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8")
            try:
                return resp.status, json.loads(raw)
            except json.JSONDecodeError:
                return resp.status, raw
    except HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace")
        try:
            return e.code, json.loads(raw)
        except json.JSONDecodeError:
            return e.code, raw
    except URLError as e:
        return -1, f"URLError: {e.reason}"


def get_json(url: str, headers: dict) -> tuple[int, dict | str]:
    hdrs = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    }
    hdrs.update(headers)
    req = urlrequest.Request(url, headers=hdrs, method="GET")
    try:
        with urlrequest.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8")
            try:
                return resp.status, json.loads(raw)
            except json.JSONDecodeError:
                return resp.status, raw
    except HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace")
        try:
            return e.code, json.loads(raw)
        except json.JSONDecodeError:
            return e.code, raw
    except URLError as e:
        return -1, f"URLError: {e.reason}"


def try_login(host: str, username: str, password: str, domain: str) -> tuple[int, dict | str]:
    url = f"{host}/dxsca-web/login"
    return post_json(url, {"username": username, "password": password, "domain": domain})


def main() -> int:
    here = Path(__file__).parent
    env = load_env(here / ".env")
    host = env["DXTRADE_HOST"].rstrip("/")
    username = env["DXTRADE_USERNAME"]
    password = env["DXTRADE_PASSWORD"]
    primary_domain = env.get("DXTRADE_DOMAIN", "default")
    account = env.get("DXTRADE_ACCOUNT", "")

    candidates = [primary_domain]
    for fallback in ("default", "tradeifycrypto", "tradeify"):
        if fallback not in candidates:
            candidates.append(fallback)

    token = None
    used_domain = None
    last_status = None
    last_body = None
    for d in candidates:
        print(f"[login] trying domain={d!r} ...")
        status, body = try_login(host, username, password, d)
        last_status, last_body = status, body
        print(f"  -> HTTP {status}")
        if status == 200 and isinstance(body, dict) and "sessionToken" in body:
            token = body["sessionToken"]
            used_domain = d
            break
        if status == 200 and isinstance(body, dict):
            for k in ("token", "session", "access_token"):
                if k in body:
                    token = body[k]
                    used_domain = d
                    break
            if token:
                break
        print(f"  body: {body}")

    if not token:
        print("\n[FAIL] could not obtain session token.")
        print(f"last status: {last_status}")
        print(f"last body:   {last_body}")
        return 1

    print(f"\n[OK] logged in with domain={used_domain!r}")
    print(f"token (first 16 chars): {str(token)[:16]}...")

    auth_headers = {
        "Authorization": f"DXAPI {token}",
        "Accept": "application/json",
    }

    from urllib.parse import quote
    a = quote(account, safe="")
    for path in (
        f"/dxsca-web/accounts/{a}/metrics",
        f"/dxsca-web/accounts/{a}/positions",
        f"/dxsca-web/accounts/{a}/orders",
        f"/dxsca-web/accounts/{a}/balances",
    ):
        url = f"{host}{path}"
        print(f"\n[probe] GET {url}")
        status, body = get_json(url, auth_headers)
        print(f"  -> HTTP {status}")
        if isinstance(body, dict):
            print(f"  body: {json.dumps(body, indent=2)[:1500]}")
        else:
            print(f"  body: {str(body)[:400]}")

    return 0


if __name__ == "__main__":
    sys.exit(main())

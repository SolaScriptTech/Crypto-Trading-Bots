"""Thin DXtrade REST client: env-loaded login + JSON GET/POST."""
import json
from pathlib import Path
from urllib import request as urlrequest
from urllib.error import HTTPError, URLError
from urllib.parse import quote

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"


def load_env(path: Path) -> dict:
    env = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip()
    return env


def _request(method: str, url: str, headers: dict, body: bytes | None = None) -> tuple[int, dict | str]:
    hdrs = {"User-Agent": UA, "Accept": "application/json"}
    hdrs.update(headers)
    if body is not None:
        hdrs.setdefault("Content-Type", "application/json")
    req = urlrequest.Request(url, data=body, headers=hdrs, method=method)
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


class DXtradeClient:
    def __init__(self, env: dict):
        self.host = env["DXTRADE_HOST"].rstrip("/")
        self.username = env["DXTRADE_USERNAME"]
        self.password = env["DXTRADE_PASSWORD"]
        self.domain = env.get("DXTRADE_DOMAIN", "default")
        self.account = env["DXTRADE_ACCOUNT"]
        self.token: str | None = None

    def login(self) -> None:
        url = f"{self.host}/dxsca-web/login"
        body = json.dumps({"username": self.username, "password": self.password, "domain": self.domain}).encode("utf-8")
        for domain in (self.domain, "default"):
            body = json.dumps({"username": self.username, "password": self.password, "domain": domain}).encode("utf-8")
            status, data = _request("POST", url, {}, body)
            if status == 200 and isinstance(data, dict) and "sessionToken" in data:
                self.token = data["sessionToken"]
                self.domain = domain
                return
        raise RuntimeError(f"login failed: {status} {data}")

    def _auth(self) -> dict:
        if not self.token:
            self.login()
        return {"Authorization": f"DXAPI {self.token}"}

    def get(self, path: str) -> tuple[int, dict | str]:
        return _request("GET", f"{self.host}{path}", self._auth())

    def post(self, path: str, payload: dict) -> tuple[int, dict | str]:
        body = json.dumps(payload).encode("utf-8")
        return _request("POST", f"{self.host}{path}", self._auth(), body)

    def account_path(self, suffix: str) -> str:
        return f"/dxsca-web/accounts/{quote(self.account, safe='')}{suffix}"

    def metrics(self) -> dict:
        status, data = self.get(self.account_path("/metrics"))
        if status != 200 or not isinstance(data, dict):
            raise RuntimeError(f"metrics failed: {status} {data}")
        return data["metrics"][0]

    def positions(self) -> list:
        status, data = self.get(self.account_path("/positions"))
        if status != 200 or not isinstance(data, dict):
            raise RuntimeError(f"positions failed: {status} {data}")
        return data.get("positions", [])

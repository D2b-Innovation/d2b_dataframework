# alodesk_api.py
import requests
import pandas as pd
from datetime import date, timedelta
from typing import Any, Dict, Optional, Iterator
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

class Alodesk_API:
    def __init__(self, base_url: str, token: str, verbose_logger=None):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.verbose = verbose_logger if verbose_logger else _null_verbose()

        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json",
            "User-Agent": "alodesk-api/1.0 (+https://automovil.alodesk.io)"
        }

        self.verbose.log("--- INIT Alodesk_API v1.0 ---")

    @retry(
        reraise=True,
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((requests.HTTPError, requests.ConnectionError, requests.Timeout)),
    )
    def _fetch(self, endpoint: str, *, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        self.verbose.log(f"GET {url} | params={params or {}}")

        resp = requests.get(url, headers=self.headers, params=params, timeout=30)

        if resp.status_code == 429:
            self.verbose.critical(f"Rate-limit alcanzado: {resp.text}")

        resp.raise_for_status()
        return resp.json()

    def _paginate(self, endpoint: str, *, params: Optional[Dict[str, Any]] = None, page_param: str = "page") -> Iterator[Dict[str, Any]]:
        base_params = params or {}
        page = 1

        while True:
            payload = {**base_params, page_param: page}
            data = self._fetch(endpoint, params=payload)

            if isinstance(data, list):  # No hay paginación
                if not data:
                    break
                yield from data
                break

            elif isinstance(data, dict):
                results = data.get("results")
                if not results:
                    break
                yield from results

                if not data.get("next"):
                    break
                page += 1

            else:
                self.verbose.critical(f"_paginate | Formato inesperado: {type(data)}")
                break

    def download_leads(self, *, days_back: int = 7) -> pd.DataFrame:
        hoy = date.today()
        inicio = hoy - timedelta(days=days_back)

        params = {
            "startDate": inicio.isoformat(),
            "endDate": hoy.isoformat(),
        }

        self.verbose.log(f"Descargando leads del {inicio.isoformat()} al {hoy.isoformat()}")

        rows = list(self._paginate("api/leads/report/", params=params))
        df = pd.DataFrame(rows)
        self.verbose.log(f"Leads descargados: {len(df)} filas")
        return df

# Utilidad fallback si no hay `Verbose`
def _null_verbose():
    class Dummy:
        def log(self, *a, **k): pass
        def critical(self, *a, **k): print("[CRITICAL]", *a)
    return Dummy()
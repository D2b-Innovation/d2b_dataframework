import os
import time
import json
import typing as t
import datetime as dt
import pandas as pd
import requests
from urllib.parse import urlencode


class HubSpot_API:
    """
    Lightweight client para extracción de datos desde HubSpot (Private App token).
    - Soporta paginación v3 (parámetro 'after')
    - Manejo de rate limit (HTTP 429) con backoff y 'Retry-After'
    - Búsqueda (POST /search) con filtro por hs_lastmodifieddate (updated since)
    - Conversión de resultados a pandas.DataFrame
    - Integración opcional con verbose_logger (tu clase Verbose u otro logger)

    Parámetros
    ----------
    token : str | None
        Token de Private App (Bearer). Si es None, intenta leer de variable de entorno HUBSPOT_TOKEN.
    base_url : str
        URL base de HubSpot API. Por defecto 'https://api.hubapi.com'.
    timeout : int
        Timeout por request (segundos).
    max_retries : int
        Reintentos ante errores transitorios (429/5xx).
    backoff_factor : float
        Factor exponencial de espera entre reintentos (1, 2, 4, ... * backoff_factor).
    verbose_logger : object | None
        Instancia de logger compatible (por ejemplo, tu clase Verbose). Es opcional.
    """

    def __init__(
        self,
        token: t.Optional[str] = None,
        base_url: str = "https://api.hubapi.com",
        timeout: int = 30,
        max_retries: int = 5,
        backoff_factor: float = 1.0,
        verbose_logger: t.Optional[object] = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.verbose_logger = verbose_logger

        self.token = token or os.getenv("HUBSPOT_TOKEN", "")
        if not self.token:
            self._v("No se encontró token. Puedes pasarlo por parámetro o variable de entorno HUBSPOT_TOKEN.", level="warning")

        # Session para reusar conexión TCP
        self.session = requests.Session()
        self.session.headers.update(self._build_headers())

    # ------------------------ Helpers de logging ------------------------

    def _v(self, msg: str, level: str = "info"):
        """
        Wrapper de logging. Intenta usar el verbose_logger si está definido.
        Acepta niveles: 'info', 'debug', 'warning', 'error', 'critical'.
        Si no hay logger, hace 'print' solo para niveles 'error'/'critical'.
        """
        logger = self.verbose_logger
        if logger is not None:
            # Intenta métodos comunes de log; si no existen, hace un fallback genérico.
            for method_name in (level, "log", "info"):
                if hasattr(logger, method_name):
                    try:
                        getattr(logger, method_name)(msg)  # logger.info(msg) o logger.log(msg)
                        return
                    except Exception:
                        pass
        # Fallback mínimo sin logger (evitar ruido):
        if level in ("error", "critical"):
            print(f"[{level.upper()}] {msg}")

    # ------------------------ Autenticación / estado ------------------------

    def _build_headers(self) -> dict:
        """
        Construye los headers estándar de autenticación y contenido.
        RETURNS
        -------
        dict : cabeceras para requests
        """
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def set_token(self, token: str) -> None:
        """
        Actualiza el token en caliente y la sesión.
        ARGS
        ----
        token : str
            Nuevo Bearer token (Private App).
        """
        self.token = token
        self.session.headers.update(self._build_headers())
        self._v("Token actualizado en sesión.", level="debug")

    def get_token(self) -> str:
        """
        Retorna el token actual (útil para verificaciones).
        RETURNS
        -------
        str
        """
        return self.token

    def get_session(self) -> requests.Session:
        """
        Retorna el objeto Session interno (similar a get_service).
        RETURNS
        -------
        requests.Session
        """
        return self.session

    # ------------------------ Requests / Retry / Rate Limit ------------------------

    def _request(
        self,
        method: str,
        path: str,
        params: t.Optional[dict] = None,
        json_body: t.Optional[dict] = None,
    ) -> dict:
        """
        Ejecuta una solicitud HTTP con reintentos y manejo de rate limit.
        ARGS
        ----
        method : str
            Método HTTP ('GET', 'POST', etc.).
        path : str
            Ruta relativa (e.g., '/crm/v3/objects/contacts').
        params : dict | None
            Parámetros de querystring.
        json_body : dict | None
            Cuerpo JSON (para POST/PUT/PATCH).

        RETURNS
        -------
        dict : Respuesta JSON parseada.

        RAISES
        ------
        requests.HTTPError si no se puede recuperar tras reintentos.
        """
        url = f"{self.base_url}{path}"
        attempt = 0

        while True:
            try:
                resp = self.session.request(
                    method=method.upper(),
                    url=url,
                    params=params,
                    json=json_body,
                    timeout=self.timeout,
                )
            except requests.RequestException as ex:
                attempt += 1
                if attempt > self.max_retries:
                    self._v(f"Error de red definitivo en {method} {url}: {ex}", level="critical")
                    raise
                sleep_s = self.backoff_factor * (2 ** (attempt - 1))
                self._v(f"Error de red. Reintentando en {sleep_s:.1f}s (intento {attempt}/{self.max_retries})", level="warning")
                time.sleep(sleep_s)
                continue

            # Rate limit
            if resp.status_code == 429:
                attempt += 1
                if attempt > self.max_retries:
                    self._v(f"HTTP 429 persistente en {method} {url}. Abortando.", level="critical")
                    resp.raise_for_status()
                wait_for = resp.headers.get("Retry-After")
                if wait_for:
                    try:
                        wait_s = float(wait_for)
                    except ValueError:
                        wait_s = self.backoff_factor * (2 ** (attempt - 1))
                else:
                    wait_s = self.backoff_factor * (2 ** (attempt - 1))
                self._v(f"Rate limit 429. Esperando {wait_s:.1f}s (intento {attempt}/{self.max_retries})", level="warning")
                time.sleep(wait_s)
                continue

            # Errores 5xx (transitorios)
            if 500 <= resp.status_code < 600:
                attempt += 1
                if attempt > self.max_retries:
                    self._v(f"HTTP {resp.status_code} persistente en {method} {url}. Abortando.", level="critical")
                    resp.raise_for_status()
                sleep_s = self.backoff_factor * (2 ** (attempt - 1))
                self._v(f"HTTP {resp.status_code}. Reintentando en {sleep_s:.1f}s (intento {attempt}/{self.max_retries})", level="warning")
                time.sleep(sleep_s)
                continue

            # Otros errores
            if not resp.ok:
                self._v(f"HTTP {resp.status_code} en {method} {url}: {resp.text}", level="error")
                resp.raise_for_status()

            try:
                return resp.json()
            except ValueError:
                # Respuesta sin JSON válido
                self._v(f"Respuesta sin JSON en {method} {url}", level="error")
                return {}

    # ------------------------ Paginación genérica (v3 objects) ------------------------

    def _paginate_objects(
        self,
        object_type: str,
        properties: t.Optional[t.List[str]] = None,
        limit: int = 100,
        archived: bool = False,
        max_pages: t.Optional[int] = None,
        after: t.Optional[str] = None,
    ) -> t.List[dict]:
        """
        Pagina objetos CRM v3: /crm/v3/objects/{object_type}
        ARGS
        ----
        object_type : str
            'contacts', 'companies', 'deals', etc.
        properties : list[str] | None
            Lista de propiedades a solicitar (reduce payload).
        limit : int
            Tamaño de página (máx. típico 100).
        archived : bool
            Incluir registros archivados.
        max_pages : int | None
            Límite de páginas a recorrer (None = sin tope).
        after : str | None
            Cursor de paginación inicial.

        RETURNS
        -------
        list[dict] : Lista de registros (dicts) tal como los entrega HubSpot.
        """
        path = f"/crm/v3/objects/{object_type}"
        params = {
            "limit": limit,
            "archived": str(archived).lower(),
        }
        if properties:
            params["properties"] = ",".join(properties)
        if after:
            params["after"] = after

        all_results: t.List[dict] = []
        pages = 0

        while True:
            res = self._request("GET", path, params=params)
            results = res.get("results", [])
            all_results.extend(results)
            pages += 1

            paging = res.get("paging", {})
            next_link = paging.get("next", {})
            next_after = next_link.get("after")

            self._v(f"Página {pages}: {len(results)} registros (acum: {len(all_results)})", level="debug")

            if not next_after:
                break
            if max_pages and pages >= max_pages:
                break
            params["after"] = next_after

        return all_results

    # ------------------------ Búsqueda (search) con filtros ------------------------

    def search_objects_updated_since(
        self,
        object_type: str,
        since,
        properties=None,
        limit_per_page: int = 100,
        max_pages: t.Optional[int] = None,
        sorts: t.Optional[t.List[dict]] = None,
    ) -> t.List[dict]:
        """
        Busca objetos con hs_lastmodifieddate > since.
        Intenta value en epoch ms (string), si falla reintenta ISO, y opcionalmente con 'createdate'.
        """
        import datetime as dt
        import pandas as pd

        # Normaliza 'since' a datetime UTC
        if isinstance(since, (dt.datetime, dt.date)):
            if isinstance(since, dt.date) and not isinstance(since, dt.datetime):
                since = dt.datetime.combine(since, dt.time.min, tzinfo=dt.timezone.utc)
            elif since.tzinfo is None:
                since = since.replace(tzinfo=dt.timezone.utc)
        else:
            since = pd.to_datetime(since, utc=True).to_pydatetime()

        since_ms = str(int(since.timestamp() * 1000))  # ← **como string**
        since_iso = since.isoformat()                   # ← ISO 8601

        path = f"/crm/v3/objects/{object_type}/search"

        def _run(body):
            all_results, pages, after = [], 0, None
            while True:
                if after:
                    body["after"] = after
                res = self._request("POST", path, json_body=body)
                results = res.get("results", [])
                all_results.extend(results)
                pages += 1
                self._v(f"Search página {pages}: {len(results)}", level="debug")
                after = res.get("paging", {}).get("next", {}).get("after")
                if not after or (max_pages and pages >= max_pages):
                    break
            return all_results

        # 1) Epoch ms (string) con hs_lastmodifieddate
        body_ms = {
            "filterGroups": [{
                "filters": [{
                    "propertyName": "hs_lastmodifieddate",
                    "operator": "GT",
                    "value": since_ms
                }]
            }],
            "properties": properties or [],
            "limit": limit_per_page,
        }
        if sorts: body_ms["sorts"] = sorts
        try:
            return _run(body_ms)
        except Exception as e1:
            self._v(f"Search falló con ms-string, reintentando con ISO: {e1}", level="warning")

        # 2) ISO 8601 con hs_lastmodifieddate
        body_iso = {
            "filterGroups": [{
                "filters": [{
                    "propertyName": "hs_lastmodifieddate",
                    "operator": "GT",
                    "value": since_iso
                }]
            }],
            "properties": properties or [],
            "limit": limit_per_page,
        }
        if sorts: body_iso["sorts"] = sorts
        try:
            return _run(body_iso)
        except Exception as e2:
            self._v(f"Search falló con ISO, probando 'createdate' con ms-string: {e2}", level="warning")

        # 3) Fallback: createdate con ms-string (para aislar problema de propiedad)
        body_createdate = {
            "filterGroups": [{
                "filters": [{
                    "propertyName": "createdate",
                    "operator": "GT",
                    "value": since_ms
                }]
            }],
            "properties": properties or [],
            "limit": limit_per_page,
        }
        if sorts: body_createdate["sorts"] = sorts
        return _run(body_createdate)
    # ------------------------ Wrappers por objeto ------------------------

    def get_contacts(
        self,
        properties: t.Optional[t.List[str]] = None,
        limit: int = 100,
        max_pages: t.Optional[int] = None,
        archived: bool = False,
        after: t.Optional[str] = None,
    ) -> t.List[dict]:
        """
        Lista contactos con paginación estándar (sin filtros de fecha).
        Usa GET /crm/v3/objects/contacts
        """
        return self._paginate_objects(
            object_type="contacts",
            properties=properties,
            limit=limit,
            archived=archived,
            max_pages=max_pages,
            after=after,
        )

    def get_companies(
        self,
        properties: t.Optional[t.List[str]] = None,
        limit: int = 100,
        max_pages: t.Optional[int] = None,
        archived: bool = False,
        after: t.Optional[str] = None,
    ) -> t.List[dict]:
        """
        Lista empresas con paginación estándar (sin filtros de fecha).
        Usa GET /crm/v3/objects/companies
        """
        return self._paginate_objects(
            object_type="companies",
            properties=properties,
            limit=limit,
            archived=archived,
            max_pages=max_pages,
            after=after,
        )

    def get_deals(
        self,
        properties: t.Optional[t.List[str]] = None,
        limit: int = 100,
        max_pages: t.Optional[int] = None,
        archived: bool = False,
        after: t.Optional[str] = None,
    ) -> t.List[dict]:
        """
        Lista deals con paginación estándar (sin filtros de fecha).
        Usa GET /crm/v3/objects/deals
        """
        return self._paginate_objects(
            object_type="deals",
            properties=properties,
            limit=limit,
            archived=archived,
            max_pages=max_pages,
            after=after,
        )

    # ------------------------ Asociaciones ------------------------

    def get_associations(
        self,
        from_object_type: str,
        from_object_id: str,
        to_object_type: str,
        limit: int = 100,
        after: t.Optional[str] = None,
    ) -> t.List[dict]:
        """
        Obtiene asociaciones entre objetos (v4).
        GET /crm/v4/objects/{from_object_type}/{from_object_id}/associations/{to_object_type}
        """
        path = f"/crm/v4/objects/{from_object_type}/{from_object_id}/associations/{to_object_type}"
        params = {"limit": limit}
        if after:
            params["after"] = after

        all_results: t.List[dict] = []
        while True:
            res = self._request("GET", path, params=params)
            results = res.get("results", [])
            all_results.extend(results)
            next_after = res.get("paging", {}).get("next", {}).get("after")
            if not next_after:
                break
            params["after"] = next_after
        return all_results

    # ------------------------ Transformaciones a DataFrame ------------------------

    def to_dataframe(
        self,
        records: t.List[dict],
        properties: t.Optional[t.List[str]] = None,
        include_system_cols: bool = True,
    ) -> pd.DataFrame:
        """
        Convierte 'results' de HubSpot a DataFrame plano.
        ARGS
        ----
        records : list[dict]
            Lista de dicts provenientes de v3 objects o search.
        properties : list[str] | None
            Si se especifica, intentará proyectar solo esas columnas (si existen).
        include_system_cols : bool
            Si True, incluye columnas 'id', 'archived', 'createdAt', 'updatedAt'.

        RETURNS
        -------
        pd.DataFrame
        """
        rows = []
        for item in records:
            props = item.get("properties", {}) or {}
            row = dict(props)

            if include_system_cols:
                row["id"] = item.get("id")
                row["archived"] = item.get("archived")
                row["createdAt"] = item.get("createdAt")
                row["updatedAt"] = item.get("updatedAt")

            rows.append(row)

        df = pd.DataFrame(rows)

        if properties:
            # Garantiza que columnas solicitadas existan; ignora faltantes
            base_cols = ["id", "archived", "createdAt", "updatedAt"] if include_system_cols else []
            desired = [c for c in (properties + base_cols) if c in df.columns]
            if desired:
                df = df[desired]

        return df

    # ------------------------ Sanity check / conexión ------------------------

    def test_connection(self) -> bool:
        """
        Verifica la conexión mínima intentando listar 1 contacto.
        RETURNS
        -------
        bool : True si responde sin error.
        """
        try:
            res = self._request("GET", "/crm/v3/objects/contacts", params={"limit": 1})
            ok = isinstance(res, dict) and "results" in res
            self._v("Conexión verificada contra /contacts (limit=1).", level="debug")
            return ok
        except Exception as ex:
            self._v(f"Fallo test_connection: {ex}", level="error")
            return False


# ------------------------ Ejemplos de uso (comentados) ------------------------
# from mypackage.hubspot_api import HubSpot_API
#
# logger = Verbose(active=True, alerts_enabled=False)  # si tienes tu clase Verbose
# hs = HubSpot_API(token=os.getenv("HUBSPOT_TOKEN"), verbose_logger=logger)
#
# # 1) Test de conexión
# assert hs.test_connection() is True
#
# # 2) Listar contactos (solo algunas propiedades)
# contacts = hs.get_contacts(properties=["email", "firstname", "lastname"], limit=100, max_pages=5)
# df_contacts = hs.to_dataframe(contacts, properties=["email", "firstname", "lastname"])
#
# # 3) Buscar deals modificados desde una fecha
# since_date = dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)
# deals = hs.search_objects_updated_since(
#     object_type="deals",
#     since=since_date,
#     properties=["dealname", "amount", "dealstage", "pipeline"],
#     limit_per_page=100,
# )
# df_deals = hs.to_dataframe(deals, properties=["dealname", "amount", "dealstage", "pipeline"])
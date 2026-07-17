import logging
from datetime import UTC, datetime
from typing import Optional

import serpapi


class SerpApiClient:
    def __init__(
        self, api_key: Optional[str] = None, verbose_logger: Optional[object] = True
    ) -> None:
        self.api_key = api_key if api_key else None
        self.verbose = verbose_logger or self._build_default_logger()
        self.verbose.log(
            "--- EXECUTING SerpApiClient Class v1.0 "
            f"- Initialized at {datetime.now(UTC).isoformat()} ---"
        )

        if api_key:
            self._check_api_key(self.api_key)
            self.verbose.log("SerpApi instantiated with API key provided")
        else:
            self.verbose.log(
                "SerpApiClient has no API key, to retrieve data must provide API Key"
            )

    @staticmethod
    def _build_default_logger() -> object:
        """Build a stdlib-based fallback logger with .log() and .critical()."""
        logger = logging.getLogger("SerpApiClient Class")
        if not logger.handlers:
            logging.basicConfig(level=logging.INFO, format="%(message)s")

        class _Adapter:
            def log(self, message: str) -> None:
                logger.info(message)

            def critical(self, message: str) -> None:
                logger.error(message)

        return _Adapter()

    def _check_api_key(self, api_key):
        """
        Some string
        """
        params = {"api_key": api_key}

        try:
            account_data = serpapi.account(params=params)
            self.verbose.log("¡Valid key!")
            self.verbose.log(f"Queries left: {account_data.get('plan_searches_left')}")
        except Exception as e:
            print(f"Validation error for provided API key: {e}")


# def extraer_dominio(url):
#     if not url or url == "N/A":
#         return None
#     try:
#         parsed = urlparse(url)
#         domain = parsed.netloc.lower()
#         if domain.startswith("www."):
#             domain = domain[4:]
#         return domain
#     except Exception:
#         return None


# def es_dominio_cliente(dominio, cliente_dominio):
#     """
#     Verifica si el dominio coincide con el cliente.
#     """
#     if not dominio or not cliente_dominio:
#         return False
#     # Estrategia simple: si el string del dominio cliente está dentro del dominio encontrado
#     # Ej: 'claro.cl' en 'tienda.claro.cl' -> True
#     return (
#         cliente_dominio in dominio
#         or cliente_dominio.split(".")[0] in dominio.split(".")[0]
#     )


# def extraer_top_n_dominios(organic_results, n=5):
#     dominios = []
#     for i in range(n):
#         if i < len(organic_results):
#             link = organic_results[i].get("link", "N/A")
#             dominio = extraer_dominio(link) or "N/A"
#             dominios.append(dominio)
#         else:
#             dominios.append("N/A")
#     return dominios


# def search_google(query, api_key, config, verbose_logger):
#     """
#     Usa config['country_code'] y config['language'] dinámicos.
#     """
#     verbose_logger.log(f"Buscando: '{query}' ({config['country_code']})")
#     params = {
#         "engine": "google",
#         "q": query,
#         "num": 10,
#         "api_key": api_key,
#         "gl": config["country_code"],
#         "hl": config["language"],
#     }
#     try:
#         search = GoogleSearch(params)
#         resultados = search.get_dict()
#         if "error" in resultados:
#             raise serpapi.serp_api_client.SerpApiClientException(resultados["error"])
#         return resultados
#     except Exception as e:
#         verbose_logger.critical(f"Error SerpAPI '{query}': {e}")
#         # Retornamos dict vacío para no romper el flujo masivo, o re-lanzamos según preferencia
#         return {}


# def analizar_con_pandas(keyword, row_data, cliente_dominio, competidores_detectados):
#     # Lógica idéntica a la original, solo asegurando que cliente_dominio viene por param
#     client_rank = None
#     for i in range(1, 6):
#         dominio = row_data.get(f"organic_pos_{i}", "N/A")
#         if dominio != "N/A" and es_dominio_cliente(dominio, cliente_dominio):
#             client_rank = i
#             break

#     client_in_snippet = False
#     client_appearances = 0
#     for i in range(1, 6):
#         ref = row_data.get(f"snippet_ref_{i}", "N/A")
#         if ref != "N/A" and es_dominio_cliente(ref, cliente_dominio):
#             client_in_snippet = True
#             client_appearances += 1

#     analysis_parts = []
#     if client_rank:
#         if client_rank == 1:
#             analysis_parts.append(f"Dominando pos #{client_rank}")
#         elif client_rank <= 3:
#             analysis_parts.append(f"Top 3 (Pos #{client_rank})")
#         else:
#             analysis_parts.append(f"Posición #{client_rank}")
#     else:
#         analysis_parts.append("Fuera del Top 5")

#     if client_in_snippet:
#         analysis_parts.append(f"En AI Overview ({client_appearances}x)")
#     elif row_data.get("has_ai_overview") == "Sí":
#         analysis_parts.append("AI Overview activo (sin cliente)")

#     # Competencia
#     comps = []
#     for i in range(1, 4):
#         d = row_data.get(f"organic_pos_{i}", "N/A")
#         if d != "N/A" and d in competidores_detectados:
#             comps.append(f"{d}(#{i})")
#     if comps:
#         analysis_parts.append(f"Comp: {', '.join(comps)}")

#     return {
#         "client_rank_organic": client_rank,
#         "client_in_snippet": client_in_snippet,
#         "client_snippet_appearances": client_appearances,
#     }


# def inferir_competidores(all_data_to_write, cliente_dominio, top_n=5):
#     excluir = [
#         "wikipedia.org",
#         "youtube.com",
#         "facebook.com",
#         "instagram.com",
#         "twitter.com",
#         "linkedin.com",
#     ]
#     headers = all_data_to_write[0]
#     dominio_counter = Counter()

#     for i in range(1, len(all_data_to_write)):
#         row = all_data_to_write[i]
#         # Mapeo seguro por si el largo de la fila varía
#         row_dict = {headers[j]: row[j] for j in range(len(row)) if j < len(headers)}

#         for k in range(1, 6):
#             d = row_dict.get(f"organic_pos_{k}", "N/A")
#             if d and d != "N/A" and not es_dominio_cliente(d, cliente_dominio):
#                 if not any(ex in d for ex in excluir):
#                     dominio_counter[d] += 1

#     return [d for d, _ in dominio_counter.most_common(top_n)]


# def analizar_y_enriquecer_programaticamente(all_data_to_write, cliente_dominio):
#     # Ya no tiene valor por defecto 'claro.com.co'
#     competidores = inferir_competidores(all_data_to_write, cliente_dominio)
#     headers = all_data_to_write[0]

#     idx_rank = headers.index("client_rank_organic")
#     idx_snip = headers.index("client_in_snippet")
#     idx_apps = headers.index("client_snippet_appearances")
#     idx_anal = headers.index("ai_analysis")
#     idx_kw = headers.index("keyword")

#     for i in range(1, len(all_data_to_write)):
#         row = all_data_to_write[i]
#         row_dict = {headers[j]: row[j] for j in range(len(row))}

#         analisis = analizar_con_pandas(
#             row_dict[headers[idx_kw]], row_dict, cliente_dominio, competidores
#         )

#         row[idx_rank] = (
#             str(analisis["client_rank_organic"])
#             if analisis["client_rank_organic"]
#             else "N/A"
#         )
#         row[idx_snip] = "Sí" if analisis["client_in_snippet"] else "No"
#         row[idx_apps] = str(analisis["client_snippet_appearances"])
#         row[idx_anal] = analisis["ai_analysis"]

#     return all_data_to_write

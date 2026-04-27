import json
import requests
import pandas as pd
from pathlib import Path

from d2b_data.verbose_logger import Verbose


VERSION = 'v21.0'
BASE_URL = f"https://graph.facebook.com/{VERSION}"


class Facebook_Organic:
    """
    Client for the Facebook Graph API focused on organic page content.

    Handles authentication via a local cache file so that page tokens are
    only fetched once. Once authenticated, provides methods to list pages
    and retrieve post-level KPIs (reactions, comments, shares).

    Note: This class is for **organic** data only. For paid/ads data use
    ``Facebook_Marketing`` instead.

    Args:
        user_token (str): Facebook user access token with ``pages_read_engagement``
            and ``pages_show_list`` permissions at minimum.
        cache_path (str | Path): Path to the JSON cache file that stores page
            tokens. Defaults to ``'debug_tokens.json'`` in the current directory.
        verbose (bool): Whether to print log messages. Defaults to ``False``.

    Example::

        fb = Facebook_Organic(user_token="EAAxxxxx", verbose=True)
        fb.authenticate()
        print(fb.list_accounts())
        df = fb.kpis("My Page Name", limit=50)
    """

    def __init__(self, user_token: str, cache_path: str | Path = 'debug_tokens.json', verbose: bool = False) -> None:
        self.user_token = user_token
        self.cache_path = Path(cache_path)
        self.pages: list[dict] = []

        self.verbose = Verbose(
            active=verbose,
            alerts_enabled=False,
            workflow_name="Facebook_Organic"
        )

        if self.cache_path.exists():
            self.pages = self._load_pages()
            self.verbose.log(f"__init__ | {len(self.pages)} pages loaded from cache.")
        else:
            self.verbose.log("__init__ | No cache found. Call .authenticate() to generate the file.")

    # ------------------------------------------------------------------
    # Authentication and cache
    # ------------------------------------------------------------------

    def authenticate(self) -> None:
        """
        Fetches user permissions and accessible page tokens from the Graph API
        and writes them to the local cache file.

        If the cache file already exists this method is a no-op, so it is safe
        to call on every run. Delete the cache file manually to force a refresh.

        Returns:
            None

        Raises:
            requests.exceptions.RequestException: If the HTTP request to the
                Graph API fails due to a network error.
        """
        if self.cache_path.exists():
            self.verbose.log(f"authenticate | Cache already exists at '{self.cache_path}'. Skipping authentication.")
            return

        debug_data = {"user_permissions": [], "pages": []}

        self.verbose.log("authenticate | Fetching user permissions...")
        res = requests.get(f"{BASE_URL}/me/permissions", params={'access_token': self.user_token})
        debug_data["user_permissions"] = res.json().get('data', [])

        self.verbose.log("authenticate | Fetching accessible pages...")
        res = requests.get(f"{BASE_URL}/me/accounts", params={'access_token': self.user_token})
        pages_data = res.json()

        if 'data' not in pages_data:
            self.verbose.log(f"authenticate | No pages found. Response: {pages_data}")
            return

        for page in pages_data['data']:
            debug_data["pages"].append({
                "name":       page.get('name'),
                "id":         page.get('id'),
                "category":   page.get('category'),
                "tasks":      page.get('tasks'),
                "page_token": page.get('access_token')
            })

        self.cache_path.write_text(json.dumps(debug_data, indent=4, ensure_ascii=False), encoding='utf-8')
        self.pages = debug_data["pages"]
        self.verbose.log(f"authenticate | {len(self.pages)} pages saved to '{self.cache_path}'.")

    def _load_pages(self) -> list[dict]:
        """
        Reads the page list from the local cache file.

        Returns:
            list[dict]: List of page records, each containing ``name``, ``id``,
                ``category``, ``tasks``, and ``page_token``.
        """
        datos = json.loads(self.cache_path.read_text(encoding='utf-8'))
        lista = datos.get('pages', [])
        self.verbose.log(f"_load_pages | {len(lista)} pages found in file.")
        return lista

    # ------------------------------------------------------------------
    # List accounts
    # ------------------------------------------------------------------

    def list_accounts(self) -> pd.DataFrame:
        """
        Returns a summary of all accessible pages (excluding tokens).

        Returns:
            pd.DataFrame: DataFrame with columns ``name``, ``id``, ``category``,
                and ``tasks``. Returns an empty DataFrame if no pages are loaded.
        """
        if not self.pages:
            self.verbose.log("list_accounts | No pages available. Call .authenticate() first.")
            return pd.DataFrame()

        self.verbose.log(f"list_accounts | Returning {len(self.pages)} pages.")
        return pd.DataFrame([
            {"name": p["name"], "id": p["id"], "category": p["category"], "tasks": p["tasks"]}
            for p in self.pages
        ])

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_page(self, identifier: str) -> dict | None:
        """
        Looks up a page by name (case-insensitive) or numeric page ID.

        Args:
            identifier (str): Page name or page ID string.

        Returns:
            dict | None: Page record with ``name``, ``id``, ``category``,
                ``tasks``, and ``page_token``, or ``None`` if not found.
        """
        for p in self.pages:
            if p["name"].lower() == identifier.lower() or p["id"] == identifier:
                self.verbose.log(f"_get_page | Page found: '{p['name']}' (id: {p['id']})")
                return p
        self.verbose.log(f"_get_page | Page '{identifier}' not found.")
        return None

    def _get_posts(self, page: dict, limit: int = 100) -> list[dict]:
        """
        Downloads published posts from a page using pagination.

        Tries the ``published_posts`` endpoint first and falls back to ``feed``
        if no results are returned. Each post includes ``id``, ``message``,
        ``created_time``, ``shares``, ``full_picture``, and summary counts for
        comments and reactions.

        Args:
            page (dict): Page record as returned by ``_get_page()``.
            limit (int): Maximum number of posts to retrieve. Defaults to 100.

        Returns:
            list[dict]: List of raw post objects from the Graph API, up to
                ``limit`` entries. Returns an empty list if no posts are found.
        """
        fields = ','.join([
            'id',
            'message',
            'created_time',
            'shares',
            'full_picture',
            'comments.limit(100).summary(total_count)',
            'reactions.limit(100).summary(total_count)',
        ])
        params = {'access_token': page["page_token"], 'fields': fields, 'limit': min(limit, 25)}

        self.verbose.log(f"_get_posts | Downloading up to {limit} posts from page '{page['name']}'...")

        for endpoint in ['published_posts', 'feed']:
            posts: list[dict] = []
            url = f"{BASE_URL}/{page['id']}/{endpoint}"
            req_params = params.copy()

            while url and len(posts) < limit:
                res = requests.get(url, params=req_params)
                raw = res.json()
                batch = raw.get('data', [])
                if not batch:
                    break
                posts.extend(batch)
                self.verbose.log(f"_get_posts | {len(posts)} posts accumulated via '{endpoint}'")
                url = raw.get('paging', {}).get('next')
                req_params = {}

            if posts:
                self.verbose.log(f"_get_posts | {len(posts[:limit])} total posts via '{endpoint}'.")
                return posts[:limit]

        self.verbose.log("_get_posts | No posts retrieved from any endpoint.")
        return []

    def _get_reactions(self, page: dict, post_id: str) -> int:
        """
        Fetches the total reaction count for a single post.

        Args:
            page (dict): Page record as returned by ``_get_page()``, used for
                its ``page_token``.
            post_id (str): Fully-qualified post ID (e.g. ``'123456_789012'``).

        Returns:
            int: Total number of reactions. Returns ``0`` if the API response
                does not include a summary.
        """
        res = requests.get(
            f"{BASE_URL}/{post_id}/reactions",
            params={'access_token': page["page_token"], 'limit': 0, 'summary': 'total_count'}
        )
        raw = res.json()
        self.verbose.log(f"_get_reactions | raw: {raw}")
        return raw.get("summary", {}).get("total_count", 0)

    # ------------------------------------------------------------------
    # KPIs per page
    # ------------------------------------------------------------------

    def kpis(self, identifier: str, limit: int = 100) -> pd.DataFrame:
        """
        Downloads and aggregates organic KPIs for each post on a given page.

        For every post, fetches reactions (via a dedicated endpoint), comments,
        and shares, then computes a combined ``engagement`` metric.

        Args:
            identifier (str): Page name (case-insensitive) or page ID.
            limit (int): Maximum number of posts to retrieve. Defaults to 100.

        Returns:
            pd.DataFrame: One row per post, sorted by ``fecha`` descending, with
                the following columns:

                - ``post_id`` (str): Unique post identifier.
                - ``fecha`` (datetime): Publication timestamp (UTC).
                - ``mensaje`` (str): First 80 characters of the post text.
                - ``reacciones`` (int): Total reactions count.
                - ``comentarios`` (int): Total comments count.
                - ``compartidos`` (int): Total shares count.
                - ``engagement`` (int): Sum of reactions + comments + shares.

                Returns an empty DataFrame if the page is not found or has no posts.
        """
        self.verbose.log(f"kpis | Starting KPI download for '{identifier}'...")

        page = self._get_page(identifier)
        if not page:
            return pd.DataFrame()

        posts = self._get_posts(page, limit=limit)
        if not posts:
            self.verbose.log(f"kpis | No posts found for '{identifier}'.")
            return pd.DataFrame()

        rows = []
        for post in posts:
            reacciones = self._get_reactions(page, post.get("id"))
            row = {
                "post_id":     post.get("id"),
                "fecha":       post.get("created_time"),
                "mensaje":     post.get("message", "")[:80],
                "reacciones":  reacciones,
                "comentarios": post.get("comments", {}).get("summary", {}).get("total_count", 0),
                "compartidos": post.get("shares", {}).get("count", 0),
            }
            rows.append(row)
            self.verbose.log(
                f"kpis | [{row['fecha'][:10]}] "
                f"reactions:{row['reacciones']} comments:{row['comentarios']} shares:{row['compartidos']} "
                f"| {row['mensaje'][:50] or '(no text)'}"
            )

        df = pd.DataFrame(rows)
        df["fecha"] = pd.to_datetime(df["fecha"])
        df["engagement"] = df["reacciones"] + df["comentarios"] + df["compartidos"]
        df = df.sort_values("fecha", ascending=False).reset_index(drop=True)
        return df

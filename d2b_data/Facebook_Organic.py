import json
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

from d2b_data.verbose_logger import Verbose


class Facebook:
    def __init__(self, user_token: str, cache_path: str | Path = 'debug_tokens.json', verbose: bool = False):
        self.user_token = user_token
        self.cache_path = Path(cache_path)
        self.verbose = verbose
        self.logger = Verbose(active=verbose, alerts_enabled=False, workflow_name="Facebook")
        self.pages: list[dict] = []
        self.VERSION = 'v21.0'
        self.BASE_URL = f"https://graph.facebook.com/{self.VERSION}"


        if self.cache_path.exists():
            self.pages = self._load_pages()
            self._log("__init__", f"{len(self.pages)} pages loaded from cache.")
        else:
            self._log("__init__", "No cache found. Call .authenticate() to generate the file.")


    def _log(self, funcion: str, detalle: str) -> None:
        self.logger.log(f"Facebook | {funcion} | {detalle}")

    def authenticate(self) -> None:
        if self.cache_path.exists():
            self._log("authenticate", f"Cache already exists at '{self.cache_path}'. Skipping authentication.")
            return

        debug_data = {"user_permissions": [], "pages": []}

        self._log("authenticate", "Fetching user permissions...")
        res = requests.get(f"{self.BASE_URL}/me/permissions", params={'access_token': self.user_token})
        debug_data["user_permissions"] = res.json().get('data', [])

        self._log("authenticate", "Fetching accessible pages...")
        res = requests.get(f"{self.BASE_URL}/me/accounts", params={'access_token': self.user_token})
        pages_data = res.json()

        if 'data' not in pages_data:
            self._log("authenticate", f"No pages found. Response: {pages_data}")
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
        self._log("authenticate", f"{len(self.pages)} pages saved to '{self.cache_path}'.")

    def _load_pages(self) -> list[dict]:
        # Read pages list from the cached JSON file
        datos = json.loads(self.cache_path.read_text(encoding='utf-8'))
        lista = datos.get('pages', [])
        self._log("_load_pages", f"{len(lista)} pages found in file.")
        return lista
    
    def get_page_by_id(self, account_id):
        for page in self.pages:
          if page.get("id") == account_id:
            return page
        return None

    # ------------------------------------------------------------------
    # List accounts
    # ------------------------------------------------------------------

    def list_accounts(self) -> pd.DataFrame:
        if not self.pages:
            self._log("list_accounts", "No pages available. Call .authenticate() first.")
            return pd.DataFrame()

        self._log("list_accounts", f"Returning {len(self.pages)} pages.")
        return pd.DataFrame([
            {"name": p["name"], "id": p["id"], "category": p["category"], "tasks": p["tasks"]}
            for p in self.pages
        ])

    def get_posts(self, account_id: str, limit: int = 100) -> list[dict]:
        self._log("get_posts", "Start download")
        fields = ','.join([
            'id',
            'message',
            'created_time',
            'shares',
            'viewer_reaction',
            'full_picture',
            'comments.limit(100).summary(total_count)',
            'reactions.limit(100).summary(total_count)'
        ])
        page = self.get_page_by_id(account_id)
        if not page:
            return []

        params = {'access_token': page.get("page_token"), 'fields': fields, 'limit': min(limit, 25)}
        self._log("get_posts", f"Downloading up to {limit} posts from page ID '{account_id}'...")
        
        for endpoint in ['published_posts', 'feed']:
            posts: list[dict] = []
            url = f"{self.BASE_URL}/{account_id}/{endpoint}"
            req_params = params.copy()

            while url and len(posts) < limit:
                res = requests.get(url, params=req_params)
                raw = res.json()
                self._log("get_posts", f"raw: {raw}")
                batch = raw.get('data', [])
                if not batch:
                    break
                posts.extend(batch)
                self._log("get_posts", f"  → {len(posts)} posts acumulados via '{endpoint}'")
                url = raw.get('paging', {}).get('next')
                req_params = {}

            if posts:
                self._log("get_posts", f"{len(posts[:limit])} posts totales via '{endpoint}'.")
                return posts[:limit]

        self._log("get_posts", "No posts retrieved from any endpoint.")
        return []


    # ------------------------------------------------------------------
    # KPIs per brand
    # ------------------------------------------------------------------

    def get_posts_summary(self, account_id: str, limit: int = 100) -> pd.DataFrame:
        self._log("kpis", f"Starting KPI download for '{account_id}'...")

        posts = self.get_posts(account_id, limit=limit)
        if not posts:
            self._log("kpis", f"No posts found for '{account_id}'.")

            return pd.DataFrame()

        rows = []
        for post in posts:
            reacciones = self._get_reactions(account_id, post.get("id"))
            row = {
                "post_id":     post.get("id"),
                "fecha":       post.get("created_time"),
                "mensaje":     post.get("message", "")[:80],
                "reacciones":  reacciones,
                "comentarios": post.get("comments", {}).get("summary", {}).get("total_count", 0),
                "compartidos": post.get("shares", {}).get("count", 0),
            }
            rows.append(row)
            self._log("kpis", f"  [{row['fecha'][:10]}] 👍{row['reacciones']} 💬{row['comentarios']} 🔁{row['compartidos']} | {row['mensaje'][:50] or '(sin texto)'}")

        df = pd.DataFrame(rows)
        df["fecha"] = pd.to_datetime(df["fecha"])
        df["engagement"] = df["reacciones"] + df["comentarios"] + df["compartidos"]
        df = df.sort_values("fecha", ascending=False).reset_index(drop=True)
        return df


    def _get_reactions(self, account_id: str, post_id: str) -> int:
        page = self.get_page_by_id(account_id)
        if not page:
            return 0
        res = requests.get(
            f"{self.BASE_URL}/{post_id}/reactions",
            params={'access_token': page.get("page_token"), 'limit': 0, 'summary': 'total_count'}
        )
        raw = res.json()
        self._log("_get_reactions", f"raw: {raw}")
        return raw.get("summary", {}).get("total_count", 0)


    def get_current_status(self, account_id: str) -> pd.DataFrame:
        page = self.get_page_by_id(account_id)
        if not page:
            return pd.DataFrame()

        fields = ("name," 
                  "fan_count,"  
                  "followers_count,"  
                  "talking_about_count,"  
                  "rating_count,"
                  "overall_star_rating,"
                  "verification_status"
                 )
        params = {
            'access_token': page.get("page_token"),
            'fields': fields
        }

        req = requests.get(f'{self.BASE_URL}/{account_id}', params=params)
        raw = req.json()
        self._log("get_current_status", str(req))
        return pd.DataFrame([raw])
      
    def query_stat(self, account_id: str, metric: str = "page_daily_follows", since: str = None, until: str = None, period: str = "day") -> pd.DataFrame:
        page = self.get_page_by_id(account_id)
        if not page:
            return pd.DataFrame()

        if until is None:
            until = datetime.now().strftime('%Y-%m-%d')
        if since is None:
            since = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')

        dt_since = datetime.strptime(since, '%Y-%m-%d')
        dt_until = datetime.strptime(until, '%Y-%m-%d')
        
        all_values = []
        current_since = dt_since
        
        while current_since <= dt_until:
            current_until = min(current_since + timedelta(days=89), dt_until)
            
            params = {'access_token': page.get("page_token"),
                      'metric'      :  metric,
                      'since'       :  current_since.strftime('%Y-%m-%d'),
                      'until'       :  current_until.strftime('%Y-%m-%d'),
                      'period'      :  period
                     }
          
            req = requests.get(f'{self.BASE_URL}/{account_id}/insights', params=params)
            self._log("query_stat", f"req: {req} ({current_since.strftime('%Y-%m-%d')} to {current_until.strftime('%Y-%m-%d')})")
            
            raw = req.json()
            data = raw.get('data', [])
            
            if data:
                for metric_data in data:
                    metric_name = metric_data.get('name', metric)
                    for val in metric_data.get('values', []):
                        if isinstance(val, dict):
                            all_values.append({
                                'metric': metric_name,
                                'value': val.get('value'),
                                'end_time': val.get('end_time')
                            })
            elif 'error' in raw:
                self._log("query_stat", f"API Error: {raw.get('error')}")
                break
                
            current_since = current_until + timedelta(days=1)
            
        df = pd.DataFrame(all_values)
        if not df.empty and 'end_time' in df.columns:
            df['end_time'] = pd.to_datetime(df['end_time'])
            df['date'] = df['end_time'].dt.date
            
            df = df.pivot_table(
                index='date', 
                columns='metric', 
                values='value', 
                aggfunc='first'
            ).reset_index()
            df.columns.name = None
            
        return df


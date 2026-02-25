from datetime import datetime, timezone, timedelta, time
from zoneinfo import ZoneInfo
from typing import Optional

class UTCConverter:
    """Clase auxiliar para conversión de fechas a UTC con soporte multiregión y modos start/end"""

    REGIONS = {
        "chile": "America/Santiago",
        "brasil": "America/Sao_Paulo",
        "argentina": "America/Buenos_Aires",
        "peru": "America/Lima",
        "colombia": "America/Bogota",
        "uruguay": "America/Montevideo",
        "mexico": "America/Mexico_City"
    }

    @staticmethod
    def get_now(region: str = "chile") -> str:
        """Obtiene el momento actual de la región y lo convierte a UTC string ISO"""
        tz_name = UTCConverter.REGIONS.get(region.lower(), "America/Santiago")
        dt_local = datetime.now(ZoneInfo(tz_name))
        return dt_local.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

    @staticmethod
    def get_yesterday(region: str = "chile") -> str:
        """Retorna la fecha de ayer de la región como string simple (YYYY-MM-DD)"""
        tz_name = UTCConverter.REGIONS.get(region.lower(), "America/Santiago")
        tz = ZoneInfo(tz_name)
        yesterday_date = datetime.now(tz).date() - timedelta(days=1)
        return yesterday_date.strftime('%Y-%m-%d')

    @staticmethod
    def convert(date_str: str, region: str = "chile", mode: Optional[str] = None) -> str:
        """
        Convierte un string de fecha a UTC.
        - Soporta ISO format de API ('2026-02-02T14:35:15-03:00') o fecha simple ('2025-01-01').
        - mode='start' -> fuerza 00:00:00 local del país.
        - mode='end' -> fuerza 23:59:59 local del país.
        """
        tz_name = UTCConverter.REGIONS.get(region.lower(), "America/Santiago")
        tz = ZoneInfo(tz_name)

        try:
            # 1. Parseo inicial
            if "T" in date_str:
                dt = datetime.fromisoformat(date_str)
            else:
                dt_naive = datetime.strptime(date_str, '%Y-%m-%d')
                dt = dt_naive.replace(tzinfo=tz)

            # 2. Aplicar modificadores de hora sobre la fecha local
            if mode == "start":
                dt = datetime.combine(dt.date(), time(0, 0, 0), tzinfo=tz)
            elif mode == "end":
                dt = datetime.combine(dt.date(), time(23, 59, 59), tzinfo=tz)

            # 3. Conversión final a UTC
            return dt.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        except ValueError as e:
            return f"Error de formato en fecha '{date_str}': {e}"
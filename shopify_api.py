import time as sleeptime
import requests
import pandas as pd
from typing import Optional, List, Dict
from utc_converter import UTCConverter

class ShopifyAPI:
    """Client to interact with Shopify API"""
    def __init__(self, shop_name: str, access_token: str, api_version: str = "2024-01", verbose: bool = False):
        """
        Initializes Shopify client
        """
        self.shop_name = shop_name
        self.access_token = access_token
        self.api_version = api_version
        self.base_url = f"https://{shop_name}.myshopify.com/admin/api/{api_version}"
        self.is_verbose = verbose
        self.headers = {
            "X-Shopify-Access-Token": access_token,
            "Content-Type": "application/json",
        }
        # Instanciamos la clase auxiliar
        self.utc_converter = UTCConverter()

    def verbose(self, *args, **kwargs):
        """Helper para imprimir solo si verbose es True"""
        if self.is_verbose:
            print(*args, **kwargs)

    def get_orders(self, date_start: Optional[str] = None, date_end: Optional[str] = None, status: str = "any", limit: int = 250) -> Optional[List[Dict]]:
            """
            Trae todas las órdenes dentro de un rango usando la navegación de Link
            """
            all_orders = []
            loop_count = 1
            # 1. Initial URL and Parameters
            url = f"{self.base_url}/orders.json"
            params = {
                "status": status,
                "limit": min(limit, 250),
                "order": "created_at asc",
            }
            if date_start:
                #params["created_at_min"] = self.utc_converter.convert_to_utc(date_start)
                params["created_at_min"] = date_start
            if date_end:
                #params["created_at_max"] = self.utc_converter.convert_to_utc(date_end)
                params["created_at_max"] = date_end

            if date_start:
                #params["created_at_min"] = self.utc_converter.convert_to_utc(date_start)
                params["created_at_min"] = date_start
            if date_end:
                #params["created_at_max"] = self.utc_converter.convert_to_utc(date_end)
                params["created_at_max"] = date_end



            while url:
                self.verbose(f"SHOPIFY | LOOP {loop_count} | Requesting: {url}")

                # 2. Make the request
                # For the first call, we send params.
                # For subsequent calls, Shopify puts everything into the 'next' URL.
                #self.verbose(f"{params}")
                response = requests.get(
                    url,
                    headers=self.headers,
                    params=params if loop_count == 1 else None
                )

                try:
                    response.raise_for_status()
                except requests.exceptions.HTTPError as e:
                    print(f"Error HTTP: {e} | {response.text}")
                    return None

                data = response.json()
                orders = data.get("orders", [])
                all_orders.extend(orders)

                self.verbose(f"SHOPIFY | Found {len(orders)} orders. Total: {len(all_orders)}")

                # 3. Handle Pagination via the 'Link' Header
                # 'requests' parses this automatically into the .links attribute
                links = response.links
                if 'next' in links:
                    url = links['next']['url']
                    loop_count += 1

                else:
                    self.verbose("SHOPIFY | No more pages (no 'next' link found).")
                    url = None

            return all_orders


    def orders_to_dataframe(self, orders: List[Dict], refunds_input = False) -> pd.DataFrame:
        """
        Converts order list to pandas DataFrame matching Shopify's sales report format
        """
        rows = []

        for orden in orders:
            gross_sales = float(orden.get('total_line_items_price', 0))
            discounts = float(orden.get('total_discounts', 0))

            returns = 0.0
            shipping_refunds = 0.0

            refunds_list = orden.get('refunds', [])
            for refund in refunds_list:
                # 1. Productos devueltos: Subtotal - Tax = Valor Neto (8,669.42)
                for refund_line_item in refund.get('refund_line_items', []):
                    subtotal = float(refund_line_item.get('subtotal', 0))
                    tax_on_refund = float(refund_line_item.get('total_tax', 0))
                    returns += (subtotal - tax_on_refund)

                # 2. Ajustes de envío (se manejan aparte para no ensuciar 'returns')
                for adjustment in refund.get('order_adjustments', []):
                    adj_amount = float(adjustment.get('amount', 0))
                    if adjustment.get('kind') == 'shipping_refund':
                        # Usamos abs() porque Shopify lo manda negativo (-3156.20)
                        shipping_refunds += abs(adj_amount)
                    else:
                        # Otros ajustes manuales
                        returns += abs(adj_amount)



            gross_sales = float(orden.get('total_line_items_price', 0))

            taxes = float(orden.get('total_tax', 0))

            shipping_set = orden.get('total_shipping_price_set')
            shipping_charges = float(shipping_set.get('shop_money', {}).get('amount', 0)) if shipping_set else 0.0

            additional_fees_set = orden.get('current_total_additional_fees_set')
            additional_fees = float(additional_fees_set.get('shop_money', {}).get('amount', 0)) if additional_fees_set else 0.0

            duties_set = orden.get('current_total_duties_set')
            duties = float(duties_set.get('shop_money', {}).get('amount', 0)) if duties_set else 0.0

            total_sales = float(orden.get('total_price', 0))
            custom_total_sales = total_sales - returns

            fulfillment = orden.get('fulfillment_status')
            estado_envio = "Pending Shipping" if fulfillment is None else fulfillment.capitalize()
            esta_cerrada = "Closed" if orden.get('closed_at') else "Open"
            net_sales = additional_fees + duties + shipping_charges + taxes
            cancel_reason = orden.get('cancel_reason', "")
            if not refunds_input:
              date= orden.get('created_at')
            else:
              date= orden.get('updated_at')
              total_sales = total_sales*-1

            rows.append({
                "orders": orden.get('name'),
                "order_id": orden.get('id'),
                "order_number": orden.get('order_number'),
                "date" : date,
                "estado_ciclo": esta_cerrada,
                "estado_envio": estado_envio,
                "financial_status": orden.get('financial_status'),
                "gross_sales": gross_sales,
                "discounts": discounts,
                "returns": returns,
                "net_sales": net_sales,
                "shipping_charges": shipping_charges,
                "duties": duties,
                "additional_fees": additional_fees,
                "taxes": taxes,
                "total_sales": total_sales,
                "custom_total_sales": custom_total_sales,
                "currency": orden.get('currency'),
                "customer_email": orden.get('email'),
                "cancel_reason"  : cancel_reason
            })

        df = pd.DataFrame(rows)

        if not df.empty and 'fecha' in df.columns:
            df['fecha'] = pd.to_datetime(df['fecha'])

        return df

    def get_orders_as_df(self, date_start: Optional[str] = None, date_end: Optional[str] = None, status: str = "any", limit: int = 250) -> pd.DataFrame:
        """Obtiene pedidos y los devuelve directamente como un DataFrame"""

        orders = self.get_orders(
            date_start=date_start,
            date_end=date_end,
            status=status,
            limit=limit
        )

        if orders is None:
            self.verbose("SHOPIFY | No se pudieron obtener pedidos, devolviendo DataFrame vacío.")
            return pd.DataFrame()

        self.verbose(f"SHOPIFY | Convirtiendo {len(orders)} pedidos a DataFrame...")
        return self.orders_to_dataframe(orders)

    def get_refunds(self, date_start: Optional[str] = None, date_end: Optional[str] = None, limit: int = 250) -> Optional[List[Dict]]:
      """
      Obtiene todas las órdenes que fueron reembolsadas dentro de un rango de fechas
      usando el filtro 'updated_at' y verificando el objeto 'refunds'.
      """
      self.verbose("SHOPIFY | Refunds")
      all_orders_with_refunds = []
      loop_count = 1

      # 1. URL Inicial y Parámetros
      # Usamos /orders.json pero con estados financieros específicos
      url = f"{self.base_url}/orders.json"

      params = {
          "status": "any", # Queremos ver órdenes abiertas, cerradas o canceladas
          "financial_status": "refunded", # Filtro clave para reembolsos
          "limit": min(limit, 250),
          "order": "updated_at asc", # Ordenamos por actualización
      }

      # IMPORTANTE: Para reembolsos usamos updated_at, no created_at
      if date_start:
          params["updated_at_min"] = date_start
      if date_end:
          params["updated_at_max"] = date_end

      while url:
          self.verbose(f"SHOPIFY REFUNDS | LOOP {loop_count} | Requesting: {url}")

          # En la primera vuelta mandamos params, en las siguientes Shopify ya lo incluye en el link 'next'
          response = requests.get(
              url,
              headers=self.headers,
              params=params if loop_count == 1 else None
          )

          try:
              response.raise_for_status()
          except requests.exceptions.HTTPError as e:
              print(f"Error HTTP: {e} | {response.text}")
              return None

          data = response.json()
          orders = data.get("orders", [])

          # Opcional: Filtrado fino
          # Shopify nos da órdenes ACTUALIZADAS en ese rango.
          # Si quieres ser 100% estricto con la fecha del reembolso interno:
          for order in orders:
              if order.get("refunds"):
                  # Si necesitas validar que la fecha del reembolso (no de la orden)
                  # esté en el rango, podrías hacerlo aquí.
                  all_orders_with_refunds.append(order)

          self.verbose(f"SHOPIFY | Encontradas {len(orders)} órdenes con actividad de reembolso.")

          # 3. Paginación
          links = response.links
          if 'next' in links:
              url = links['next']['url']
              loop_count += 1
              #time.sleep(0.5) # Respetar rate limit
          else:
              url = None

      return all_orders_with_refunds


    def get_partially_refundeds(self, date_start: Optional[str] = None, date_end: Optional[str] = None, limit: int = 250) -> Optional[List[Dict]]:
          """
          Obtiene todas las órdenes que fueron reembolsadas dentro de un rango de fechas
          usando el filtro 'updated_at' y verificando el objeto 'refunds'.
          """
          self.verbose("SHOPIFY | Refunds")
          all_orders_with_refunds = []
          loop_count = 1

          # 1. URL Inicial y Parámetros
          # Usamos /orders.json pero con estados financieros específicos
          url = f"{self.base_url}/orders.json"

          params = {
              "status": "any", # Queremos ver órdenes abiertas, cerradas o canceladas
              "financial_status": "partially_refunded", # Filtro clave para reembolsos
              "limit": min(limit, 250),
              "order": "updated_at asc", # Ordenamos por actualización
          }

          # IMPORTANTE: Para reembolsos usamos updated_at, no created_at
          if date_start:
              params["updated_at_min"] = date_start
          if date_end:
              params["updated_at_max"] = date_end

          while url:
              self.verbose(f"SHOPIFY REFUNDS | LOOP {loop_count} | Requesting: {url}")

              # En la primera vuelta mandamos params, en las siguientes Shopify ya lo incluye en el link 'next'
              response = requests.get(
                  url,
                  headers=self.headers,
                  params=params if loop_count == 1 else None
              )

              try:
                  response.raise_for_status()
              except requests.exceptions.HTTPError as e:
                  print(f"Error HTTP: {e} | {response.text}")
                  return None

              data = response.json()
              orders = data.get("orders", [])

              # Opcional: Filtrado fino
              # Shopify nos da órdenes ACTUALIZADAS en ese rango.
              # Si quieres ser 100% estricto con la fecha del reembolso interno:
              for order in orders:
                  if order.get("refunds"):
                      # Si necesitas validar que la fecha del reembolso (no de la orden)
                      # esté en el rango, podrías hacerlo aquí.
                      all_orders_with_refunds.append(order)

              self.verbose(f"SHOPIFY | Encontradas {len(orders)} órdenes con actividad de reembolso.")

              # 3. Paginación
              links = response.links
              if 'next' in links:
                  url = links['next']['url']
                  loop_count += 1
                  #time.sleep(0.5) # Respetar rate limit
              else:
                  url = None

          return all_orders_with_refunds

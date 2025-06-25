from datetime import datetime

class Verbose:
    def __init__(self, active=True, alerts_enabled=True, workflow_name="UnknownWorkflow"):
        self.active = active
        self.alerts_enabled = alerts_enabled
        self.workflow_name = workflow_name  
        self.bot_url = "https://us-central1-d2b-data-management.cloudfunctions.net/innovation-messenger-hangout"

    def log(self, msg):
        if self.active:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{timestamp}]: {msg}")

    def critical(self, msg, current_workflow_name=None):
        name_to_use = current_workflow_name or self.workflow_name or "UnknownWorkflow"
        
        full_msg = f"[{name_to_use}] {msg}" # Mensaje para alerta y consola (sin 'e' ni 'exc_info')
        if self.active:
            print(f"CRITICAL: {full_msg}")
        if self.alerts_enabled:
            try:
                import requests 
                payload = {"message": full_msg} 
                response = requests.post(self.bot_url, json=payload, timeout=15)
                log_prefix_alert_status = f"[{name_to_use} via Verbose.critical]"
                if response.status_code >= 200 and response.status_code < 300:
                    if self.active: print(f"{log_prefix_alert_status} Critical alert sent successfully.")
                else:
                    if self.active: print(f"{log_prefix_alert_status} Failed to send critical alert. Status: {response.status_code}, Response: {response.text}")
            except Exception as alert_exception:
                if self.active: print(f"ERROR: [{name_to_use} via Verbose.critical] Exception sending critical alert: {alert_exception}")

    def set_workflow_name(self, name):
        self.workflow_name = name

# CONFIGURACIÓN GLOBAL
VERBOSE_ACTIVE = True
ALERTS_ENABLED = True 
verbose_logger = Verbose(active=VERBOSE_ACTIVE, alerts_enabled=ALERTS_ENABLED)
"""
config.py — Configurações centrais do Sistema 1 (Extrator de Leads INPI)

Carrega variáveis do .env e define constantes do projeto.
NÃO faz nenhuma chamada externa. Apenas leitura de arquivo local.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Carregar .env do diretório execution/
_ENV_PATH = Path(__file__).parent / ".env"
load_dotenv(_ENV_PATH)


# ============================================================
# PATHS
# ============================================================
BASE_DIR = Path(__file__).parent
TEMP_DIR = BASE_DIR / os.getenv("TEMP_DIR", ".tmp")
OUTPUT_DIR = BASE_DIR / os.getenv("OUTPUT_DIR", "output")
DATA_DIR = BASE_DIR / "data"

# Criar pastas se não existirem
TEMP_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# BANCO DE DADOS
# ============================================================
DATABASE_URL = os.getenv("DATABASE_URL", f"sqlite:///{DATA_DIR / 'leads.db'}")


# ============================================================
# CONFIGURAÇÃO
# ============================================================

# Foco na "Oposição para se manifestar" (Primeira rodada de teste)
TARGET_CODES = {'IPAS423'} # Código IPAS423 = Oposição
# TARGET_CODES = {'IPAS024', 'IPAS423', 'IPAS400'} # Códigos originais

IPAS_RENOVACAO_CODES = set()  # Sem renovação por ora
LEADS_ENRICH_LIMIT = int(os.getenv("LEADS_ENRICH_LIMIT", "50"))


# ============================================================
# ANTI-BAN: CONFIGURAÇÕES DO RPA
# ============================================================
INPI_URL_BASE = "https://busca.inpi.gov.br/pePI/"
INPI_URL_PESQUISA = "https://busca.inpi.gov.br/pePI/jsp/marcas/Pesquisa_num_processo.jsp"
INPI_RECAPTCHA_SITE_KEY = "6LfhwSAaAAAAANyx2xt8Ikk-YkQ3PGeAVhCfF3i2"

# Limites por sessão
MAX_PROCESSOS_POR_SESSAO = 15
DELAY_MIN_SEGUNDOS = 1.5
DELAY_MAX_SEGUNDOS = 4.0

# Cooldown quando conta é bloqueada (horas)
COOLDOWN_HORAS = 12


# ============================================================
# CREDENCIAIS (carregadas do .env, NUNCA hardcoded)
# ============================================================
CAPMONSTER_API_KEY = os.getenv("CAPMONSTER_API_KEY", "")
# Proxy (Suporta lista separada por vírgula para rotação)
_PROXY_RAW = os.getenv("PROXY_URL", "")
PROXY_LIST = [p.strip() for p in _PROXY_RAW.split(",") if p.strip()]

INFOSIMPLES_TOKEN = os.getenv("INFOSIMPLES_TOKEN", "")

# Notificação por email (Resend)
RESEND_API_KEY = os.getenv("RESEND_API_KEY", "")
NOTIFY_EMAIL = os.getenv("NOTIFY_EMAIL", "")

def carregar_contas_inpi() -> list[dict]:
    """
    Carrega as contas INPI do .env.
    Formato esperado: 'login1:senha1,login2:senha2'
    Retorna lista de dicts: [{"login": "x", "senha": "y"}, ...]
    """
    raw = os.getenv("INPI_CONTAS", "")
    if not raw:
        return []
    
    contas = []
    for par in raw.split(","):
        par = par.strip()
        if ":" in par:
            login, senha = par.split(":", 1)
            contas.append({"login": login.strip(), "senha": senha.strip()})
    
    return contas

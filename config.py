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
# CÓDIGOS IPAS RELEVANTES (filtro do parser XML)
# ============================================================
# Foco apenas no "Ouro" (Oposição, Nulidade, Indeferimento)
IPAS_CODES = {
    "IPAS423": "Oposição",
    "IPAS025": "Indeferimento do pedido",
    "IPAS400": "Nulidade Administrativa",
    "IPAS029": "Recurso",
}

# Quais códigos geram leads urgentes (processos sem procurador = oportunidade)
IPAS_LEAD_CODES = {"IPAS423", "IPAS025", "IPAS400"}

# Quais códigos indicam renovação (leads de marcas vencendo)
IPAS_RENOVACAO_CODES = set()


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
PROXY_URL = os.getenv("PROXY_URL", "")
INFOSIMPLES_TOKEN = os.getenv("INFOSIMPLES_TOKEN", "")

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

"""
enriquecimento.py — Worker 03: Orquestrador de Enriquecimento de Leads

Baseado na Directive 03. Este script varre a tabela `lead` em busca de registros
pendentes e gerencia a extração através de múltiplas camadas (sem InfoSimples - pulado por decisão do usuário):

Camada 1: RPA no INPI (rpa_pepi.py) -> Extrai CNPJ e e-mail do PDF do Despacho.
Camada 2: Minha Receita API (Gratuita) -> Enriquecimento através do CNPJ.
"""

import sys
import time
import requests
import logging
from config import INFOSIMPLES_TOKEN # Embora n usado ativamente na chamada, fica p compliance se precisar dps
from db import get_session, Lead, Processo
from rpa_pepi import executar_rpa_num_processo

# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ENRIQUECIMENTO")

# ============================================================
# CAMADA: API MINHA RECEITA (Busca via CNPJ)
# ============================================================

def consultar_cnpj_minha_receita(cnpj: str) -> dict:
    """Consome a API aberta Minha Receita para buscar todos os dados da empresa."""
    url = f"https://minhareceita.org/{cnpj}"
    logger.info(f"Consultando Minha Receita para CNPJ: {cnpj}")
    
    try:
        r = requests.get(url, timeout=15)
        if r.status_code == 200:
            return r.json()
        elif r.status_code == 429: # Rate limit
            logger.warning("Minha Receita bateu em Rate Limit (HTTP 429). Retrying em 5s...")
            time.sleep(5)
            r2 = requests.get(url, timeout=15)
            if r2.status_code == 200:
                return r2.json()
        logger.warning(f"Minha Receita falhou com status {r.status_code}")
    except Exception as e:
        logger.error(f"Erro ao requisitar Minha Receita: {e}")
    
    return {}


# ============================================================
# CÁLCULO FINAL DE SCORE
# ============================================================

def recalcular_score_final(lead: Lead) -> None:
    """Adiciona pontos ao lead baseado nos achados de enriquecimento."""
    score_atual = lead.score or 0
    
    # Bonificações da Directive 03
    if lead.email:
        score_atual += 20
        # Check heurístico (Email corporativo = + qualitativo)
        if lead.email_tipo == "titular":
            score_atual += 10
            
    if lead.telefone:
        score_atual += 10
        
    if lead.cnpj_dados:
        # Pega a chave correta da API ("situacao_cadastral" na Minha Receita = 2 Ativa)
        situacao = lead.cnpj_dados.get("descricao_situacao_cadastral", "").upper()
        if situacao == "ATIVA":
            score_atual += 5
        elif situacao == "INAPTA" or situacao == "BAIXADA":
            score_atual -= 20
            
    # Trava em 100
    lead.score = min(max(int(score_atual), 0), 100)
    
    # Reeclassifica
    if lead.score >= 80:
        lead.classificacao = "QUENTE 🔥"
    elif lead.score >= 40:
        lead.classificacao = "MORNO 🌡️"
    else:
        lead.classificacao = "FRIO ❄️"


# ============================================================
# ORQUESTRAÇÃO PRINCIPAL
# ============================================================

def processar_leads_pendentes(limite: int = 50):
    session = get_session()
    
    try:
        # Buscar leads pendentes na DB local, ordenados pelos melhores Scores
        query = session.query(Lead).filter(Lead.status == "PENDENTE").order_by(Lead.score.desc())
        
        if limite and limite > 0:
            query = query.limit(limite)
            
        leads = query.all()
        
        if not leads:
            logger.info("Nenhum lead pendente para enriquecimento no banco SQLite.")
            return

        logger.info(f"Iniciando enriquecimento para {len(leads)} leads.")
        
        for lead in leads:
            logger.info(f"--- Processando Lead: {lead.numero_processo} ---")
            
            # PASSO 1: Extrair CNPJ/Email via RPA
            cnpj_titular = None
            email_titular = None
            
            # Se o processo ja tem um doc na table Processos (as vezes o XML traz mas nulo)
            if lead.processo and lead.processo.titular_documento:
                doc = str(lead.processo.titular_documento).replace(".", "").replace("-", "").replace("/", "")
                if len(doc) == 14:  # É CNPJ!
                    cnpj_titular = doc
            
            # Executar RPA Playwright para buscar PDF e extrair Email/CNPJ
            dados_rpa = executar_rpa_num_processo(lead.numero_processo)
            
            if dados_rpa.get("status") == "sucesso":
                if dados_rpa.get("cnpj"):
                    cnpj_titular = dados_rpa["cnpj"]
                if dados_rpa.get("email"):
                    email_titular = dados_rpa["email"]
                    # Regra heurística PDF
                    if "marca" in email_titular or "patente" in email_titular or "advocacia" in email_titular:
                        lead.email_tipo = "escritorio"
                    else:
                        lead.email_tipo = "titular"
            
            lead.email = email_titular
            lead.fonte_enriquecimento = "pepi_pdf_rpa"
            
            # Atualiza score matemático da Directive 03
            recalcular_score_final(lead)
            
            # Muda status de processamento
            lead.status = "ENRIQUECIDO"
            
            session.commit()
            logger.info(f"Lead {lead.numero_processo} - STATUS = {lead.classificacao} - Score = {lead.score}")
            
    except Exception as e:
        logger.error(f"Erro catastrófico no Orquestrador de Enriquecimento: {e}")
        session.rollback()
    finally:
        session.close()


if __name__ == "__main__":
    processar_leads_pendentes()

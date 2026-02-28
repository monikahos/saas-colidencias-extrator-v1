"""
rpa_pepi.py — Worker 04: RPA do Portal pePI (INPI)

Automatiza o navegador via Playwright para baixar PDFs de petições e extrair
CNPJ e Email do titular da marca.
Usa as contas armazenadas no banco de dados e resolve o CAPTCHA do INPI.
"""

import sys
import io
import re
import time
import random
import logging
from pathlib import Path
from datetime import datetime

from playwright.sync_api import sync_playwright, Page, BrowserContext
from capmonster_python import RecaptchaV2Task
import PyPDF2

# Imports internos
from config import (
    INPI_URL_BASE, 
    INPI_URL_PESQUISA, 
    INPI_RECAPTCHA_SITE_KEY, 
    CAPMONSTER_API_KEY,
    TEMP_DIR
)
from db import get_session, ContaINPI, Lead


# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("RPA_PEPI")


# ============================================================
# ANTI-BAN: GERENCIADOR DE CONTAS
# ============================================================

def obter_conta_inpi() -> ContaINPI | None:
    session = get_session()
    try:
        conta = session.query(ContaINPI).filter(
            ContaINPI.status == "ATIVA",
            ContaINPI.total_processos_hoje < ContaINPI.daily_limit
        ).order_by(ContaINPI.ultimo_uso.asc().nulls_first()).first()
        return conta
    finally:
        session.close()

def atualizar_uso_conta(conta_id: int):
    session = get_session()
    try:
        conta = session.query(ContaINPI).get(conta_id)
        if conta:
            conta.ultimo_uso = datetime.utcnow()
            conta.total_processos_hoje += 1
            session.commit()
    finally:
        session.close()
        
def marcar_conta_falha(conta_id: int):
    session = get_session()
    try:
        conta = session.query(ContaINPI).get(conta_id)
        if conta:
            conta.status = "COOLDOWN"
            session.commit()
            logger.warning(f"Conta {conta.login} colocada em COOLDOWN.")
    finally:
        session.close()


# ============================================================
# UTILITÁRIOS RPA
# ============================================================

def delay_humano():
    """Pausa aleatória entre 1.5s e 4.0s para simular humano."""
    time.sleep(random.uniform(1.5, 4.0))


def resolver_captcha(page: Page) -> bool:
    """Usa CapMonster para resolver o reCAPTCHA da página atual."""
    if not CAPMONSTER_API_KEY:
        logger.error("CAPMONSTER_API_KEY não configurada no .env!")
        return False
        
    logger.info("Enviando desafio para o CapMonster...")
    capmonster = RecaptchaV2Task(CAPMONSTER_API_KEY)
    
    try:
        task_id = capmonster.create_task(page.url, INPI_RECAPTCHA_SITE_KEY)
        resultado = capmonster.join_task_result(task_id)
        token = resultado.get("gRecaptchaResponse")
        
        if token:
            logger.info("Captcha resolvido com sucesso.")
            # Injeta token no DOM
            page.evaluate(f'document.querySelector(\'[name="g-recaptcha-response"]\').value = "{token}";')
            return True
        return False
    except Exception as e:
        logger.error(f"Erro ao resolver captcha: {e}")
        return False


# ============================================================
# FLUXOS DO PLAYWRIGHT
# ============================================================

def login_inpi(page: Page, login: str, senha_enc: str) -> bool:
    """Realiza o login no portal pePI."""
    logger.info(f"Tentando login com usuário: {login}")
    page.goto(INPI_URL_BASE)
    
    # Preencher credenciais (senha aqui deveria ser descriptografada em prd)
    # Por hora, assume que o banco local tem a senha em plaintext
    page.locator('input[name="T_Login"]').fill(login)
    page.locator('input[name="T_Senha"]').fill(senha_enc)
    
    page.locator('input[type="submit"]').click()
    page.wait_for_load_state("networkidle")
    delay_humano()
    
    # Verifica se sucesso (tem link pra pesquisa)
    if page.locator('a[href*="Pesquisa_num_processo.jsp"]').count() > 0:
        logger.info("Login bem-sucedido.")
        return True
    
    logger.warning("Falha no login.")
    return False


def buscar_processo(page: Page, numero_processo: str) -> bool:
    """Pesquisa por um número de processo específico."""
    logger.info(f"Buscando processo: {numero_processo}")
    page.goto(INPI_URL_PESQUISA)
    page.locator('input[name="NumPedido"]').fill(numero_processo)
    page.locator('input[type="submit"][name="botao"]').click()
    page.wait_for_load_state("networkidle")
    
    # Clica no link de detalhe se existir
    link_detalhe = page.locator('a[href*="Action=detail"]')
    if link_detalhe.count() > 0:
        link_detalhe.first.click()
        page.wait_for_load_state("networkidle")
        delay_humano()
        return True
    
    logger.warning(f"Processo {numero_processo} não encontrado.")
    return False


def baixar_pdf_peticao(page: Page) -> bytes | None:
    """Navega para petições, busca cód 389/394, resolve captcha e baixa o PDF."""
    link_acesso = page.locator('a:has-text("Clique aqui para ter acesso")')
    if link_acesso.count() == 0:
        logger.info("Nenhuma petição disponível ou link de acesso ausente.")
        return None
        
    link_acesso.click()
    page.wait_for_load_state("networkidle")
    delay_humano()
    
    # Destruir overlays modais irritantes do INPI via eval
    page.evaluate('''
        document.querySelectorAll('.overlay, .modal-backdrop, #overlay').forEach(el => el.remove());
    ''')
    
    # Procurar as linhas de tabela com despachos originais
    linhas = page.locator('table tr')
    count = linhas.count()
    
    for i in range(count):
        linha = linhas.nth(i)
        texto = linha.inner_text()
        if "389" in texto or "394" in texto:
            logger.info("Encontramos petição alvo (389 ou 394).")
            
            # Clicar no icone PDF
            icone_pdf = linha.locator('img[src*="pdf.gif"]')
            if icone_pdf.count() > 0:
                icone_pdf.first.click()
                delay_humano()
                
                # Se aparece botão do captcha, precisamos resolver
                botao_captcha = page.locator('#captchaButton')
                if botao_captcha.count() > 0:
                    if not resolver_captcha(page):
                        return None
                    
                    with page.expect_download(timeout=60000) as download_info:
                        botao_captcha.click()
                    
                    download = download_info.value
                    path_temp = TEMP_DIR / download.suggested_filename
                    download.save_as(path_temp)
                    logger.info(f"PDF baixado: {path_temp}")
                    
                    # Ler o arquivo pra memória e apagar (menos lixo)
                    conteudo = path_temp.read_bytes()
                    path_temp.unlink(missing_ok=True)
                    
                    return conteudo
                    
    logger.info("Arquivo alvo 389/394 não encontrado na lista de petições.")
    return None


def apagar_rastro_acesso(page: Page):
    """
    CRÍTICO: O INPI te cadastra no processo ao ver petição.
    Precisa ir e remover o "Amplo Acesso" para não poluir caixa e não alertar titular.
    """
    logger.info("Apagando rastros de acesso (descadastramento)...")
    try:
        # Volta ao detalhe do processo se necessário
        link_terceiros = page.locator('a:has-text("Listagem de Terceiros Interessados")')
        if link_terceiros.count() > 0:
            with page.expect_popup() as popup_info:
                link_terceiros.click()
            popup = popup_info.value
            popup.wait_for_load_state()
            
            bt_desativar = popup.locator('a[href*="DesativarAmploAcesso"]')
            if bt_desativar.count() > 0:
                bt_desativar.first.click()
                logger.info("Rastroapagado com sucesso.")
            popup.close()
    except Exception as e:
        logger.warning(f"Não conseguiu apagar rastros: {e}")


# ============================================================
# EXTRAÇÃO DO PDF (LOCAL)
# ============================================================

def extrair_dados_do_pdf(pdf_bytes: bytes) -> dict:
    """Usa PyPDF2 para ler bytes do PDF e aplicar Regex extraindo Email e CNPJ."""
    dados = {"cnpj": None, "email": None}
    
    try:
        reader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
        texto_completo = ""
        for i in range(len(reader.pages)):
            page_text = reader.pages[i].extract_text()
            if page_text:
                texto_completo += page_text + "\n"
        
        # Regex CNPJ
        # Procura algo como 'cpf/cnpj/número inpi: 12.345.678/0001-90'
        match_cnpj = re.search(r'(?i)cpf/cnpj/n.mero inpi:\s*([0-9\.\-/]+)', texto_completo)
        if match_cnpj:
            cnpj_cru = match_cnpj.group(1).strip()
            # Deixar só os números
            dados["cnpj"] = re.sub(r'[^0-9]', '', cnpj_cru)
            
        # Regex Email
        # Procura qualquer email válido no texto (simplificado)
        match_email = re.search(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', texto_completo)
        if match_email:
            dados["email"] = match_email.group(0).strip().lower()
            
    except Exception as e:
        logger.error(f"Erro ao parsear PDF: {e}")
        
    return dados


# ============================================================
# EXECUÇÃO PRINCIPAL
# ============================================================

def executar_rpa_num_processo(numero_processo: str) -> dict:
    """Fluxo completo do RPA para um único processo."""
    conta = obter_conta_inpi()
    if not conta:
        logger.error("Nenhuma conta INPI disponível para uso no momento.")
        return {"status": "erro", "erro": "Sem contas ativas"}
        
    logger.info(f"Iniciando RPA para processo {numero_processo} usando DB_ID: {conta.id}")
    
    from playwright.sync_api import sync_playwright
    
    dados = {"status": "pendente", "email": None, "cnpj": None}
    
    with sync_playwright() as p:
        # Lança o Chromium local invisivel (headless)
        browser = p.chromium.launch(headless=True)
        # Isolar sessão por conta do INPI
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = context.new_page()
        
        try:
            if login_inpi(page, conta.login, conta.senha_enc):
                delay_humano()
                if buscar_processo(page, numero_processo):
                    delay_humano()
                    pdf_bytes = baixar_pdf_peticao(page)
                    
                    if pdf_bytes:
                        dados_extraidos = extrair_dados_do_pdf(pdf_bytes)
                        dados.update(dados_extraidos)
                        dados["status"] = "sucesso"
                    else:
                        dados["status"] = "falha"
                        dados["erro"] = "PDF não localizado ou Falha no Captcha"
                    
                    # Passinho crucial do RPA
                    apagar_rastro_acesso(page)
            else:
                marcar_conta_falha(conta.id)
                dados["status"] = "erro"
                dados["erro"] = "Falha no Login"
                
            # Se usou a conta de boas, incrementamos o contador
            atualizar_uso_conta(conta.id)
                
        except Exception as e:
            logger.error(f"Exceção inexperada no RPA: {e}")
            dados["status"] = "erro"
            dados["erro"] = str(e)
        finally:
            context.close()
            browser.close()
            
    return dados


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("processo", help="Número do processo no INPI")
    args = parser.parse_args()
    
    resultado = executar_rpa_num_processo(args.processo)
    print(f"\nResultado final:\n{resultado}")

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
    TEMP_DIR,
    PETICOES_DIR,
    PROXY_LIST
)
from db import get_session, ContaINPI, Lead, Processo


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
    """Pausa aleatória entre 3.0s e 8.0s para simular humano nas ações."""
    time.sleep(random.uniform(3.0, 8.0))

def pausa_entre_leads():
    """Pausa longa entre processos para evitar bloqueio de IP/Conta."""
    segundos = random.randint(60, 120)
    logger.info(f"Aguardando {segundos} segundos antes do próximo lead (simulação humana)...")
    time.sleep(segundos)


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
    
    # Preencher credenciais
    page.locator('input[name="T_Login"]').fill(login)
    page.locator('input[name="T_Senha"]').fill(senha_enc)
    
    page.locator('input[type="submit"]').click()
    page.wait_for_load_state("networkidle")
    delay_humano()
    
    # Verifica se sucesso: o INPI mostra "Login: <usuario>" na página pós-login
    texto_pagina = page.inner_text("body")
    if f"Login: {login}" in texto_pagina or f"login: {login}" in texto_pagina.lower():
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
    # Seletor robusto: busca pelo link que dispara a solicitação de acesso (independente do texto exato)
    link_acesso = page.locator('a[href*="AmploAcesso"], a:has-text("Clique aqui para ter acesso")').first
    
    try:
        # Espera o link aparecer por até 10 segundos e faz scroll até ele
        link_acesso.wait_for(state="visible", timeout=10000)
        link_acesso.scroll_into_view_if_needed()
    except Exception:
        logger.info("Link de acesso às petições não localizado ou processo sem petições públicas.")
        return None
        
    # O link de acesso abre um POPUP novo para a LGPD
    with page.context.expect_page() as popup_info:
        link_acesso.click()
    popup = popup_info.value
    popup.wait_for_load_state("networkidle")
    delay_humano()
    
    # Lidar com a Declaração de Finalidade (LGPD) no POPUP
    try:
        if "Declaração da Finalidade" in popup.content() or popup.locator("#codigoHipotese").count() > 0:
            logger.info("Preenchendo declaração de finalidade (LGPD) no popup...")
            # Seleciona 'Exercício de Direito Fundamental'
            popup.select_option("#codigoHipotese", label="Exercício de Direito Fundamental")
            # Marca o checkbox de concordância
            popup.locator("#aceite").check()
            # Clica em Enviar (pode ser um input ou link estilizado como botão)
            btn_enviar = popup.locator('input[value="Enviar"], .div-moddal-sol-amplo-acesso-btn-enviar')
            btn_enviar.first.click()
            popup.wait_for_load_state("networkidle")
            delay_humano()
    except Exception as e:
        logger.warning(f"Erro ao lidar com popup de finalidade: {e}")
    finally:
        if not popup.is_closed():
            popup.close()

    # Volta para a página principal (que agora deve ter os ícones de PDF liberados)
    page.bring_to_front()
    page.reload() # Recarregar para garantir que os ícones reflitam a liberação
    page.wait_for_load_state("networkidle")
    
    # Procurar as linhas de tabela com despachos originais
    linhas = page.locator('tr')
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
                
                # O botão de download fica dentro de um modal com reCAPTCHA
                if page.locator('#captchaButton').count() > 0 or page.locator('iframe[src*="recaptcha"]').count() > 0:
                    logger.info("Resolvendo reCAPTCHA de download...")
                    if not resolver_captcha(page):
                        logger.error("Falha ao resolver captcha de download.")
                        return None
                    
                    # Clicar no botão Download (captchaButton) após resolver captcha
                    try:
                        with page.expect_download(timeout=90000) as download_info:
                            page.locator('#captchaButton').click()
                        
                        download = download_info.value
                        filename = f"peticao_{page.url.split('CodPedido=')[-1].split('&')[0]}.pdf"
                        path_final = PETICOES_DIR / filename
                        download.save_as(path_final)
                        logger.info(f"PDF salvo em: {path_final}")
                        
                        return str(path_final).encode() # Retorna o path como bytes para compatibilidade temporária ou ajuste dps
                    except Exception as e:
                        logger.error(f"Erro ao disparar download do PDF: {e}")
                        return None
                    
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

def extrair_dados_do_pdf(pdf_path_bytes: bytes) -> dict:
    """Extrai Email do Titular, Email do Procurador e Nome do Procurador do PDF salvo."""
    dados = {"cnpj": None, "email_titular": None, "email_procurador": None, "nome_procurador": None, "pdf_path": None}
    
    path_str = pdf_path_bytes.decode()
    path_file = Path(path_str)
    dados["pdf_path"] = path_str
    
    try:
        reader = PyPDF2.PdfReader(path_file)
        texto_completo = ""
        for i in range(len(reader.pages)):
            page_text = reader.pages[i].extract_text()
            if page_text:
                texto_completo += page_text + "\n"
        
        # Regex CNPJ
        match_cnpj = re.search(r'(?i)cpf/cnpj/n.mero inpi:\s*([0-9\.\-/]+)', texto_completo)
        if match_cnpj:
            dados["cnpj"] = re.sub(r'[^0-9]', '', match_cnpj.group(1).strip())
            
        # Extração de E-mails
        emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', texto_completo)
        for email in emails:
            email_lower = email.lower()
            if any(key in email_lower for key in ["adv", "marca", "patente", "juridico", "contato@", "atendimento@"]):
                if not dados["email_procurador"]:
                    dados["email_procurador"] = email_lower
            else:
                if not dados["email_titular"]:
                    dados["email_titular"] = email_lower
                    
        # Tentativa de pegar Nome do Procurador
        # Geralmente segue o rótulo "Procurador:" ou aparece após a OAB
        match_proc = re.search(r'(?i)Procurador:\s*([^\n]+)', texto_completo)
        if match_proc:
            dados["nome_procurador"] = match_proc.group(1).strip()
            
    except Exception as e:
        logger.error(f"Erro ao parsear PDF {path_file}: {e}")
        
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
    
    dados = {"status": "pendente", "email_titular": None, "email_procurador": None, "nome_procurador": None, "cnpj": None, "pdf_path": None}
    
    with sync_playwright() as p:
        # Configuração de Proxy se existir no .env
        launch_args = {"headless": True}
        context_args = {
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        
        if PROXY_LIST:
            proxy_escolhido = random.choice(PROXY_LIST)
            logger.info(f"Usando Proxy para navegação: {proxy_escolhido.split('@')[-1]}")
            launch_args["proxy"] = {"server": proxy_escolhido}
        
        # Lança o Chromium
        browser = p.chromium.launch(**launch_args)
        # Isolar sessão por conta do INPI
        context = browser.new_context(**context_args)
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

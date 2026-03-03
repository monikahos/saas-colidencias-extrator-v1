"""
run_pipeline.py — Orquestrador Automático do Pipeline de Leads

Executa o pipeline completo:
  1. Calcula o número da RPI da semana
  2. Baixa o XML do INPI
  3. Parseia e gera leads
  4. Enriquece leads (RPA pePI) -> Extrai e-mail do PDF do protocolo
  5. Envia email com resultado via Resend

USO:
    python run_pipeline.py              # Calcula a RPI da semana automaticamente
    python run_pipeline.py --rpi 2878   # Força um número específico

CRON (toda quarta às 09h):
    0 9 * * 3 cd ~/saas-colidencias/execution && .venv/bin/python run_pipeline.py >> ~/saas-colidencias/execution/pipeline.log 2>&1
"""

import argparse
import sys
import traceback
from datetime import date, timedelta

import random
import time
import requests

from config import RESEND_API_KEY, NOTIFY_EMAIL, LEADS_ENRICH_LIMIT
from download_rpi import executar as download_rpi
from parser_xml import parsear_xml, criar_tabelas
from enriquecimento import processar_leads_pendentes
from db import get_session, RPIHistory


# ============================================================
# CÁLCULO AUTOMÁTICO DO NÚMERO DA RPI
# ============================================================

# Referência conhecida: RPI 2878 sai dia 03/03/2026 (terça)
RPI_REFERENCIA_NUMERO = 2878
RPI_REFERENCIA_DATA = date(2026, 3, 3)


def calcular_rpi_da_semana(data_hoje: date = None) -> int:
    """
    Calcula o número da RPI com base na data atual.
    A RPI sai toda terça-feira, incrementando 1 por semana.
    """
    if data_hoje is None:
        data_hoje = date.today()
    
    dias_diff = (data_hoje - RPI_REFERENCIA_DATA).days
    semanas_diff = dias_diff // 7
    
    return RPI_REFERENCIA_NUMERO + semanas_diff


# ============================================================
# NOTIFICAÇÃO POR EMAIL (RESEND)
# ============================================================

def enviar_email(assunto: str, corpo_html: str) -> bool:
    """Envia email via Resend API. Retorna True se enviou."""
    if not RESEND_API_KEY or not NOTIFY_EMAIL:
        print("⚠️  Email não configurado (RESEND_API_KEY ou NOTIFY_EMAIL vazio)")
        return False
    
    try:
        resp = requests.post(
            "https://api.resend.com/emails",
            headers={
                "Authorization": f"Bearer {RESEND_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "from": "Pipeline INPI <onboarding@resend.dev>",
                "to": [NOTIFY_EMAIL],
                "subject": assunto,
                "html": corpo_html,
            },
            timeout=15,
        )
        
        if resp.status_code in (200, 201):
            print(f"📧 Email enviado para {NOTIFY_EMAIL}")
            return True
        else:
            print(f"⚠️  Erro ao enviar email: HTTP {resp.status_code} - {resp.text}")
            return False
    except Exception as e:
        print(f"⚠️  Erro ao enviar email: {e}")
        return False


def _email_sucesso(numero_rpi: int, resultado: dict) -> str:
    """Gera o HTML do email de sucesso."""
    return f"""
    <div style="font-family: sans-serif; max-width: 600px; margin: auto; border: 1px solid #eee; padding: 20px;">
        <h2 style="color: #2e7d32;">✅ Pipeline RPI {numero_rpi} — Concluído</h2>
        <p>O processamento semanal foi finalizado com sucesso.</p>
        
        <table style="width: 100%; border-collapse: collapse; margin: 20px 0;">
            <tr style="background: #f9f9f9;">
                <td style="padding: 10px; border-bottom: 1px solid #eee;"><strong>📊 Total no XML:</strong></td>
                <td style="padding: 10px; border-bottom: 1px solid #eee;">{resultado.get('total_xml', 0):,}</td>
            </tr>
            <tr>
                <td style="padding: 10px; border-bottom: 1px solid #eee;"><strong>🎯 Processos Relevantes:</strong></td>
                <td style="padding: 10px; border-bottom: 1px solid #eee;">{resultado.get('relevantes', 0):,}</td>
            </tr>
            <tr style="background: #f9f9f9;">
                <td style="padding: 10px; border-bottom: 1px solid #eee;"><strong>🔥 Leads Gerados:</strong></td>
                <td style="padding: 10px; border-bottom: 1px solid #eee;">{resultado.get('leads', 0):,}</td>
            </tr>
        </table>
        
        <p style="color: #666; font-size: 14px;">
            Os leads já foram enriquecidos com e-mails extraídos dos PDFs de protocolo (RPA pePI).
        </p>
        
        <div style="margin-top: 30px; padding: 15px; background: #e8f5e9; border-radius: 4px; text-align: center;">
            <p style="margin: 0; font-weight: bold; color: #1b5e20;">Os dados já estão disponíveis no banco da aplicação.</p>
        </div>
    </div>
    """


def _email_erro(numero_rpi: int, erro: str) -> str:
    """Gera o HTML do email de erro."""
    return f"""
    <div style="font-family: sans-serif; max-width: 600px; margin: auto; border: 1px solid #fce4e4; padding: 20px;">
        <h2 style="color: #c62828;">❌ Pipeline RPI {numero_rpi} — Falhou</h2>
        <p>Ocorreu um erro crítico durante o processamento da RPI.</p>
        
        <div style="background: #fff5f5; padding: 15px; border-left: 4px solid #c62828; margin: 20px 0;">
            <code style="font-size: 13px; color: #c62828; white-space: pre-wrap;">{erro}</code>
        </div>
        
        <p style="color: #666; font-size: 14px;">Verifique os logs na VPS para mais detalhes.</p>
    </div>
    """


def aplicar_atraso_furtivo(max_minutos: int = 45):
    """Aguarda um tempo aleatório para evitar padrões de execução fixos."""
    segundos = random.randint(0, max_minutos * 60)
    minutos = segundos // 60
    restante = segundos % 60
    print(f"🕵️  MODO FURTIVO ATIVO: Aguardando {minutos}m {restante}s antes de iniciar...")
    time.sleep(segundos)


# ============================================================
# PIPELINE PRINCIPAL
# ============================================================

def executar_pipeline(numero_rpi: int):
    """Executa o pipeline completo: download → parse → enriquecer → email."""
    print(f"\n{'='*60}")
    print(f"🚀 Pipeline automático — RPI {numero_rpi}")
    print(f"📅 Data: {date.today()}")
    print(f"{'='*60}\n")
    
    try:
        # 1. Download
        print("📥 [1/4] Baixando XML do INPI...")
        caminho_xml = download_rpi(numero_rpi)
        
        if caminho_xml is None:
            print("⏭️  RPI já processada anteriormente. Nada a fazer.")
            return
        
        # 2. Parse
        print("\n🔍 [2/4] Parseando XML e gerando leads...")
        resultado = parsear_xml(caminho_xml, numero_rpi=numero_rpi)
        
        # 3. Enriquecer (Extrair e-mails dos PDFs no pePI)
        print(f"\n🤖 [3/4] Iniciando Enriquecimento (Limite: {LEADS_ENRICH_LIMIT} melhores leads)...")
        # Processa apenas os melhores leads (com base no score inicial do XML)
        processar_leads_pendentes(limite=LEADS_ENRICH_LIMIT)
        
        # 4. Notificar
        print("\n📧 [4/4] Enviando notificação...")
        enviar_email(
            assunto=f"✅ RPI {numero_rpi} — {resultado.get('leads', 0)} leads capturados",
            corpo_html=_email_sucesso(numero_rpi, resultado),
        )
        
        print(f"\n🎉 Pipeline concluído com sucesso!")
        
    except Exception as e:
        erro_msg = traceback.format_exc()
        print(f"\n💀 Pipeline falhou: {e}")
        
        enviar_email(
            assunto=f"❌ RPI {numero_rpi} — Pipeline falhou",
            corpo_html=_email_erro(numero_rpi, erro_msg),
        )
        
        raise


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Pipeline automático de leads INPI")
    parser.add_argument("--rpi", type=int, help="Número da RPI (auto-calcula se omitido)")
    args = parser.parse_args()
    
    criar_tabelas()
    
    numero_rpi = args.rpi or calcular_rpi_da_semana()
    
    # Aplica o atraso se estiver rodando no automático (sem RPI forçada)
    if not args.rpi:
        aplicar_atraso_furtivo(max_minutos=50) # Varia em até 50 minutos
        
    print(f"📰 RPI alvo: {numero_rpi}")
    
    executar_pipeline(numero_rpi)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⛔ Cancelado pelo usuário.")
        sys.exit(1)
    except Exception as e:
        print(f"\n💀 Erro fatal: {e}")
        sys.exit(1)

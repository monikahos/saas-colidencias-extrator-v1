"""
download_rpi.py — Worker 01: Download da RPI Semanal
Baixa o ZIP da Revista da Propriedade Industrial (RPI) do portal INPI.
USO:
    python download_rpi.py --numero 2877  # Baixa a RPI pelo número sequencial (Recomendado)
"""

import argparse
import sys
import time
import zipfile
import io
from datetime import datetime, date
from pathlib import Path

import requests

# Imports internos do projeto
from config import TEMP_DIR
from db import criar_tabelas, get_session, RPIHistory


# ============================================================
# CONSTANTES
# ============================================================

# Padrão de URL do INPI para XML (Marcas - RM)
URL_PATTERNS = [
    "https://revistas.inpi.gov.br/txt/RM{numero}.zip",
    "https://revistas.inpi.gov.br/rpi/RPMA/{ano}/RPMA{semana:02d}.zip", # Fallback antigo
]

DOWNLOAD_TIMEOUT = 180
MAX_RETRIES = 3

# ============================================================
# FUNÇÕES
# ============================================================

def ja_foi_processada(session, numero_rpi: int) -> bool:
    registro = session.query(RPIHistory).filter_by(numero_rpi=numero_rpi).first()
    return registro is not None and registro.status == "COMPLETED"


def tentar_download(numero: int, ano: int = None, semana: int = None) -> tuple[bytes, str]:
    erros = []
    
    # Prioridade para o número sequencial (RMxxxx.zip)
    urls_to_try = []
    if numero:
        urls_to_try.append(URL_PATTERNS[0].format(numero=numero))
    
    if ano and semana:
        urls_to_try.append(URL_PATTERNS[1].format(ano=ano, semana=semana))

    for url in urls_to_try:
        try:
            print(f"  Tentando: {url}")
            response = requests.get(url, timeout=DOWNLOAD_TIMEOUT)
            if response.status_code == 200:
                print(f"  ✅ Download OK ({len(response.content) / 1024 / 1024:.1f} MB)")
                return response.content, url
            else:
                erros.append(f"{url} → HTTP {response.status_code}")
        except Exception as e:
            erros.append(f"{url} → Erro: {e}")
    
    raise RuntimeError("Falha em todas as URLs tentadas.")


def salvar_zip(conteudo: bytes, numero: int) -> Path:
    nome_arquivo = f"RM{numero}.zip"
    caminho = TEMP_DIR / nome_arquivo
    caminho.write_bytes(conteudo)
    print(f"  💾 Salvo em: {caminho}")
    return caminho


def extrair_xml(caminho_zip: Path) -> Path:
    with zipfile.ZipFile(caminho_zip, "r") as zf:
        xmls = [n for n in zf.namelist() if n.lower().endswith(".xml")]
        if not xmls:
            raise ValueError(f"Nenhum XML encontrado dentro de {caminho_zip}")
        nome_xml = xmls[0]
        caminho_xml = TEMP_DIR / nome_xml
        zf.extract(nome_xml, TEMP_DIR)
        print(f"  📄 XML extraído: {caminho_xml}")
        return caminho_xml


def executar(numero: int):
    criar_tabelas()
    session = get_session()
    
    try:
        if ja_foi_processada(session, numero):
            print(f"⏭️  RPI {numero} já processada. Pulando.")
            return None
        
        registro = RPIHistory(numero_rpi=numero, data_publicacao=date.today(), status="PROCESSING")
        session.merge(registro)
        session.commit()
        
        conteudo, _ = tentar_download(numero)
        
        # Validar ZIP
        try:
            with zipfile.ZipFile(io.BytesIO(conteudo)) as zf:
                if not any(n.lower().endswith(".xml") for n in zf.namelist()):
                    raise ValueError("ZIP sem XML")
        except:
            registro.status = "FAILED"
            session.commit()
            print("❌ ZIP Inválido.")
            return None

        caminho_zip = salvar_zip(conteudo, numero)
        caminho_xml = extrair_xml(caminho_zip)
        
        registro.arquivo_path = str(caminho_xml)
        registro.status = "COMPLETED"
        session.commit()
        
        print(f"\n🎉 RPI {numero} pronta! XML em: {caminho_xml}")
        return caminho_xml
        
    except Exception as e:
        print(f"💀 Erro: {e}")
        session.rollback()
        raise
    finally:
        session.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--numero", type=int, required=True, help="Número da RPI (ex: 2877)")
    args = parser.parse_args()
    executar(args.numero)


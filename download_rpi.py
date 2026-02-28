"""
download_rpi.py — Worker 01: Download da RPI Semanal

Baseado na Directive 01 (adaptado para o Sistema 1 — sem S3/Redis).
Baixa o ZIP da Revista da Propriedade Industrial (RPI) do portal INPI
e salva no disco local (.tmp/).

USO:
    python download_rpi.py              # Baixa a RPI da semana atual
    python download_rpi.py --semana 10  # Baixa a RPI da semana 10
    python download_rpi.py --ano 2025 --semana 45

NÃO executa nenhuma ação destrutiva. Apenas faz download de arquivo público.
"""

import argparse
import sys
import time
import zipfile
from datetime import datetime, date
from pathlib import Path

import requests

# Imports internos do projeto
from config import TEMP_DIR
from db import criar_tabelas, get_session, RPIHistory


# ============================================================
# CONSTANTES
# ============================================================

# Padrão de URL da RPI de Marcas (RPMA)
# Aprendizado das referências: o INPI às vezes muda maiúsculas/minúsculas
URL_PATTERNS = [
    "https://revistas.inpi.gov.br/rpi/RPMA/{ano}/RPMA{semana:02d}.zip",
    "https://revistas.inpi.gov.br/rpi/rpma/{ano}/rpma{semana:02d}.zip",
    "https://revistas.inpi.gov.br/rpi/RPMA/{ano}/rpma{semana:02d}.zip",
]

DOWNLOAD_TIMEOUT = 180  # 3 minutos (arquivos podem ter >200MB)
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 3600  # 1 hora entre retries (a RPI pode atrasar)


# ============================================================
# FUNÇÕES
# ============================================================

def calcular_semana_atual() -> tuple[int, int]:
    """Retorna (ano, semana_iso) da data atual."""
    hoje = date.today()
    ano_iso, semana_iso, _ = hoje.isocalendar()
    return ano_iso, semana_iso


def ja_foi_processada(session, numero_rpi: int) -> bool:
    """Verifica se essa RPI já foi baixada com sucesso."""
    registro = session.query(RPIHistory).filter_by(
        numero_rpi=numero_rpi
    ).first()
    return registro is not None and registro.status == "COMPLETED"


def tentar_download(ano: int, semana: int) -> tuple[bytes, str]:
    """
    Tenta baixar o ZIP tentando diferentes padrões de URL.
    
    Retorna (conteudo_bytes, url_usada) ou levanta exceção.
    Aprendizado: o INPI muda o case da URL sem aviso.
    """
    erros = []
    
    for pattern in URL_PATTERNS:
        url = pattern.format(ano=ano, semana=semana)
        try:
            print(f"  Tentando: {url}")
            response = requests.get(url, timeout=DOWNLOAD_TIMEOUT, stream=False)
            
            if response.status_code == 200:
                print(f"  ✅ Download OK ({len(response.content) / 1024 / 1024:.1f} MB)")
                return response.content, url
            else:
                erros.append(f"{url} → HTTP {response.status_code}")
                
        except requests.exceptions.Timeout:
            erros.append(f"{url} → Timeout ({DOWNLOAD_TIMEOUT}s)")
        except requests.exceptions.ConnectionError as e:
            erros.append(f"{url} → Conexão falhou: {e}")
    
    raise RuntimeError(
        f"Falha em todas as URLs para RPI semana {semana}/{ano}:\n"
        + "\n".join(f"  - {e}" for e in erros)
    )


def validar_zip(conteudo: bytes) -> bool:
    """
    Valida que o conteúdo baixado é um ZIP legítimo.
    Aprendizado: o INPI pode retornar HTML de erro com status 200.
    """
    import io
    try:
        with zipfile.ZipFile(io.BytesIO(conteudo)) as zf:
            # Verificar se contém pelo menos 1 arquivo XML
            nomes = zf.namelist()
            tem_xml = any(n.lower().endswith(".xml") for n in nomes)
            if not tem_xml:
                print(f"  ⚠️ ZIP válido mas sem XML dentro. Arquivos: {nomes}")
                return False
            print(f"  ✅ ZIP válido. Contém: {nomes}")
            return True
    except zipfile.BadZipFile:
        print("  ❌ Arquivo corrompido (não é um ZIP válido)")
        return False


def salvar_zip(conteudo: bytes, ano: int, semana: int) -> Path:
    """Salva o ZIP no disco local (.tmp/)."""
    nome_arquivo = f"RPMA{semana:02d}_{ano}.zip"
    caminho = TEMP_DIR / nome_arquivo
    caminho.write_bytes(conteudo)
    print(f"  💾 Salvo em: {caminho}")
    return caminho


def extrair_xml(caminho_zip: Path) -> Path:
    """Extrai o XML do ZIP para a mesma pasta .tmp/."""
    with zipfile.ZipFile(caminho_zip, "r") as zf:
        # Pegar o primeiro XML encontrado
        xmls = [n for n in zf.namelist() if n.lower().endswith(".xml")]
        if not xmls:
            raise ValueError(f"Nenhum XML encontrado dentro de {caminho_zip}")
        
        nome_xml = xmls[0]
        caminho_xml = TEMP_DIR / nome_xml
        zf.extract(nome_xml, TEMP_DIR)
        print(f"  📄 XML extraído: {caminho_xml}")
        return caminho_xml


# ============================================================
# MAIN
# ============================================================

def executar(ano: int | None = None, semana: int | None = None):
    """Fluxo principal do download, seguindo a Directive 01."""
    
    # Garantir que as tabelas existem
    criar_tabelas()
    
    # Passo 1: Calcular semana/ano
    if ano is None or semana is None:
        ano, semana = calcular_semana_atual()
    
    numero_rpi = int(f"{ano}{semana:02d}")  # Ex: 202510
    print(f"\n{'='*50}")
    print(f"📰 RPI Semana {semana}/{ano} (ID: {numero_rpi})")
    print(f"{'='*50}")
    
    # Passo 2: Verificar se já processou
    session = get_session()
    try:
        if ja_foi_processada(session, numero_rpi):
            print("⏭️  Já processada anteriormente. Pulando.")
            return None
        
        # Passo 3: Criar registro PROCESSING
        registro = RPIHistory(
            numero_rpi=numero_rpi,
            data_publicacao=date.today(),
            status="PROCESSING"
        )
        session.merge(registro)  # merge para caso já exista com FAILED
        session.commit()
        
        # Passo 4: Download com retry
        conteudo = None
        for tentativa in range(1, MAX_RETRIES + 1):
            try:
                print(f"\n📥 Tentativa {tentativa}/{MAX_RETRIES}...")
                conteudo, url_usada = tentar_download(ano, semana)
                break
            except RuntimeError as e:
                print(f"  ❌ {e}")
                if tentativa < MAX_RETRIES:
                    print(f"  ⏳ Aguardando {RETRY_DELAY_SECONDS}s antes de tentar novamente...")
                    time.sleep(RETRY_DELAY_SECONDS)
        
        if conteudo is None:
            registro.status = "FAILED"
            session.commit()
            print("\n💀 Todas as tentativas falharam.")
            return None
        
        # Passo 5: Validar ZIP
        if not validar_zip(conteudo):
            registro.status = "FAILED"
            session.commit()
            print("\n💀 ZIP inválido ou corrompido.")
            return None
        
        # Passo 6: Salvar no disco
        caminho_zip = salvar_zip(conteudo, ano, semana)
        
        # Passo 7: Extrair XML
        caminho_xml = extrair_xml(caminho_zip)
        
        # Passo 8: Atualizar registro como COMPLETED
        registro.arquivo_path = str(caminho_xml)
        registro.status = "COMPLETED"
        session.commit()
        
        print(f"\n🎉 Download concluído! XML pronto em: {caminho_xml}")
        print(f"   Próximo passo: python parser_xml.py --rpi {numero_rpi}")
        
        return caminho_xml
        
    except Exception as e:
        # Qualquer erro inesperado → marcar como FAILED
        try:
            registro = session.query(RPIHistory).get(numero_rpi)
            if registro:
                registro.status = "FAILED"
                session.commit()
        except Exception:
            pass
        raise
    finally:
        session.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Baixa o ZIP da RPI semanal do INPI")
    parser.add_argument("--ano", type=int, default=None, help="Ano ISO (ex: 2026)")
    parser.add_argument("--semana", type=int, default=None, help="Semana ISO (ex: 10)")
    args = parser.parse_args()
    
    try:
        executar(ano=args.ano, semana=args.semana)
    except KeyboardInterrupt:
        print("\n⛔ Cancelado pelo usuário.")
        sys.exit(1)
    except Exception as e:
        print(f"\n💀 Erro fatal: {e}")
        sys.exit(1)

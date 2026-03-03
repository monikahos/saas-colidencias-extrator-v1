"""
parser_xml.py — Worker 02: Parser Streaming do XML da RPI

Faz streaming do XML da RPI usando lxml.etree.iterparse,
filtra por códigos IPAS relevantes, calcula lead score inicial
e insere no banco de dados.

USO:
    python parser_xml.py                        # Processa a última RPI baixada
    python parser_xml.py --arquivo .tmp/RPMA10_2026.xml
    python parser_xml.py --rpi 202610

NÃO faz nenhuma chamada externa. Apenas lê o XML local e grava no banco.
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

from lxml import etree
import re

from config import TARGET_CODES
from db import criar_tabelas, get_session, RPIHistory, Processo, Lead


# ============================================================
# FILTROS E CLASSIFICAÇÃO
# ============================================================

EXCLUDE_KEYWORDS = [
    'advocacia', 'advogado', 'advogados', 'advogada', 'advogadas',
    'juridico', 'jurídico', 'juridica', 'jurídica',
    'marca', 'marcas', 'patente', 'patentes',
    'propriedade intelectual'
]

PJ_KEYWORDS = [
    r'\bltda\b', r'\bme\b', r'\bmei\b', r'\bepp\b', r'\beireli\b', 
    r'\bs/?a\b', r'\bcia\b', r'\bcomercio\b', r'\bcomércio\b', 
    r'\bindustria\b', r'\bindústria\b', r'\bassociacao\b', r'\bassociação\b',
    r'\bcondominio\b', r'\binstituto\b', r'\bfundação\b', r'\bfundacao\b',
    r'\bclube\b', r'\bsindicato\b', r'\bcooperativa\b', r'\bempresa\b', 
    r'\bparticipacoes\b', r'\bparticipações\b', r'\bholding\b', r'\bmicroempresa\b',
    r'\bdistribuidora\b', r'\btransportes\b', r'\btransportadora\b'
]
PJ_PATTERN = re.compile('|'.join(PJ_KEYWORDS), re.IGNORECASE)


def is_concorrente(nome: str) -> bool:
    """Retorna True se o nome pertence a um escritório/concorrente."""
    if not nome:
        return False
    nome_lower = nome.lower()
    return any(kw in nome_lower for kw in EXCLUDE_KEYWORDS)


def classificar_tipo_pessoa(nome: str) -> str:
    """Classifica como PJ ou PF com base em keywords no nome."""
    if not nome:
        return "Pessoa Física"
    if PJ_PATTERN.search(nome):
        return "Pessoa Jurídica"
    return "Pessoa Física"


def extrair_oponente(texto: str) -> str:
    """Extrai o nome do oponente do texto complementar do despacho."""
    if not texto:
        return "N/A"
    match = re.search(r'(?:oposta por|oposto por|por)\s*(.+)', texto, re.IGNORECASE)
    if match:
        nome = match.group(1).strip().rstrip('.')
        return nome
    return "N/A"


# ============================================================
# CONSTANTES XML
# ============================================================

TAG_PROCESSO = "processo"
TAG_DESPACHO = "despacho"
TAG_CLASSE_NICE = "classe-nice"

ANO_DEPOSITO_MINIMO = 2010


# ============================================================
# DETECÇÃO DE ENCODING
# ============================================================

def detectar_encoding(caminho_xml: Path) -> str:
    """
    Lê os primeiros bytes do XML para detectar o encoding.
    O INPI alterna entre ISO-8859-1 e UTF-8 sem aviso.
    """
    with open(caminho_xml, "rb") as f:
        cabecalho = f.read(200).lower()
    
    if b"iso-8859-1" in cabecalho or b"latin" in cabecalho:
        return "iso-8859-1"
    return "utf-8"


# ============================================================
# LEAD SCORING
# ============================================================

def calcular_score_inicial(dados: dict) -> int:
    """
    Calcula o score inicial do lead.
    - PJ: +30
    - IPAS400 (Nulidade): +50
    - IPAS423 (Oposição): +40
    - IPAS024 (Indeferimento): +15
    """
    score = 0
    
    if dados.get("tipo_pessoa") == "Pessoa Jurídica":
        score += 30
    
    for cod in dados.get("codigos_ocorridos", []):
        if cod == "IPAS400":
            score += 50
        elif cod == "IPAS423":
            score += 40
        elif cod == "IPAS024":
            score += 15
    
    return min(score, 100)


def classificar_lead(score: int) -> str:
    """Classifica o lead em Tiers A/B/C."""
    if score >= 70:
        return "A (Alta Prioridade)"
    elif score >= 40:
        return "B (Prioridade Média)"
    return "C (Baixa Prioridade)"


# ============================================================
# EXTRAÇÃO DE DADOS DO PROCESSO
# ============================================================

def extrair_dados_processo(elem_processo) -> dict | None:
    """
    Extrai os campos relevantes de um elemento <processo> do XML.
    Retorna None se o processo deve ser descartado.
    
    Regras de descarte:
    - Sem número de processo
    - Tem procurador (já tem representação)
    - Nenhum despacho com código IPAS relevante
    - Titular estrangeiro (sem UF)
    - Titular é concorrente (escritório de marca/advocacia)
    - Data de depósito anterior a ANO_DEPOSITO_MINIMO
    """
    numero = elem_processo.get("numero", "").strip()
    if not numero:
        return None
    
    # Procurador → descarta (já tem representação)
    if elem_processo.find("procurador") is not None:
        return None
    
    # Titular — extrair ANTES do loop de despachos (pertence ao processo)
    titular_elem = elem_processo.find(".//titular")
    titular_nome = ""
    titular_uf = ""
    titular_doc = None
    if titular_elem is not None:
        titular_nome = titular_elem.get("nome-razao-social", "").strip()
        titular_uf = titular_elem.get("uf", "").strip()
        titular_doc = titular_elem.get("cnpj-cpf", "").strip() or None
    
    # Estrangeiro (sem UF) → descarta
    if not titular_uf or titular_uf == "N/A":
        return None
    
    # Concorrente → descarta
    if is_concorrente(titular_nome):
        return None
    
    # Marca
    marca_elem = elem_processo.find(".//marca/nome")
    marca_nome = marca_elem.text.strip() if (marca_elem is not None and marca_elem.text) else "N/A"
    
    # Loop despachos — buscar o PRIMEIRO código IPAS relevante
    codigo_ipas = None
    detalhes = None
    
    for desp in elem_processo.iter(TAG_DESPACHO):
        codigo = desp.get("codigo", "").strip().upper()
        if codigo not in TARGET_CODES:
            continue
        
        # Tipo de procedimento
        if codigo == "IPAS400":
            tipo = "Nulidade"
        elif codigo == "IPAS423":
            tipo = "Oposição"
        else:
            tipo = "Indeferimento"
        
        # Texto complementar (quem pediu a oposição/nulidade)
        texto_comp_elem = desp.find("texto-complementar")
        texto_comp = ""
        if texto_comp_elem is not None and texto_comp_elem.text:
            texto_comp = texto_comp_elem.text.replace("\n", " ").replace("\r", "").strip()
        
        quem_pediu = extrair_oponente(texto_comp) if codigo != "IPAS024" else "INPI (Governo)"
        
        codigo_ipas = codigo
        detalhes = f"[{tipo}] Proc: {numero} - Marca: {marca_nome} - Origem: {quem_pediu}"
        break  # Só pega o PRIMEIRO despacho relevante
    
    if not codigo_ipas:
        return None
    
    # Classe NICE
    classe_nice = None
    classe_elem = elem_processo.find(TAG_CLASSE_NICE)
    if classe_elem is not None:
        classe_nice = classe_elem.get("codigo", "").strip() or None
    
    # Data de depósito
    data_deposito = None
    deposito_elem = elem_processo.find("data-deposito")
    if deposito_elem is not None and deposito_elem.text:
        try:
            data_deposito = datetime.strptime(deposito_elem.text.strip(), "%d/%m/%Y").date()
        except ValueError:
            pass
    
    # Filtro de data mínima
    if data_deposito and data_deposito.year < ANO_DEPOSITO_MINIMO:
        return None
    
    tipo_pessoa = classificar_tipo_pessoa(titular_nome)
    
    return {
        "numero_processo": numero,
        "marca_nome": marca_nome,
        "titular_nome": titular_nome,
        "titular_documento": titular_doc,
        "titular_uf": titular_uf,
        "classe_nice": classe_nice,
        "codigo_ipas": codigo_ipas,
        "data_deposito": data_deposito,
        "tipo_pessoa": tipo_pessoa,
        "codigos_ocorridos": [codigo_ipas],
        "detalhes_processos": detalhes,
    }


# ============================================================
# PERSISTÊNCIA NO BANCO
# ============================================================

def _upsert_processo(session, dados: dict, numero_rpi: int | None):
    """Insere ou atualiza um Processo no banco."""
    existente = session.query(Processo).get(dados["numero_processo"])
    if existente:
        existente.marca_nome = dados["marca_nome"]
        existente.codigo_ipas = dados["codigo_ipas"]
        existente.titular_nome = dados["titular_nome"]
        existente.titular_documento = dados["titular_documento"]
        existente.classe_nice = dados["classe_nice"]
    else:
        session.add(Processo(
            numero_processo=dados["numero_processo"],
            marca_nome=dados["marca_nome"],
            titular_nome=dados["titular_nome"],
            titular_documento=dados["titular_documento"],
            classe_nice=dados["classe_nice"],
            codigo_ipas=dados["codigo_ipas"],
            titular_uf=dados["titular_uf"],
            data_deposito=dados["data_deposito"],
            numero_rpi=numero_rpi,
        ))


def _upsert_lead(session, dados: dict, leads_cache: dict) -> bool:
    """
    Insere ou agrupa um Lead no banco.
    Retorna True se criou um lead novo.
    """
    titular = dados.get("titular_nome")
    if not titular:
        return False
    
    lead_existente = leads_cache.get(titular)
    
    if not lead_existente:
        lead_existente = session.query(Lead).join(Processo).filter(
            Processo.titular_nome == titular
        ).first()
        if lead_existente:
            leads_cache[titular] = lead_existente
    
    if lead_existente:
        # Agrupar: mesmo titular com múltiplos ataques
        lead_existente.quantidade_ataques = (lead_existente.quantidade_ataques or 0) + 1
        
        novo_det = dados.get("detalhes_processos")
        if novo_det:
            if lead_existente.detalhes_processos:
                lead_existente.detalhes_processos += f" || {novo_det}"
            else:
                lead_existente.detalhes_processos = novo_det
        
        lead_existente.score = min((lead_existente.score or 0) + 10, 100)
        lead_existente.classificacao = classificar_lead(lead_existente.score)
        return False
    
    # Criar novo lead
    score = calcular_score_inicial(dados)
    novo_lead = Lead(
        numero_processo=dados["numero_processo"],
        score=score,
        classificacao=classificar_lead(score),
        tipo_pessoa=dados.get("tipo_pessoa"),
        quantidade_ataques=1,
        detalhes_processos=dados.get("detalhes_processos"),
        status="PENDENTE",
    )
    session.add(novo_lead)
    leads_cache[titular] = novo_lead
    return True


# ============================================================
# PARSER PRINCIPAL (STREAMING)
# ============================================================

def parsear_xml(caminho_xml: Path, numero_rpi: int | None = None):
    """
    Faz streaming do XML gigante usando iterparse.
    NÃO carrega o arquivo inteiro na memória.
    """
    session = get_session()
    
    encoding = detectar_encoding(caminho_xml)
    print(f"📄 Encoding detectado: {encoding}")
    print(f"📂 Arquivo: {caminho_xml}")
    print(f"🔍 Filtrando IPAS: {TARGET_CODES}")
    print(f"{'='*60}")
    
    total_processos = 0
    total_relevantes = 0
    total_leads = 0
    leads_cache = {}
    
    try:
        context = etree.iterparse(
            str(caminho_xml),
            events=("end",),
            tag=TAG_PROCESSO,
            encoding=encoding,
            recover=True,
        )
        
        for _event, elem in context:
            total_processos += 1
            
            if total_processos % 5000 == 0:
                print(f"  ... {total_processos} processos lidos, {total_relevantes} relevantes")
            
            dados = extrair_dados_processo(elem)
            
            # Limpar memória do elemento XML (CRUCIAL pro iterparse)
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
            
            if dados is None:
                continue
            
            total_relevantes += 1
            
            _upsert_processo(session, dados, numero_rpi)
            
            if _upsert_lead(session, dados, leads_cache):
                total_leads += 1
            
            if total_relevantes % 500 == 0:
                session.commit()
        
        session.commit()
        
        if numero_rpi:
            rpi = session.query(RPIHistory).get(numero_rpi)
            if rpi:
                rpi.total_processos = total_relevantes
                rpi.status = "COMPLETED"
                session.commit()
        
        print(f"\n{'='*60}")
        print(f"✅ Parsing concluído!")
        print(f"   📊 Total no XML:    {total_processos:,}")
        print(f"   🎯 Relevantes:      {total_relevantes:,}")
        print(f"   🔥 Leads criados:   {total_leads:,}")
        print(f"\n   Próximo passo: python enriquecimento.py")
        
        return {
            "total_xml": total_processos,
            "relevantes": total_relevantes,
            "leads": total_leads,
        }
        
    except Exception as e:
        session.rollback()
        if numero_rpi:
            try:
                rpi = session.query(RPIHistory).get(numero_rpi)
                if rpi:
                    rpi.status = "FAILED"
                    session.commit()
            except Exception:
                pass
        raise RuntimeError(f"Erro ao parsear XML: {e}") from e
    finally:
        session.close()


# ============================================================
# RESOLUÇÃO DE CAMINHO XML
# ============================================================

def _resolver_caminho_xml(args) -> tuple[Path, int | None]:
    """
    Resolve o caminho do XML e o número da RPI a partir dos argumentos.
    Retorna (caminho, numero_rpi).
    """
    if args.arquivo:
        caminho = Path(args.arquivo)
        if not caminho.exists():
            print(f"❌ Arquivo não encontrado: {caminho}")
            sys.exit(1)
        return caminho, args.rpi
    
    if args.rpi:
        session = get_session()
        rpi = session.query(RPIHistory).get(args.rpi)
        session.close()
        
        if not rpi or not rpi.arquivo_path:
            print(f"❌ RPI {args.rpi} não encontrada no banco ou sem arquivo.")
            sys.exit(1)
        
        caminho = Path(rpi.arquivo_path)
        if not caminho.exists():
            print(f"❌ Arquivo {caminho} não existe no disco.")
            sys.exit(1)
        return caminho, args.rpi
    
    # Modo automático: última RPI com status COMPLETED
    session = get_session()
    ultima_rpi = session.query(RPIHistory).filter_by(
        status="COMPLETED"
    ).order_by(RPIHistory.numero_rpi.desc()).first()
    session.close()
    
    if not ultima_rpi or not ultima_rpi.arquivo_path:
        print("❌ Nenhuma RPI encontrada. Execute primeiro: python download_rpi.py")
        sys.exit(1)
    
    caminho = Path(ultima_rpi.arquivo_path)
    if not caminho.exists():
        print(f"❌ Arquivo {caminho} não existe. Re-execute o download.")
        sys.exit(1)
    
    print(f"📰 Usando última RPI: {ultima_rpi.numero_rpi}")
    return caminho, ultima_rpi.numero_rpi


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Parser streaming do XML da RPI")
    parser.add_argument("--arquivo", type=str, help="Caminho direto para o XML")
    parser.add_argument("--rpi", type=int, help="Número da RPI (ex: 202610)")
    args = parser.parse_args()
    
    criar_tabelas()
    
    caminho, numero_rpi = _resolver_caminho_xml(args)
    parsear_xml(caminho, numero_rpi=numero_rpi)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⛔ Cancelado pelo usuário.")
        sys.exit(1)
    except Exception as e:
        print(f"\n💀 Erro fatal: {e}")
        sys.exit(1)

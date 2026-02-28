"""
parser_xml.py — Worker 02: Parser Streaming do XML da RPI

Baseado na Directive 02 (adaptado para o Sistema 1 — sem Redis, sem S3).
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
from datetime import date, datetime
from pathlib import Path

from lxml import etree
import re

from config import IPAS_CODES, IPAS_LEAD_CODES, IPAS_RENOVACAO_CODES, TEMP_DIR
from db import criar_tabelas, get_session, RPIHistory, Processo, Lead

# ============================================================
# NLP E EXCLUSÕES (Vendas)
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

def is_concorrente(nome):
    if not nome: return False
    nome_lower = nome.lower()
    return any(keyword in nome_lower for keyword in EXCLUDE_KEYWORDS)

def classificar_tipo_pessoa(nome):
    if not nome: return "Pessoa Física"
    if PJ_PATTERN.search(nome): return "Pessoa Jurídica"
    return "Pessoa Física"

def extrair_oponente(texto):
    if not texto: return "N/A"
    match = re.search(r'(?:oposta por|oposto por|por)\s*(.+)', texto, re.IGNORECASE)
    if match:
        nome_oponente = match.group(1).strip()
        if nome_oponente.endswith('.'): nome_oponente = nome_oponente[:-1]
        return nome_oponente
    return "N/A"


# ============================================================
# CONSTANTES
# ============================================================

# Tags XML que o INPI usa (aprendidas das referências)
TAG_PROCESSO = "processo"
TAG_DESPACHO = "despacho"
TAG_MARCA = "marca"
TAG_TITULAR = "titular"
TAG_PROCURADOR = "procurador"
TAG_CLASSE_NICE = "classe-nice"

# Filtro de data mínima de depósito (referência: extrator_leads usava 2010)
ANO_DEPOSITO_MINIMO = 2010


# ============================================================
# DETECÇÃO DE ENCODING
# ============================================================

def detectar_encoding(caminho_xml: Path) -> str:
    """
    Lê os primeiros bytes do XML para detectar o encoding.
    Aprendizado: o INPI alterna entre ISO-8859-1 e UTF-8 sem aviso.
    """
    with open(caminho_xml, "rb") as f:
        cabecalho = f.read(200)
    
    if b"iso-8859-1" in cabecalho.lower():
        return "iso-8859-1"
    elif b"utf-8" in cabecalho.lower():
        return "utf-8"
    elif b"latin" in cabecalho.lower():
        return "iso-8859-1"
    else:
        # Default seguro
        return "utf-8"


# ============================================================
# EXTRAÇÃO DE DADOS DO PROCESSO
# ============================================================

def extrair_dados_processo(elem_processo) -> dict | None:
    """
    Extrai os campos relevantes de um elemento <processo> do XML.
    Retorna None se o processo deve ser descartado.
    """
    numero = elem_processo.get("numero", "").strip()
    if not numero:
        return None
    
    dados = {
        "numero_processo": numero,
        "marca_nome": None,
        "titular_nome": None,
        "titular_documento": None,
        "titular_pais": None,
        "tem_procurador": False,
        "procurador_nome": None,
        "classe_nice": None,
        "codigo_ipas": None,
        "data_deposito": None,
        "tipo_marca": None,
    }
    
    # --- Despachos (IPAS) ---
    despachos_relevantes = []
    codigos_ocorridos = []
    detalhes_processos_list = []
    
    for desp in elem_processo.iter(TAG_DESPACHO):
        codigo = desp.get("codigo", "").strip().upper()
        if not codigo: continue
        
        codigos_ocorridos.append(codigo)
        
        if codigo in IPAS_CODES:
            despachos_relevantes.append(codigo)
            
        if codigo in {'IPAS024', 'IPAS423', 'IPAS400'} or codigo in IPAS_LEAD_CODES:
            tipo_procedimento = "Outro"
            if codigo == "IPAS400": tipo_procedimento = "Nulidade"
            elif codigo == "IPAS423": tipo_procedimento = "Oposição"
            elif codigo == "IPAS024": tipo_procedimento = "Indeferimento"
            else: tipo_procedimento = IPAS_CODES.get(codigo, "Despacho")
            
            texto_comp_elem = desp.find('texto-complementar')
            texto_comp = texto_comp_elem.text if (texto_comp_elem is not None and texto_comp_elem.text) else ""
            texto_comp = texto_comp.replace('\n', ' ').replace('\r', '').strip()
            
            quem_pediu = extrair_oponente(texto_comp) if codigo != "IPAS024" else "INPI (Governo)"
            # A marca_nome será preenchida logo abaixo, atualizaremos a string se for necessário lá na frente
            detalhes_processos_list.append({
                "tipo": tipo_procedimento,
                "origem": quem_pediu
            })
    
    if not despachos_relevantes:
        return None  # Nenhum despacho relevante, pular
    
    # Usar o despacho mais recente (último na lista)
    dados["codigo_ipas"] = despachos_relevantes[-1]
    
    # --- Marca ---
    marca_elem = elem_processo.find(TAG_MARCA)
    if marca_elem is not None:
        nome_elem = marca_elem.find("nome")
        if nome_elem is not None and nome_elem.text:
            dados["marca_nome"] = nome_elem.text.strip()
        
        apresentacao = marca_elem.get("apresentacao", "").lower()
        dados["tipo_marca"] = apresentacao  # "nominativa", "mista", "figurativa"
    
    # Descartar figurativas sem nome (Directive 02, passo 9)
    if dados["tipo_marca"] == "figurativa" and not dados["marca_nome"]:
        return None
    
    # --- Titular ---
    titular_elem = elem_processo.find(TAG_TITULAR)
    if titular_elem is not None:
        nome_razao = titular_elem.get("nome-razao-social", "").strip() or None
        if nome_razao and is_concorrente(nome_razao):
            return None # Filtro de concorrente
        
        dados["titular_nome"] = nome_razao
        dados["titular_documento"] = titular_elem.get("cnpj-cpf", "").strip() or None
        dados["titular_pais"] = titular_elem.get("pais", "").strip().upper() or None
        dados["titular_uf"] = titular_elem.get("uf", "").strip() or None
        dados["tipo_pessoa"] = classificar_tipo_pessoa(nome_razao)
        
    dados["codigos_ocorridos"] = codigos_ocorridos
    
    # Finalizando a string de detalhes com a marca
    resumos = []
    m_nome = dados.get("marca_nome") or "N/A"
    for dp in detalhes_processos_list:
        resumos.append(f"[{dp['tipo']}] Proc: {numero} - Marca: {m_nome} - Origem: {dp['origem']}")
    dados["detalhes_processos"] = " || ".join(resumos) if resumos else ""
    
    # Descartar estrangeiros (referência: só processar BR)
    if dados["titular_pais"] and dados["titular_pais"] != "BR":
        return None
    
    # --- Procurador ---
    proc_elem = elem_processo.find(TAG_PROCURADOR)
    if proc_elem is not None:
        nome_proc = proc_elem.get("nome-razao-social", "").strip()
        if nome_proc:
            dados["tem_procurador"] = True
            dados["procurador_nome"] = nome_proc
    
    # --- Classe NICE ---
    classe_elem = elem_processo.find(TAG_CLASSE_NICE)
    if classe_elem is not None:
        dados["classe_nice"] = classe_elem.get("codigo", "").strip() or None
    
    # --- Data de Depósito ---
    deposito_elem = elem_processo.find("data-deposito")
    if deposito_elem is not None and deposito_elem.text:
        try:
            dados["data_deposito"] = datetime.strptime(
                deposito_elem.text.strip(), "%d/%m/%Y"
            ).date()
        except ValueError:
            pass  # Formato inesperado, ignorar
    
    # Filtro de data mínima
    if dados["data_deposito"] and dados["data_deposito"].year < ANO_DEPOSITO_MINIMO:
        return None
    
    return dados


# ============================================================
# LEAD SCORING INICIAL (Baseado no Extrator_Consolidado do Usuário)
# ============================================================

def calcular_score_inicial(dados: dict) -> int:
    """
    Calcula o score inicial do lead somando o peso de todos os despachos observados,
    mais o bônus de Pessoa Jurídica e sem procurador.
    """
    score = 0
    codigos = dados.get("codigos_ocorridos", [])
    tipo_pessoa = dados.get("tipo_pessoa", "Pessoa Física")
    
    # Regra 1: Capacidade de pagamento
    if tipo_pessoa == "Pessoa Jurídica":
        score += 30
        
    # Regra 2: Bônus por não ter procurador (do pipeline original)
    if not dados.get("tem_procurador", True):
        score += 30
        
    # Regra 3: Peso dos despachos no alvo
    for cod in codigos:
        if cod == "IPAS400": score += 50    # Nulidade
        elif cod == "IPAS423": score += 40  # Oposição
        elif cod == "IPAS024": score += 15  # Indeferimento
        elif cod == "IPAS029": score += 40  # Recurso
        elif cod == "IPAS158": score += 45  # Concessão
        elif cod == "IPAS025": score += 35  # Indeferimento final
        elif cod == "IPAS009": score += 20  # Pedido novo
    
    return min(score, 100) # Mantém o cap de 100 original


def classificar_lead(score: int) -> str:
    """Classifica o lead pelo score em Tiers."""
    if score >= 70:
        return "TIER A (Alta Prioridade)"
    elif score >= 40:
        return "TIER B (Prioridade Média)"
    else:
        return "TIER C (Baixa Prioridade)"


# ============================================================
# PARSER PRINCIPAL (STREAMING)
# ============================================================

def parsear_xml(caminho_xml: Path, numero_rpi: int | None = None):
    """
    Faz streaming do XML gigante usando iterparse.
    NÃO carrega o arquivo inteiro na memória.
    """
    criar_tabelas()
    session = get_session()
    
    encoding = detectar_encoding(caminho_xml)
    print(f"📄 Encoding detectado: {encoding}")
    print(f"📂 Arquivo: {caminho_xml}")
    print(f"🔍 Filtrando IPAS: {list(IPAS_CODES.keys())}")
    print(f"{'='*60}")
    
    total_processos_xml = 0
    total_relevantes = 0
    total_leads = 0
    
    try:
        # iterparse: lê o XML como stream, processando tag por tag
        context = etree.iterparse(
            str(caminho_xml),
            events=("end",),
            tag=TAG_PROCESSO,
            encoding=encoding,
            recover=True  # Tolera erros no XML (o INPI às vezes gera XML mal-formado)
        )
        
        for event, elem in context:
            total_processos_xml += 1
            
            # Progresso a cada 5000 processos
            if total_processos_xml % 5000 == 0:
                print(f"  ... {total_processos_xml} processos lidos, {total_relevantes} relevantes")
            
            # Extrair dados
            dados = extrair_dados_processo(elem)
            
            if dados is None:
                # Limpar memória do elemento processado (CRUCIAL pro iterparse)
                elem.clear()
                while elem.getprevious() is not None:
                    del elem.getparent()[0]
                continue
            
            total_relevantes += 1
            
            # Upsert no banco: Processo
            processo_existente = session.query(Processo).get(dados["numero_processo"])
            if processo_existente:
                # Atualizar campos
                processo_existente.marca_nome = dados["marca_nome"]
                processo_existente.codigo_ipas = dados["codigo_ipas"]
                processo_existente.titular_nome = dados["titular_nome"]
                processo_existente.titular_documento = dados["titular_documento"]
                processo_existente.tem_procurador = dados["tem_procurador"]
                processo_existente.procurador_nome = dados["procurador_nome"]
                processo_existente.classe_nice = dados["classe_nice"]
            else:
                novo_processo = Processo(
                    numero_processo=dados["numero_processo"],
                    marca_nome=dados["marca_nome"],
                    titular_nome=dados["titular_nome"],
                    titular_documento=dados["titular_documento"],
                    tem_procurador=dados["tem_procurador"],
                    procurador_nome=dados["procurador_nome"],
                    classe_nice=dados["classe_nice"],
                    codigo_ipas=dados["codigo_ipas"],
                    titular_uf=dados.get("titular_uf"),
                    data_deposito=dados["data_deposito"],
                    numero_rpi=numero_rpi,
                )
                session.add(novo_processo)
            
            # Criar Lead se elegível (sem procurador + IPAS de oportunidade)
            ipas = dados["codigo_ipas"]
            sem_procurador = not dados["tem_procurador"]
            
            eh_lead = (
                (sem_procurador and ipas in IPAS_LEAD_CODES) or
                (ipas in IPAS_RENOVACAO_CODES)
            )
            
            if eh_lead:
                # Verificar se lead já existe
                lead_existente = session.query(Lead).filter_by(
                    numero_processo=dados["numero_processo"]
                ).first()
                
                if not lead_existente:
                    score = calcular_score_inicial(dados)
                    classificacao = classificar_lead(score)
                    
                    novo_lead = Lead(
                        numero_processo=dados["numero_processo"],
                        score=score,
                        classificacao=classificacao,
                        tipo_pessoa=dados.get("tipo_pessoa"),
                        quantidade_ataques=len(dados.get("codigos_ocorridos", [])),
                        detalhes_processos=dados.get("detalhes_processos"),
                        status="PENDENTE",
                    )
                    session.add(novo_lead)
                    total_leads += 1
            
            # Commit a cada 500 registros (performance)
            if total_relevantes % 500 == 0:
                session.commit()
            
            # CRUCIAL: limpar memória do elemento XML já processado
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
        
        # Commit final
        session.commit()
        
        # Atualizar rpi_history se temos o número
        if numero_rpi:
            rpi = session.query(RPIHistory).get(numero_rpi)
            if rpi:
                rpi.total_processos = total_relevantes
                rpi.status = "COMPLETED"
                session.commit()
        
        print(f"\n{'='*60}")
        print(f"✅ Parsing concluído!")
        print(f"   📊 Total no XML:    {total_processos_xml:,}")
        print(f"   🎯 Relevantes:      {total_relevantes:,}")
        print(f"   🔥 Leads criados:   {total_leads:,}")
        print(f"\n   Próximo passo: python enriquecimento.py")
        
        return {
            "total_xml": total_processos_xml,
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
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Parser streaming do XML da RPI")
    parser.add_argument("--arquivo", type=str, help="Caminho direto para o XML")
    parser.add_argument("--rpi", type=int, help="Número da RPI (ex: 202610)")
    args = parser.parse_args()
    
    criar_tabelas()
    
    if args.arquivo:
        # Modo direto: arquivo específico
        caminho = Path(args.arquivo)
        if not caminho.exists():
            print(f"❌ Arquivo não encontrado: {caminho}")
            sys.exit(1)
        parsear_xml(caminho, numero_rpi=args.rpi)
    
    elif args.rpi:
        # Buscar no banco o caminho do XML dessa RPI
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
        
        parsear_xml(caminho, numero_rpi=args.rpi)
    
    else:
        # Modo automático: pegar a última RPI com status COMPLETED
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
        parsear_xml(caminho, numero_rpi=ultima_rpi.numero_rpi)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⛔ Cancelado pelo usuário.")
        sys.exit(1)
    except Exception as e:
        print(f"\n💀 Erro fatal: {e}")
        sys.exit(1)

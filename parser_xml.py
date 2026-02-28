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

from config import TARGET_CODES, IPAS_RENOVACAO_CODES, TEMP_DIR
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
    Lógica baseada no extrator_consolidado.py do usuário:
    - Se tiver procurador: descartar
    - Só importa: IPAS400, IPAS423, IPAS024
    - Agrupar por titular_nome
    Retorna None se o processo deve ser descartado.
    """
    numero = elem_processo.get("numero", "").strip()
    if not numero:
        return None
    
    # === REGRA 1: PROCURADOR (se tiver, descarta TUDO) ===
    if elem_processo.find("procurador") is not None:
        return None

    dados = {
        "numero_processo": numero,
        "marca_nome": None,
        "titular_nome": None,
        "titular_documento": None,
        "titular_pais": None,
        "titular_uf": None,
        "tem_procurador": False,
        "procurador_nome": None,
        "classe_nice": None,
        "codigo_ipas": None,
        "data_deposito": None,
        "tipo_marca": None,
        "tipo_pessoa": "Pessoa Física",
        "codigos_ocorridos": [],
        "detalhes_processos": None,
    }
    
    despachos_relevantes = []
    detalhes_processos_list = []
    
    # === LOOP DESPACHOS (somente TARGET_CODES) ===
    for desp in elem_processo.iter(TAG_DESPACHO):
        codigo = desp.get("codigo", "").strip().upper()
        if not codigo:
            continue
        
        # Só processa os 3 códigos relevantes
        if codigo not in TARGET_CODES:
            continue
        
        # --- Titular (extrair aqui pois precisamos para filtros) ---
        titular_elem = elem_processo.find(".//titular")
        titular_nome = ""
        titular_uf = ""
        if titular_elem is not None:
            titular_nome = titular_elem.get("nome-razao-social", "").strip()
            titular_uf = titular_elem.get("uf", "").strip()
        
        # Descartar estrangeiros (sem UF)
        if not titular_uf or titular_uf == "N/A":
            break
        
        # Descartar concorrentes (escritórios de marca/advocacia)
        if is_concorrente(titular_nome):
            break
        
        # Tipo de procedimento
        if codigo == "IPAS400":
            tipo_procedimento = "Nulidade"
        elif codigo == "IPAS423":
            tipo_procedimento = "Oposição"
        else:  # IPAS024
            tipo_procedimento = "Indeferimento"
        
        # Extrair marca
        marca_elem = elem_processo.find(".//marca/nome")
        marca_nome = marca_elem.text.strip() if (marca_elem is not None and marca_elem.text) else "N/A"
        
        # Extrair texto complementar (para quem pediu a oposição)
        texto_comp_elem = desp.find("texto-complementar")
        texto_comp = ""
        if texto_comp_elem is not None and texto_comp_elem.text:
            texto_comp = texto_comp_elem.text.replace("\n", " ").replace("\r", "").strip()
        
        quem_pediu = extrair_oponente(texto_comp) if codigo != "IPAS024" else "INPI (Governo)"
        
        resumo = f"[{tipo_procedimento}] Proc: {numero} - Marca: {marca_nome} - Origem: {quem_pediu}"
        detalhes_processos_list.append(resumo)
        despachos_relevantes.append(codigo)
        
        # Preencher dados do titular (igual ao extrator_consolidado faz no defaultdict)
        dados["titular_nome"] = titular_nome
        dados["titular_uf"] = titular_uf
        dados["titular_documento"] = titular_elem.get("cnpj-cpf", "").strip() or None if titular_elem is not None else None
        dados["titulo_pais"] = titular_elem.get("pais", "").strip().upper() or None if titular_elem is not None else None
        dados["tipo_pessoa"] = classificar_tipo_pessoa(titular_nome)
        dados["marca_nome"] = marca_nome
        
        break  # Só pega o PRIMEIRO despacho relevante (igual ao extrator_consolidado)
    
    # Se nenhum despacho relevante, descartar
    if not despachos_relevantes:
        return None
    
    dados["codigo_ipas"] = despachos_relevantes[-1]
    dados["codigos_ocorridos"] = despachos_relevantes
    dados["detalhes_processos"] = " || ".join(detalhes_processos_list) if detalhes_processos_list else None
    
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
            pass
    
    # Filtro de data mínima
    if dados["data_deposito"] and dados["data_deposito"].year < ANO_DEPOSITO_MINIMO:
        return None
    
    return dados


# ============================================================
# LEAD SCORING INICIAL (Baseado no Extrator_Consolidado do Usuário)
# ============================================================

def calcular_score_inicial(dados: dict) -> int:
    """
    Calcula o score inicial do lead.
    Lógica idêntica ao extrator_consolidado.py:
      - PJ: +30
      - IPAS400 (Nulidade): +50
      - IPAS423 (Oposição): +40
      - IPAS024 (Indeferimento): +15
    Pessoas físicas são incluídas mas com pontuação menor.
    """
    score = 0
    codigos = dados.get("codigos_ocorridos", [])
    tipo_pessoa = dados.get("tipo_pessoa", "Pessoa Física")
    
    # Regra 1: Capacidade de pagamento (PJ > PF)
    if tipo_pessoa == "Pessoa Jurídica":
        score += 30
    
    # Regra 2: Gravidade e volume dos despachos
    for cod in codigos:
        if cod == "IPAS400":
            score += 50    # Nulidade (Altíssima)
        elif cod == "IPAS423":
            score += 40   # Oposição (Alta)
        elif cod == "IPAS024":
            score += 15   # Indeferimento (Regular)
    
    return min(score, 100)  # Cap em 100


def classificar_lead(score: int) -> str:
    """Classifica o lead em Tiers (idêntico ao extrator_consolidado.py)."""
    if score >= 70:
        return "A (Alta Prioridade)"
    elif score >= 40:
        return "B (Prioridade Média)"
    else:
        return "C (Baixa Prioridade)"


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
    print(f"🔍 Filtrando IPAS: {TARGET_CODES}")
    print(f"{'='*60}")
    
    total_processos_xml = 0
    total_relevantes = 0
    total_leads = 0
    
    leads_cache = {}  # Cache de 'titular_nome' -> objeto Lead
    
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
            
            # Criar Lead: se chegou aqui, procurador já foi filtrado na extração
            # Basta verificar se tem titular para criar o lead
            eh_lead = (dados["codigo_ipas"] in TARGET_CODES)
            
            if eh_lead:
                titular = dados.get("titular_nome")
                
                if titular:
                    lead_existente = leads_cache.get(titular)
                    
                    if not lead_existente:
                        # Verifica no banco apenas se não estiver no cache
                        lead_existente = session.query(Lead).join(Processo).filter(
                            Processo.titular_nome == titular
                        ).first()
                        if lead_existente:
                            leads_cache[titular] = lead_existente
                    
                    if lead_existente:
                        # Agrupar: atualizar o lead que já existe para o mesmo Titular
                        qtd = len(dados.get("codigos_ocorridos", []))
                        lead_existente.quantidade_ataques = (lead_existente.quantidade_ataques or 0) + qtd
                        
                        novo_det = dados.get("detalhes_processos")
                        if novo_det:
                            if lead_existente.detalhes_processos:
                                lead_existente.detalhes_processos += f" || {novo_det}"
                            else:
                                lead_existente.detalhes_processos = novo_det
                                
                        # Soma um pequeno bônus ao score por ter sofrido ataque múltiplo (e limita em 100)
                        lead_existente.score = min((lead_existente.score or 0) + 10, 100)
                        lead_existente.classificacao = classificar_lead(lead_existente.score)
                    else:
                        # Criar novo lead
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
                        leads_cache[titular] = novo_lead
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

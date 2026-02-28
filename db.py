"""
db.py — Camada de banco de dados do Sistema 1 (Extrator de Leads INPI)

Usa SQLAlchemy para abstrair SQLite (dev) e PostgreSQL (prod).
NÃO faz nenhuma chamada externa. Apenas gerencia o banco local.
"""

from datetime import datetime
from sqlalchemy import (
    create_engine, Column, Integer, Text, Boolean, Date,
    DateTime, ForeignKey, JSON, event
)
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

from config import DATABASE_URL

# ============================================================
# ENGINE E SESSÃO
# ============================================================
engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()


# ============================================================
# MODELS (espelho do schema definido na Arquitetura v2)
# ============================================================

class RPIHistory(Base):
    """Controle de qual RPI já foi processada."""
    __tablename__ = "rpi_history"

    numero_rpi = Column(Integer, primary_key=True)
    data_publicacao = Column(Date, nullable=True)
    arquivo_path = Column(Text, nullable=True)
    status = Column(Text, default="PENDING")  # PENDING | PROCESSING | COMPLETED | FAILED
    total_processos = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)

    processos = relationship("Processo", back_populates="rpi")


class Processo(Base):
    """Processo/marca extraído do XML da RPI."""
    __tablename__ = "processo"

    numero_processo = Column(Text, primary_key=True)
    marca_nome = Column(Text, nullable=True)
    titular_nome = Column(Text, nullable=True)
    titular_documento = Column(Text, nullable=True)  # CNPJ ou CPF
    titular_uf = Column(Text, nullable=True)
    tem_procurador = Column(Boolean, default=False)
    procurador_nome = Column(Text, nullable=True)
    classe_nice = Column(Text, nullable=True)
    codigo_ipas = Column(Text, nullable=True)
    data_deposito = Column(Date, nullable=True)
    numero_rpi = Column(Integer, ForeignKey("rpi_history.numero_rpi"), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    rpi = relationship("RPIHistory", back_populates="processos")
    lead = relationship("Lead", back_populates="processo", uselist=False)


class Lead(Base):
    """Lead enriquecido — output final do pipeline."""
    __tablename__ = "lead"

    id = Column(Integer, primary_key=True, autoincrement=True)
    numero_processo = Column(Text, ForeignKey("processo.numero_processo"), unique=True)
    email = Column(Text, nullable=True)
    telefone = Column(Text, nullable=True)
    email_tipo = Column(Text, nullable=True)  # 'direto' | 'escritorio'
    cnpj_dados = Column(JSON, nullable=True)  # Razão social, endereço, etc.
    tipo_pessoa = Column(Text, nullable=True) # Pessoa Jurídica | Pessoa Física
    score = Column(Integer, default=0)  # 0-100
    classificacao = Column(Text, nullable=True)  # TIER A, B, C
    quantidade_ataques = Column(Integer, default=0)
    detalhes_processos = Column(Text, nullable=True)
    parecer_ia = Column(Text, nullable=True)
    argumento_vendas = Column(Text, nullable=True)
    fonte_enriquecimento = Column(Text, nullable=True)  # 'infosimples' | 'pepi_pdf' | 'api_cnpj'
    status = Column(Text, default="PENDENTE")  # PENDENTE | ENRIQUECIDO | ERRO
    created_at = Column(DateTime, default=datetime.utcnow)

    processo = relationship("Processo", back_populates="lead")


class ContaINPI(Base):
    """Pool de contas INPI para rotação anti-ban."""
    __tablename__ = "conta_inpi"

    id = Column(Integer, primary_key=True, autoincrement=True)
    login = Column(Text, unique=True, nullable=False)
    senha_enc = Column(Text, nullable=False)  # Criptografada com Fernet
    status = Column(Text, default="ATIVA")  # ATIVA | BLOQUEADA | COOLDOWN
    ultimo_uso = Column(DateTime, nullable=True)
    total_processos_hoje = Column(Integer, default=0)
    daily_limit = Column(Integer, default=15)


# ============================================================
# FUNÇÕES UTILITÁRIAS
# ============================================================

def criar_tabelas():
    """Cria todas as tabelas no banco. Seguro para rodar múltiplas vezes."""
    Base.metadata.create_all(engine)


def get_session():
    """Retorna uma sessão do banco. Usar com context manager."""
    return SessionLocal()


# Ativar WAL mode no SQLite para melhor performance de escrita
@event.listens_for(engine, "connect")
def _set_sqlite_pragma(dbapi_conn, connection_record):
    if "sqlite" in DATABASE_URL:
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA journal_mode=WAL;")
        cursor.execute("PRAGMA synchronous=NORMAL;")
        cursor.close()

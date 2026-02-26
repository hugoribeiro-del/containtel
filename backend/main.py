"""
ContaIntel — Backend API
FastAPI + SQLite + Anthropic Claude

Instalação:
    pip install fastapi uvicorn python-multipart openpyxl anthropic aiofiles

Execução:
    uvicorn main:app --reload --port 8000
"""

import os, json, hashlib, uuid, time, secrets
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Optional, List

# JWT auth
try:
    import jwt as pyjwt
except ImportError:
    pyjwt = None

import bcrypt

from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Depends, Security, Request
from collections import deque
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
import sqlite3
import openpyxl
import time
import collections

# ──────────────────────────────────────────
# APP SETUP
# ──────────────────────────────────────────
app = FastAPI(title="ContaIntel API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],       # Permite SharePoint, OneDrive, qualquer origem
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=False,
)

# ── RATE LIMITER ──────────────────────────────────────────────
class _RateLimiter:
    """Token-bucket rate limiter — sem dependências externas."""
    def __init__(self):
        self._buckets: dict[str, deque] = {}

    def _key(self, request: Request) -> str:
        ip = request.headers.get("X-Forwarded-For", request.client.host if request.client else "unknown")
        return ip.split(",")[0].strip()

    def check(self, request: Request, max_calls: int, window_secs: int) -> bool:
        """True = permitido, False = bloqueado."""
        key  = f"{self._key(request)}:{request.url.path}"
        now  = time.monotonic()
        hist = self._buckets.setdefault(key, collections.deque())
        # Limpar janela expirada
        while hist and hist[0] < now - window_secs:
            hist.popleft()
        if len(hist) >= max_calls:
            return False
        hist.append(now)
        return True

_rl = _RateLimiter()

def rate_limit(max_calls: int = 20, window_secs: int = 60):
    """Decorator de rate limiting para endpoints FastAPI."""
    def dependency(request: Request):
        if not _rl.check(request, max_calls, window_secs):
            raise HTTPException(
                status_code=429,
                detail=f"Demasiados pedidos — máximo {max_calls} por {window_secs}s. Tente mais tarde."
            )
    return Depends(dependency)

# ── JWT CONFIG ──
_JWT_SECRET_ENV = os.environ.get("JWT_SECRET")
if not _JWT_SECRET_ENV:
    import sys
    print("⚠️  AVISO: JWT_SECRET não definido — tokens invalidados ao reiniciar. Defina JWT_SECRET nas variáveis de ambiente.", file=sys.stderr)
JWT_SECRET = _JWT_SECRET_ENV or secrets.token_hex(32)
JWT_ALGO = "HS256"
JWT_EXPIRE_HOURS = 8
security_scheme = HTTPBearer(auto_error=False)

BASE_DIR = Path(__file__).parent.parent
# Suporta volume persistente no Railway/Render via variável de ambiente
DATA_DIR = Path(os.environ.get("DATA_DIR", str(BASE_DIR / "data")))
DB_PATH = DATA_DIR / "db" / "containtel.db"
UPLOADS_DIR = DATA_DIR / "uploads"
UPLOADS_DIR.mkdir(parents=True, exist_ok=True)

# Frontend dir
FRONTEND_DIR = BASE_DIR / "frontend"
# Serve frontend static files
if FRONTEND_DIR.exists():
    app.mount("/static-files", StaticFiles(directory=str(FRONTEND_DIR)), name="frontend")


# ──────────────────────────────────────────
# DATABASE
# ──────────────────────────────────────────
def get_db():
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def new_conn():
    """Create a fresh connection - use this in async endpoints."""
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    c = conn.cursor()

    c.executescript("""
    CREATE TABLE IF NOT EXISTS entities (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        nif TEXT UNIQUE NOT NULL,
        legal_form TEXT DEFAULT 'LDA',
        cae_code TEXT,
        regime_irc TEXT DEFAULT 'geral',
        regime_iva TEXT DEFAULT 'geral',
        email TEXT,
        address TEXT,
        is_active INTEGER DEFAULT 1,
        created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS fiscal_years (
        id TEXT PRIMARY KEY,
        entity_id TEXT NOT NULL,
        year INTEGER NOT NULL,
        status TEXT DEFAULT 'open',
        created_at TEXT DEFAULT (datetime('now')),
        FOREIGN KEY(entity_id) REFERENCES entities(id),
        UNIQUE(entity_id, year)
    );

    CREATE TABLE IF NOT EXISTS trial_balance_files (
        id TEXT PRIMARY KEY,
        entity_id TEXT NOT NULL,
        fiscal_year_id TEXT NOT NULL,
        file_name TEXT NOT NULL,
        file_hash TEXT,
        source TEXT DEFAULT 'excel',
        period_start TEXT,
        period_end TEXT,
        total_accounts INTEGER,
        imported_at TEXT DEFAULT (datetime('now')),
        FOREIGN KEY(entity_id) REFERENCES entities(id),
        FOREIGN KEY(fiscal_year_id) REFERENCES fiscal_years(id)
    );

    CREATE TABLE IF NOT EXISTS trial_balance_entries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_id TEXT NOT NULL,
        entity_id TEXT NOT NULL,
        fiscal_year_id TEXT NOT NULL,
        classe INTEGER,
        nivel INTEGER,
        integradora INTEGER DEFAULT 0,
        conta TEXT NOT NULL,
        descricao TEXT,
        deb_periodo REAL DEFAULT 0,
        cred_periodo REAL DEFAULT 0,
        deb_acum REAL DEFAULT 0,
        cred_acum REAL DEFAULT 0,
        devedor REAL DEFAULT 0,
        credor REAL DEFAULT 0,
        saldo_tot REAL DEFAULT 0,
        FOREIGN KEY(file_id) REFERENCES trial_balance_files(id)
    );

    CREATE TABLE IF NOT EXISTS ai_queries (
        id TEXT PRIMARY KEY,
        entity_id TEXT,
        fiscal_year_id TEXT,
        query_type TEXT,
        question TEXT,
        answer TEXT,
        model TEXT DEFAULT 'claude-sonnet-4-6',
        tokens_used INTEGER,
        response_ms INTEGER,
        created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS tax_calculations (
        id TEXT PRIMARY KEY,
        entity_id TEXT NOT NULL,
        fiscal_year_id TEXT NOT NULL,
        calc_type TEXT NOT NULL,
        parameters TEXT,
        result TEXT,
        calculated_at TEXT DEFAULT (datetime('now')),
        FOREIGN KEY(entity_id) REFERENCES entities(id)
    );

    CREATE TABLE IF NOT EXISTS users (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        role TEXT NOT NULL DEFAULT 'gestor',
        is_active INTEGER DEFAULT 1,
        created_at TEXT DEFAULT (datetime('now')),
        last_login TEXT
    );

    CREATE TABLE IF NOT EXISTS user_entities (
        user_id TEXT NOT NULL,
        entity_id TEXT NOT NULL,
        PRIMARY KEY (user_id, entity_id)
    );

    -- ── Índices para performance ──
    CREATE INDEX IF NOT EXISTS idx_tbe_entity_year
        ON trial_balance_entries(entity_id, fiscal_year_id);
    CREATE INDEX IF NOT EXISTS idx_tbe_conta
        ON trial_balance_entries(conta);
    CREATE INDEX IF NOT EXISTS idx_tbf_entity
        ON trial_balance_files(entity_id);
    CREATE INDEX IF NOT EXISTS idx_taxcalc_entity_year
        ON tax_calculations(entity_id, fiscal_year_id, calc_type);
    CREATE INDEX IF NOT EXISTS idx_ai_entity
        ON ai_queries(entity_id);
    CREATE INDEX IF NOT EXISTS idx_fy_entity
        ON fiscal_years(entity_id);

    CREATE TABLE IF NOT EXISTS budgets (
        id        TEXT PRIMARY KEY,
        entity_id TEXT NOT NULL,
        year      INTEGER NOT NULL,
        data      TEXT NOT NULL,
        updated_at TEXT DEFAULT (datetime('now')),
        FOREIGN KEY(entity_id) REFERENCES entities(id),
        UNIQUE(entity_id, year)
    );
    CREATE INDEX IF NOT EXISTS idx_budgets_entity
        ON budgets(entity_id, year);

    CREATE TABLE IF NOT EXISTS conformidade_items (
        id           TEXT PRIMARY KEY,
        entity_id    TEXT NOT NULL,
        year         INTEGER NOT NULL,
        obrigacao_id TEXT NOT NULL,       -- ex: 'ies', 'mod22', 'iva-t1'
        estado       TEXT DEFAULT 'pendente', -- pendente | concluido | nao_aplicavel
        data_conclusao TEXT,
        notas        TEXT,
        updated_at   TEXT DEFAULT (datetime('now')),
        FOREIGN KEY(entity_id) REFERENCES entities(id),
        UNIQUE(entity_id, year, obrigacao_id)
    );

    CREATE INDEX IF NOT EXISTS idx_conf_entity_year
        ON conformidade_items(entity_id, year);

    -- ═══════════════════════════════════════
    -- SAF-T FATURAÇÃO (PT-04)
    -- ═══════════════════════════════════════
    CREATE TABLE IF NOT EXISTS saft_files (
        id           TEXT PRIMARY KEY,
        entity_id    TEXT NOT NULL,
        file_name    TEXT NOT NULL,
        file_hash    TEXT,
        fiscal_year  INTEGER,
        period_start TEXT,
        period_end   TEXT,
        company_name TEXT,
        company_nif  TEXT,
        software     TEXT,
        version      TEXT,
        total_invoices INTEGER DEFAULT 0,
        total_debit  REAL DEFAULT 0,
        total_credit REAL DEFAULT 0,
        imported_at  TEXT DEFAULT (datetime('now')),
        FOREIGN KEY(entity_id) REFERENCES entities(id)
    );
    CREATE INDEX IF NOT EXISTS idx_saft_entity ON saft_files(entity_id);

    -- Clientes do SAF-T (MasterFiles/Customer)
    CREATE TABLE IF NOT EXISTS saft_customers (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        saft_file_id    TEXT NOT NULL,
        entity_id       TEXT NOT NULL,
        customer_id     TEXT,
        account_id      TEXT,
        company_name    TEXT,
        contact         TEXT,
        nif             TEXT,
        country         TEXT DEFAULT 'PT',
        postal_code     TEXT,
        city            TEXT,
        FOREIGN KEY(saft_file_id) REFERENCES saft_files(id)
    );
    CREATE INDEX IF NOT EXISTS idx_saft_cust ON saft_customers(saft_file_id);
    CREATE INDEX IF NOT EXISTS idx_saft_cust_nif ON saft_customers(entity_id, nif);

    -- Produtos/Serviços (MasterFiles/Product)
    CREATE TABLE IF NOT EXISTS saft_products (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        saft_file_id    TEXT NOT NULL,
        entity_id       TEXT NOT NULL,
        product_code    TEXT,
        product_group   TEXT,
        product_desc    TEXT,
        product_type    TEXT,  -- P=produto, S=serviço, O=outros, I=impostos, E=encargos
        unit_of_measure TEXT,
        FOREIGN KEY(saft_file_id) REFERENCES saft_files(id)
    );
    CREATE INDEX IF NOT EXISTS idx_saft_prod ON saft_products(saft_file_id);

    -- Invoices (SalesInvoices/Invoice)
    CREATE TABLE IF NOT EXISTS saft_invoices (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        saft_file_id    TEXT NOT NULL,
        entity_id       TEXT NOT NULL,
        invoice_no      TEXT NOT NULL,
        invoice_type    TEXT,   -- FT, FR, FS, NC, ND, etc.
        invoice_date    TEXT,
        invoice_status  TEXT DEFAULT 'N',  -- N=normal, A=anulada
        customer_id     TEXT,
        nif_cliente     TEXT,
        country_cliente TEXT DEFAULT 'PT',
        gross_total     REAL DEFAULT 0,
        net_total       REAL DEFAULT 0,
        tax_payable     REAL DEFAULT 0,
        settlement      REAL DEFAULT 0,
        serie            TEXT,
        hash_chars      TEXT,    -- primeiros 4 chars do hash
        FOREIGN KEY(saft_file_id) REFERENCES saft_files(id)
    );
    CREATE INDEX IF NOT EXISTS idx_saft_inv_file ON saft_invoices(saft_file_id);
    CREATE INDEX IF NOT EXISTS idx_saft_inv_date ON saft_invoices(entity_id, invoice_date);
    CREATE INDEX IF NOT EXISTS idx_saft_inv_cust ON saft_invoices(entity_id, customer_id);

    -- Invoice Lines (detalhes por linha/produto)
    CREATE TABLE IF NOT EXISTS saft_invoice_lines (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        invoice_id      INTEGER NOT NULL,
        saft_file_id    TEXT NOT NULL,
        entity_id       TEXT NOT NULL,
        line_no         INTEGER,
        product_code    TEXT,
        description     TEXT,
        quantity        REAL DEFAULT 1,
        unit_price      REAL DEFAULT 0,
        credit_amount   REAL DEFAULT 0,
        debit_amount    REAL DEFAULT 0,
        tax_base        REAL DEFAULT 0,
        tax_percentage  REAL DEFAULT 0,
        tax_code        TEXT,  -- NOR, ISE, RED, INT
        tax_amount      REAL DEFAULT 0,
        FOREIGN KEY(invoice_id) REFERENCES saft_invoices(id)
    );
    CREATE INDEX IF NOT EXISTS idx_saft_lines ON saft_invoice_lines(invoice_id);
    CREATE INDEX IF NOT EXISTS idx_saft_lines_prod ON saft_invoice_lines(saft_file_id, product_code);

    -- Tax Table (MasterFiles/TaxTable)
    CREATE TABLE IF NOT EXISTS saft_tax_table (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        saft_file_id    TEXT NOT NULL,
        entity_id       TEXT NOT NULL,
        tax_type        TEXT,   -- IVA, IS, NS
        tax_country     TEXT DEFAULT 'PT',
        tax_code        TEXT,   -- NOR, INT, RED, ISE, OUT
        tax_description TEXT,
        tax_expiration  TEXT,
        tax_percentage  REAL,
        FOREIGN KEY(saft_file_id) REFERENCES saft_files(id)
    );
    """)

    # Seed demo entity — sempre garantido no arranque
    c.execute("""INSERT OR IGNORE INTO entities (id, name, nif, legal_form, cae_code, email, is_active)
                 VALUES (?, ?, ?, ?, ?, ?, 1)""",
              ("demo-1", "HVR Business Consulting, Unipessoal, Lda",
               "513847200", "UNI", "70220", "geral@hvr.pt"))
    c.execute("""INSERT OR IGNORE INTO fiscal_years (id, entity_id, year, status)
                 VALUES (?, ?, ?, ?)""",
              ("fy-demo-2025", "demo-1", 2025, "open"))
    c.execute("""INSERT OR IGNORE INTO fiscal_years (id, entity_id, year, status)
                 VALUES (?, ?, ?, ?)""",
              ("fy-demo-2024", "demo-1", 2024, "closed"))


    # Seed admin user (password: admin123 — change immediately!)
    try:
        import bcrypt as _bcrypt
        _hash = _bcrypt.hashpw(b"admin123", _bcrypt.gensalt()).decode()
    except:
        import hashlib as _hl
        _hash = "sha256:" + _hl.sha256(b"admin123").hexdigest()
    c.execute("""INSERT OR IGNORE INTO users (id, name, email, password_hash, role)
        VALUES ('admin-001', 'Administrador', 'admin@containtel.pt', ?, 'admin')
    """, (_hash,))
    conn.commit()
    conn.close()

init_db()


# ──────────────────────────────────────────
# MODELS
# ──────────────────────────────────────────
class EntityCreate(BaseModel):
    name: str
    nif: str
    legal_form: str = "LDA"
    cae_code: Optional[str] = None
    regime_irc: str = "geral"
    regime_iva: str = "geral"
    email: Optional[str] = None
    address: Optional[str] = None

class ChatRequest(BaseModel):
    question: str
    entity_id: Optional[str] = None
    fiscal_year_id: Optional[str] = None
    context: Optional[dict] = None

class IRCSimulationRequest(BaseModel):
    entity_id: str
    fiscal_year_id: str
    acrescimos: float = 0
    deducoes: float = 0
    prejuizos_anteriores: float = 0
    retencoes_na_fonte: float = 0
    pagamentos_conta: float = 0


# ──────────────────────────────────────────
# HELPERS — EXCEL PARSER
# ──────────────────────────────────────────
def parse_excel_balancete(file_path: Path) -> dict:
    """
    Parse a Portuguese trial balance Excel file.
    Auto-detects columns: Conta, Descrição, Déb/Créd Período, Acumulado, Saldos.
    """
    wb = openpyxl.load_workbook(str(file_path), data_only=True)
    ws = wb.active

    all_rows = list(ws.iter_rows(values_only=True))

    # ── Auto-detect header row ──
    header_row_idx = None
    col_map = {}

    for i, row in enumerate(all_rows):
        row_text = " ".join(str(c).lower() for c in row if c)
        if "conta" in row_text and ("débito" in row_text or "debito" in row_text or "déb" in row_text):
            header_row_idx = i
            # Map columns
            for j, cell in enumerate(row):
                if cell is None:
                    continue
                t = str(cell).lower().strip()
                if t in ("conta", "account"):
                    col_map["conta"] = j
                elif "descrição" in t or "descricao" in t or "description" in t:
                    col_map["descricao"] = j
                elif "nível" in t or "nivel" in t or "level" in t:
                    col_map["nivel"] = j
                elif "classe" in t or "class" in t:
                    col_map["classe"] = j
                elif "integr" in t:
                    col_map["integradora"] = j
                elif "déb" in t and "per" in t:
                    col_map["deb_periodo"] = j
                elif "créd" in t and "per" in t:
                    col_map["cred_periodo"] = j
                elif "déb" in t and "acu" in t:
                    col_map["deb_acum"] = j
                elif "créd" in t and "acu" in t:
                    col_map["cred_acum"] = j
                elif "devedor" in t or "deved" in t:
                    col_map["devedor"] = j
                elif "credor" in t:
                    col_map["credor"] = j
                elif "saldo" in t and "tot" in t:
                    col_map["saldo_tot"] = j
            break

    if header_row_idx is None:
        raise ValueError("Não foi possível detetar o cabeçalho do balancete. "
                         "Verifique se o ficheiro contém colunas 'Conta' e 'Débito'.")

    # ── Extract metadata from header area ──
    meta = {"empresa": None, "nif": None, "periodo": None, "ano": None}
    for row in all_rows[:header_row_idx]:
        for cell in row:
            if cell is None:
                continue
            s = str(cell)
            # NIF pattern: 9 digits possibly with spaces
            import re
            nif_match = re.search(r'\b(\d{9})\b', s)
            if nif_match and not meta["nif"]:
                meta["nif"] = nif_match.group(1)
            if ("lda" in s.lower() or "sa" in s.lower() or "unipessoal" in s.lower()
                    or "consulting" in s.lower() or "lda" in s.lower()):
                if not meta["empresa"] or len(s) > len(meta["empresa"]):
                    meta["empresa"] = s.strip()
            if "exercício" in s.lower() or "balancete" in s.lower():
                meta["periodo"] = s.strip()
                # Extract year
                year_match = re.search(r'\b(20\d\d)\b', s)
                if year_match:
                    meta["ano"] = int(year_match.group(1))

    # ── Parse data rows ──
    entries = []
    for row in all_rows[header_row_idx + 1:]:
        if not any(c is not None for c in row):
            continue

        def get(key, default=None):
            idx = col_map.get(key)
            if idx is None:
                return default
            v = row[idx] if idx < len(row) else None
            return v

        conta = get("conta")
        if conta is None:
            continue
        conta = str(conta).strip()
        if not conta or not any(c.isdigit() for c in conta):
            continue

        def to_float(v):
            if v is None:
                return 0.0
            try:
                return float(v)
            except (ValueError, TypeError):
                return 0.0

        entries.append({
            "classe": int(to_float(get("classe"))) if get("classe") else int(conta[0]) if conta else 0,
            "nivel": int(to_float(get("nivel"))) if get("nivel") else len(conta),
            "integradora": bool(get("integradora")) if get("integradora") is not None else False,
            "conta": conta,
            "descricao": str(get("descricao", "")).strip(),
            "deb_periodo": to_float(get("deb_periodo")),
            "cred_periodo": to_float(get("cred_periodo")),
            "deb_acum": to_float(get("deb_acum")),
            "cred_acum": to_float(get("cred_acum")),
            "devedor": to_float(get("devedor")),
            "credor": to_float(get("credor")),
            "saldo_tot": to_float(get("saldo_tot")),
        })

    return {"meta": meta, "entries": entries, "col_map": col_map}


# ──────────────────────────────────────────
# HELPERS — FINANCIAL CALCULATIONS
# ──────────────────────────────────────────
def calculate_financials(entries: list) -> dict:
    """Calculate all KPIs, ratios and P&L from trial balance entries."""

    def get_acct(conta):
        for r in entries:
            if str(r["conta"]).strip() == str(conta).strip():
                return r
        return None

    def to_f(v):
        try: return float(v or 0)
        except: return 0.0

    def sum_class(cl):
        """Sum devedor - credor for a class, using nivel=2 accounts."""
        total_dev, total_cred = 0, 0
        for r in entries:
            if r["classe"] == cl and r["nivel"] == 2:
                total_dev += to_f(r.get("devedor", 0))
                total_cred += to_f(r.get("credor", 0))
        return total_dev, total_cred

    def get_val(conta, field="devedor"):
        """Get value for a specific account."""
        r = get_acct(conta)
        if not r: return 0.0
        return to_f(r.get(field, 0))

    # ── RENDIMENTOS (Classe 7) — credor = rendimento ──
    r7_dev, r7_cred = sum_class(7)
    total_rendimentos = r7_cred - r7_dev

    # ── GASTOS (Classe 6) — devedor = gasto ──
    r6_dev, r6_cred = sum_class(6)
    total_gastos = r6_dev - r6_cred

    resultado_liquido = total_rendimentos - total_gastos

    # ── COMPONENTES P&L ──
    fse = get_val("62", "devedor")
    gastos_pessoal = get_val("63", "devedor")
    dep_val = get_val("64", "devedor")
    outros_gastos = get_val("68", "devedor")
    fin_val = get_val("69", "devedor")
    prestacoes = get_val("72", "credor")
    subsidios = get_val("75", "credor")
    outros_rend = get_val("78", "credor")

    # EBITDA = RL + IRC contab. + Gastos financiamento + Depreciações (EBITDA = resultado antes de tudo)
    irc_contab_simple = get_val("8122", "devedor") or get_val("812", "devedor") or get_val("81", "devedor")
    ebitda = resultado_liquido + irc_contab_simple + fin_val + dep_val

    # ── BALANÇO ──
    caixa_dev = get_val("11", "devedor")
    caixa_cred = get_val("11", "credor")
    depositos = get_val("12", "devedor")
    clientes_val = get_val("21", "devedor")
    fornec_val = get_val("22", "credor")
    pessoal_pass = get_val("23", "credor")
    estado_val = get_val("24", "credor")
    # Conta 25: 251x = financiamentos MLP, 252x = empréstimos CP — separar por subconta
    fin_mlp_sub = sum(to_f(r.get("credor", 0)) for r in entries if str(r["conta"]).startswith("251"))
    fin_cp_sub  = sum(to_f(r.get("credor", 0)) for r in entries if str(r["conta"]).startswith("252"))
    financiamentos_val = fin_mlp_sub if (fin_mlp_sub or fin_cp_sub) else get_val("25", "credor")
    financ_cp = fin_cp_sub  # empréstimos correntes (CP)
    outras_crp = get_val("27", "credor")
    diferimentos = get_val("28", "devedor")
    ativo_tang = get_val("43", "devedor")
    inv_fin = get_val("41", "devedor")
    capital_val = get_val("51", "credor")
    reservas_val = get_val("55", "credor")
    res_trans = get_val("56", "credor")

    disp = depositos + max(0, caixa_dev - caixa_cred)
    ac = disp + clientes_val + diferimentos
    anc = ativo_tang + inv_fin
    at_total = ac + anc

    pc = fornec_val + pessoal_pass + estado_val + outras_crp
    pnc = financiamentos_val
    passivo = pc + pnc
    cp = capital_val + reservas_val + res_trans + resultado_liquido

    # ── RÁCIOS ──
    def sd(a, b): return round(a / b, 4) if b else 0

    liq_geral = sd(ac, pc)
    liq_red = sd(ac - inventarios, pc)   # exclui existências (menos líquidas)
    liq_im = sd(disp, pc)
    solvabilidade = sd(cp, passivo)
    autonomia = sd(cp, at_total)
    endividamento = sd(passivo, at_total)
    mg_ebitda = sd(ebitda, total_rendimentos) * 100
    mg_liquida = sd(resultado_liquido, total_rendimentos) * 100
    roe = sd(resultado_liquido, cp) * 100 if cp > 0 else 0
    roa = sd(ebit, at_total) * 100 if at_total > 0 else 0
    pmr = sd(clientes_val * 365, total_rendimentos) if total_rendimentos > 0 else 0
    pmp = sd(fornec_val * 365, fse) if fse > 0 else 0
    ciclo_caixa = pmr - pmp

    # ── IRC ──
    rai = resultado_liquido
    mc = max(0, rai)
    irc_bruto = min(mc, 50000) * 0.17 + max(0, mc - 50000) * 0.21
    taxa_ef = sd(irc_bruto, rai) * 100 if rai > 0 else 0

    # ── ALERTAS ──
    alerts = []
    if pmr > 90:
        alerts.append({"type": "risk", "severity": "high",
                        "title": f"PMR de {pmr:.0f} dias",
                        "desc": "Prazo médio de recebimento acima dos 90 dias recomendados."})
    if liq_geral < 1.0 and liq_geral > 0:
        alerts.append({"type": "risk", "severity": "critical",
                        "title": "Liquidez Geral abaixo de 1",
                        "desc": f"Rácio {liq_geral:.2f} — empresa pode não cobrir passivo corrente."})
    if 0 < solvabilidade < 0.2:
        alerts.append({"type": "risk", "severity": "critical",
                        "title": "Risco Art. 35.º CIRE",
                        "desc": f"Solvabilidade {solvabilidade:.2f} — risco de insolvência técnica."})
    if mg_liquida > 10:
        alerts.append({"type": "opportunity", "severity": "info",
                        "title": f"Margem líquida de {mg_liquida:.1f}%",
                        "desc": "Rentabilidade saudável para o setor de consultoria."})

    # ── P&L DETAIL ──
    rendimentos_detail = [
        {"conta": "72", "descricao": "Prestações de serviços", "valor": prestacoes},
        {"conta": "75", "descricao": "Subsídios à exploração", "valor": subsidios},
        {"conta": "78", "descricao": "Outros rendimentos", "valor": outros_rend},
    ]
    gastos_detail = [
        {"conta": "62", "descricao": "Fornecimentos e serviços externos", "valor": fse},
        {"conta": "63", "descricao": "Gastos com o pessoal", "valor": gastos_pessoal},
        {"conta": "64", "descricao": "Depreciações e amortizações", "valor": dep_val},
        {"conta": "68", "descricao": "Outros gastos", "valor": outros_gastos},
        {"conta": "69", "descricao": "Gastos de financiamento", "valor": fin_val},
    ]
    # Filter zero values
    rendimentos_detail = [r for r in rendimentos_detail if r["valor"] > 0]
    gastos_detail = [g for g in gastos_detail if g["valor"] > 0]

    # ── DEMONSTRAÇÃO DE RESULTADOS COMPLETA (NCRF) ──
    vendas = get_val("71", "credor")           # 71=Vendas de mercadorias/produtos
    # prestacoes já calculado acima (conta 72)
    vn_total = vendas + prestacoes             # VN = 71 + 72
    cmvmc = get_val("61", "devedor")            # 61=CMVMC
    mg_bruta = vn_total - cmvmc  # Margem bruta sobre VN
    var_prod = get_val("73", "credor") - get_val("73", "devedor")  # 73=Variação prod
    outros_rend_exp = get_val("74", "credor") + get_val("75", "credor") + get_val("76", "credor") + get_val("78", "credor")
    rbe = mg_bruta + var_prod + outros_rend_exp - fse - gastos_pessoal
    impar_inv = get_val("65", "devedor")        # 65=Imparidades
    prov = get_val("67", "devedor")             # 67=Provisões
    outros_gastos_op = get_val("68", "devedor")
    ebit = rbe - dep_val - impar_inv - prov - outros_gastos_op
    rend_fin = get_val("79", "credor")          # 79=Rendimentos financeiros
    rai_val = ebit + rend_fin - fin_val
    irc_contab = get_val("8122", "devedor") or get_val("812", "devedor")
    resultado_liquido_dr = rai_val - irc_contab

    # ── BALANÇO DETALHADO (NCRF) ──
    inventarios = get_val("32", "devedor") + get_val("33", "devedor") + get_val("34", "devedor") + get_val("35", "devedor")
    estado_dev = get_val("24", "devedor")       # Estado devedor (IVA a recuperar)
    acionistas = get_val("26", "devedor")       # Acionistas
    outras_crp_dev = get_val("27", "devedor")
    inv_prop = get_val("42", "devedor")         # 42=Prop. investimento
    ativo_intang = get_val("44", "devedor") + get_val("45", "devedor")  # 44=Ativos intangíveis
    partic_rel = get_val("41", "devedor")
    outros_anc = get_val("46", "devedor") + get_val("47", "devedor")
    # Passivo detalhado
    financ_cp = get_val("25", "credor") if financiamentos_val == 0 else 0  # já em PNC
    outras_cp_pass = get_val("26", "credor") + get_val("28", "credor")
    # Capital próprio detalhado
    res_liquido_ant = get_val("56", "credor")
    acoes_proprias = get_val("52", "devedor")
    outras_reservas = get_val("53", "credor") + get_val("54", "credor") + get_val("55", "credor")
    excedentes = get_val("58", "credor")

    # Recalculate with full detail
    ac_full = disp + clientes_val + inventarios + estado_dev + outras_crp_dev + diferimentos
    anc_full = ativo_tang + ativo_intang + inv_prop + partic_rel + inv_fin + outros_anc
    at_full = ac_full + anc_full
    if at_full == 0: at_full = at_total  # fallback

    cp_full = capital_val - acoes_proprias + outras_reservas + excedentes + res_trans + resultado_liquido
    if cp_full == 0: cp_full = cp

    return {
        "pnl": {
            "total_rendimentos": round(total_rendimentos, 2),
            "total_gastos": round(total_gastos, 2),
            "resultado_bruto": round(total_rendimentos - total_gastos, 2),
            "depreciacao": round(dep_val, 2),
            "gastos_financiamento": round(fin_val, 2),
            "ebitda": round(ebitda, 2),
            "resultado_liquido": round(resultado_liquido, 2),
            "rendimentos_detail": rendimentos_detail,
            "gastos_detail": gastos_detail,
            # Full DR (NCRF)
            "vn": round(vn_total, 2),
            "cmvmc": round(cmvmc, 2),
            "margem_bruta": round(mg_bruta, 2),
            "fse": round(fse, 2),
            "gastos_pessoal": round(gastos_pessoal, 2),
            "rbe": round(rbe, 2),
            "imparidades": round(impar_inv, 2),
            "provisoes": round(prov, 2),
            "ebit": round(ebit, 2),
            "rendimentos_financeiros": round(rend_fin, 2),
            "rai": round(rai_val, 2),
            "irc_contabilistico": round(irc_contab, 2),
            "resultado_liquido_dr": round(resultado_liquido_dr if resultado_liquido_dr else resultado_liquido, 2),
            "outros_rendimentos": round(outros_rend_exp, 2),
            "outros_gastos": round(outros_gastos_op, 2),
        },
        "balanco": {
            "ativo_corrente": round(ac_full, 2),
            "ativo_nao_corrente": round(anc_full, 2),
            "ativo_total": round(at_full, 2),
            "passivo_corrente": round(pc, 2),
            "passivo_nao_corrente": round(pnc, 2),
            "passivo_total": round(passivo, 2),
            "capital_proprio": round(cp_full, 2),
            "disponibilidades": round(disp, 2),
            "clientes": round(clientes_val, 2),
            "existencias": round(inventarios, 2),
            # Detalhe ativo
            "inventarios": round(inventarios, 2),
            "estado_devedor": round(estado_dev, 2),
            "ativo_tangivel": round(ativo_tang, 2),
            "ativo_intangivel": round(ativo_intang, 2),
            "inv_propriedades": round(inv_prop, 2),
            "participacoes": round(partic_rel, 2),
            # Detalhe passivo
            "financiamentos_cp": round(financ_cp, 2),
            "fornecedores": round(fornec_val, 2),
            "estado_credor": round(estado_val, 2),
            "pessoal_passivo": round(pessoal_pass, 2),
            "financiamentos_mlp": round(financiamentos_val, 2),
            # Detalhe capital próprio
            "capital_social": round(capital_val, 2),
            "reservas": round(outras_reservas, 2),
            "resultados_transitados": round(res_trans, 2),
            "excedentes_revalorizacao": round(excedentes, 2),
            "passivo_nao_corrente": round(pnc, 2),
        },
        "ratios": {
            "liquidez_geral": round(liq_geral, 2),
            "liquidez_reduzida": round(liq_red, 2),
            "liquidez_imediata": round(liq_im, 2),
            "solvabilidade": round(solvabilidade, 2),
            "autonomia_financeira": round(autonomia, 2),
            "endividamento": round(endividamento, 2),
            "margem_bruta": round(mg_ebitda, 1),
            "margem_ebitda": round(mg_ebitda, 1),
            "margem_liquida": round(mg_liquida, 1),
            "roe": round(roe, 1),
            "roa": round(roa, 1),
            "pmr_dias": round(pmr, 0),
            "pmp_dias": round(pmp, 0),
            "ciclo_caixa_dias": round(ciclo_caixa, 0),
        },
        "irc_estimate": {
            "rai": round(rai, 2),
            "materia_coletavel": round(mc, 2),
            "is_pme": True,
            "taxa_reduzida_pct": 17.0,
            "taxa_normal_pct": 21.0,
            "irc_bruto": round(irc_bruto, 2),
            "taxa_efetiva_pct": round(taxa_ef, 1),
        },
        "alerts": alerts,
    }


# ══════════════════════════════════════
# AUTH HELPERS
# ══════════════════════════════════════

def hash_password(password: str) -> str:
    try:
        import bcrypt
        return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
    except ImportError:
        import hashlib
        return "sha256:" + hashlib.sha256(password.encode()).hexdigest()

def verify_password(password: str, hashed: str) -> bool:
    try:
        import bcrypt
        if hashed.startswith("sha256:"):
            import hashlib
            return "sha256:" + hashlib.sha256(password.encode()).hexdigest() == hashed
        return bcrypt.checkpw(password.encode(), hashed.encode())
    except ImportError:
        import hashlib
        return "sha256:" + hashlib.sha256(password.encode()).hexdigest() == hashed

def create_token(user_id: str, role: str, entity_ids: list) -> str:
    if pyjwt is None:
        # Fallback: simple base64 token (not secure, use jwt in production)
        import base64
        payload = json.dumps({"sub": user_id, "role": role, "entities": entity_ids, "exp": time.time() + JWT_EXPIRE_HOURS*3600})
        return base64.b64encode(payload.encode()).decode()
    payload = {
        "sub": user_id,
        "role": role,
        "entities": entity_ids,
        "exp": datetime.utcnow() + timedelta(hours=JWT_EXPIRE_HOURS)
    }
    return pyjwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGO)

def decode_token(token: str) -> dict:
    if pyjwt is None:
        import base64
        try:
            payload = json.loads(base64.b64decode(token.encode()).decode())
            if payload.get("exp", 0) < time.time():
                raise HTTPException(status_code=401, detail="Token expirado")
            return payload
        except:
            raise HTTPException(status_code=401, detail="Token inválido")
    try:
        return pyjwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGO])
    except Exception:
        raise HTTPException(status_code=401, detail="Token inválido ou expirado")

def get_current_user(credentials: HTTPAuthorizationCredentials = Security(security_scheme)):
    if not credentials:
        raise HTTPException(status_code=401, detail="Autenticação necessária")
    return decode_token(credentials.credentials)

def require_admin(user=Depends(get_current_user)):
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Acesso restrito a administradores")
    return user

def require_gestor(user=Depends(get_current_user)):
    if user.get("role") not in ("admin", "gestor"):
        raise HTTPException(status_code=403, detail="Acesso restrito")
    return user

def can_access_entity(entity_id: str, user: dict) -> bool:
    if user.get("role") == "admin":
        return True
    return entity_id in (user.get("entities") or [])

# ── PYDANTIC MODELS ──
class LoginRequest(BaseModel):
    email: str
    password: str

class UserCreate(BaseModel):
    name: str
    email: str
    password: str
    role: str = "gestor"
    entity_ids: List[str] = []

class UserUpdate(BaseModel):
    name: Optional[str] = None
    email: Optional[str] = None
    password: Optional[str] = None
    role: Optional[str] = None
    is_active: Optional[bool] = None
    entity_ids: Optional[List[str]] = None

# ══════════════════════════════════════
# AUTH ENDPOINTS
# ══════════════════════════════════════

@app.post("/api/auth/login")
def login(req: LoginRequest, request: Request, db=Depends(get_db), _rl=rate_limit(5, 60)):
    user = db.execute(
        "SELECT * FROM users WHERE email=? AND is_active=1", (req.email,)
    ).fetchone()
    if not user or not verify_password(req.password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Email ou password incorretos")
    # Get entity access
    entity_rows = db.execute(
        "SELECT entity_id FROM user_entities WHERE user_id=?", (user["id"],)
    ).fetchall()
    entity_ids = [r["entity_id"] for r in entity_rows]
    # Admin sees all
    if user["role"] == "admin":
        all_entities = db.execute("SELECT id FROM entities WHERE is_active=1").fetchall()
        entity_ids = [r["id"] for r in all_entities]
    token = create_token(user["id"], user["role"], entity_ids)
    db.execute("UPDATE users SET last_login=? WHERE id=?", (datetime.utcnow().isoformat(), user["id"]))
    db.commit()
    return {
        "token": token,
        "user": {"id": user["id"], "name": user["name"], "email": user["email"], "role": user["role"]},
        "entity_ids": entity_ids,
        "expires_in": JWT_EXPIRE_HOURS * 3600
    }

@app.get("/api/auth/me")
def get_me(db=Depends(get_db), user=Depends(get_current_user)):
    row = db.execute("SELECT id,name,email,role,last_login FROM users WHERE id=?", (user["sub"],)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Utilizador não encontrado")
    entity_rows = db.execute(
        "SELECT entity_id FROM user_entities WHERE user_id=?", (user["sub"],)
    ).fetchall()
    return dict(row) | {"entity_ids": [r["entity_id"] for r in entity_rows]}

@app.post("/api/auth/change-password")
def change_password(body: dict, db=Depends(get_db), user=Depends(get_current_user)):
    current = body.get("current_password", "")
    new_pw = body.get("new_password", "")
    if len(new_pw) < 6:
        raise HTTPException(status_code=400, detail="Password deve ter pelo menos 6 caracteres")
    row = db.execute("SELECT password_hash FROM users WHERE id=?", (user["sub"],)).fetchone()
    if not row or not verify_password(current, row["password_hash"]):
        raise HTTPException(status_code=401, detail="Password atual incorreta")
    db.execute("UPDATE users SET password_hash=? WHERE id=?", (hash_password(new_pw), user["sub"]))
    db.commit()
    return {"status": "ok"}

# ══════════════════════════════════════
# USER MANAGEMENT (admin only)
# ══════════════════════════════════════

@app.get("/api/users")
def list_users(db=Depends(get_db), user=Depends(require_admin)):
    rows = db.execute("SELECT id,name,email,role,is_active,created_at,last_login FROM users ORDER BY name").fetchall()
    result = []
    for r in rows:
        entity_rows = db.execute("SELECT entity_id FROM user_entities WHERE user_id=?", (r["id"],)).fetchall()
        result.append(dict(r) | {"entity_ids": [e["entity_id"] for e in entity_rows]})
    return result

@app.post("/api/users")
def create_user(req: UserCreate, db=Depends(get_db), user=Depends(require_admin)):
    uid = str(uuid.uuid4())
    try:
        db.execute(
            "INSERT INTO users (id,name,email,password_hash,role) VALUES (?,?,?,?,?)",
            (uid, req.name, req.email, hash_password(req.password), req.role)
        )
        for eid in req.entity_ids:
            db.execute("INSERT OR IGNORE INTO user_entities (user_id,entity_id) VALUES (?,?)", (uid, eid))
        db.commit()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Email já existe: {e}")
    return {"id": uid, "status": "created"}

@app.put("/api/users/{uid}")
def update_user(uid: str, req: UserUpdate, db=Depends(get_db), user=Depends(require_admin)):
    if req.name:
        db.execute("UPDATE users SET name=? WHERE id=?", (req.name, uid))
    if req.email:
        db.execute("UPDATE users SET email=? WHERE id=?", (req.email, uid))
    if req.password:
        db.execute("UPDATE users SET password_hash=? WHERE id=?", (hash_password(req.password), uid))
    if req.role:
        db.execute("UPDATE users SET role=? WHERE id=?", (req.role, uid))
    if req.is_active is not None:
        db.execute("UPDATE users SET is_active=? WHERE id=?", (1 if req.is_active else 0, uid))
    if req.entity_ids is not None:
        db.execute("DELETE FROM user_entities WHERE user_id=?", (uid,))
        for eid in req.entity_ids:
            db.execute("INSERT OR IGNORE INTO user_entities (user_id,entity_id) VALUES (?,?)", (uid, eid))
    db.commit()
    return {"status": "updated"}

@app.delete("/api/users/{uid}")
def delete_user(uid: str, db=Depends(get_db), user=Depends(require_admin)):
    if uid == "admin-001":
        raise HTTPException(status_code=400, detail="Não pode apagar o admin principal")
    db.execute("UPDATE users SET is_active=0 WHERE id=?", (uid,))
    db.commit()
    return {"status": "deleted"}

@app.get("/api/entities")
def list_entities(db=Depends(get_db), user=Depends(get_current_user)):
    if user.get("role") == "admin":
        rows = db.execute("SELECT * FROM entities WHERE is_active=1 ORDER BY name").fetchall()
    else:
        allowed = user.get("entities") or []
        if not allowed:
            return []
        placeholders = ",".join("?" * len(allowed))
        rows = db.execute(f"SELECT * FROM entities WHERE is_active=1 AND id IN ({placeholders}) ORDER BY name", allowed).fetchall()
    return [dict(r) for r in rows]

@app.post("/api/entities")
def create_entity(entity: EntityCreate, db=Depends(get_db), user=Depends(get_current_user)):
    eid = str(uuid.uuid4())
    try:
        db.execute("""INSERT INTO entities (id, name, nif, legal_form, cae_code, regime_irc, regime_iva, email, address)
                      VALUES (?,?,?,?,?,?,?,?,?)""",
                   (eid, entity.name, entity.nif, entity.legal_form, entity.cae_code,
                    entity.regime_irc, entity.regime_iva, entity.email, entity.address))
        # Create current fiscal year automatically
        fy_id = str(uuid.uuid4())
        current_year = datetime.now().year
        db.execute("INSERT INTO fiscal_years (id, entity_id, year) VALUES (?,?,?)",
                   (fy_id, eid, current_year))
        db.commit()
    except sqlite3.IntegrityError:
        raise HTTPException(status_code=409, detail=f"NIF {entity.nif} já existe.")
    return {"id": eid, "message": "Entidade criada com sucesso"}

@app.get("/api/entities/{entity_id}")
def get_entity(entity_id: str, db=Depends(get_db), user=Depends(get_current_user)):
    row = db.execute("SELECT * FROM entities WHERE id=?", (entity_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Entidade não encontrada")
    entity = dict(row)
    years = db.execute("SELECT * FROM fiscal_years WHERE entity_id=? ORDER BY year DESC",
                        (entity_id,)).fetchall()
    entity["fiscal_years"] = [dict(y) for y in years]
    return entity

@app.delete("/api/entities/{entity_id}")
def delete_entity(entity_id: str, db=Depends(get_db), user=Depends(require_admin)):
    db.execute("UPDATE entities SET is_active=0 WHERE id=?", (entity_id,))
    db.commit()
    return {"message": "Entidade desativada"}


# ──────────────────────────────────────────
# ROUTES — FISCAL YEARS
# ──────────────────────────────────────────
@app.get("/api/entities/{entity_id}/fiscal-years")
def list_fiscal_years(entity_id: str, db=Depends(get_db)):
    rows = db.execute("SELECT * FROM fiscal_years WHERE entity_id=? ORDER BY year DESC",
                       (entity_id,)).fetchall()
    return [dict(r) for r in rows]

@app.post("/api/entities/{entity_id}/fiscal-years")
def create_fiscal_year(entity_id: str, year: int = Form(...), db=Depends(get_db)):
    fy_id = str(uuid.uuid4())
    try:
        db.execute("INSERT INTO fiscal_years (id, entity_id, year) VALUES (?,?,?)",
                   (fy_id, entity_id, year))
        db.commit()
    except sqlite3.IntegrityError:
        raise HTTPException(409, f"Ano fiscal {year} já existe para esta entidade.")
    return {"id": fy_id}


# ──────────────────────────────────────────
# ROUTES — IMPORT
# ──────────────────────────────────────────
@app.post("/api/import/excel")
async def import_excel(
    request: Request,
    entity_id: str = Form(...),
    fiscal_year_id: str = Form(...),
    file: UploadFile = File(...),
):
    """Import a trial balance Excel file. Auto-detects columns and maps to SNC accounts."""
    if not _rl.check(request, 10, 60):
        raise HTTPException(429, "Demasiados imports — máximo 10 por minuto.")
    db = new_conn()
    start = time.time()
    try:
        # Validate file type
        if not file.filename.lower().endswith((".xlsx", ".xls", ".csv")):
            raise HTTPException(400, "Formato não suportado. Use .xlsx, .xls ou .csv")

        # Save uploaded file
        file_id = str(uuid.uuid4())
        save_path = UPLOADS_DIR / f"{file_id}_{file.filename}"
        content = await file.read()
        save_path.write_bytes(content)

        # Hash for dedup
        file_hash = hashlib.md5(content).hexdigest()

        # Parse
        try:
            parsed = parse_excel_balancete(save_path)
        except Exception as e:
            save_path.unlink(missing_ok=True)
            raise HTTPException(422, f"Erro ao processar ficheiro: {str(e)}")

        entries = parsed["entries"]
        meta = parsed["meta"]

        if not entries:
            raise HTTPException(422, "Nenhuma conta encontrada no ficheiro.")

        # Delete previous import for same entity+year (replace strategy)
        old_files = db.execute(
            "SELECT id FROM trial_balance_files WHERE entity_id=? AND fiscal_year_id=?",
            (entity_id, fiscal_year_id)
        ).fetchall()
        for old in old_files:
            db.execute("DELETE FROM trial_balance_entries WHERE file_id=?", (old["id"],))
            db.execute("DELETE FROM trial_balance_files WHERE id=?", (old["id"],))

        # Insert file record
        db.execute("""INSERT INTO trial_balance_files
                      (id, entity_id, fiscal_year_id, file_name, file_hash, source, total_accounts)
                      VALUES (?,?,?,?,?,?,?)""",
                   (file_id, entity_id, fiscal_year_id, file.filename, file_hash, "excel", len(entries)))

        # Insert entries in bulk
        db.executemany("""INSERT INTO trial_balance_entries
                          (file_id, entity_id, fiscal_year_id, classe, nivel, integradora,
                           conta, descricao, deb_periodo, cred_periodo, deb_acum, cred_acum,
                           devedor, credor, saldo_tot)
                          VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                       [(file_id, entity_id, fiscal_year_id,
                         e["classe"], e["nivel"], int(e["integradora"]),
                         e["conta"], e["descricao"],
                         e["deb_periodo"], e["cred_periodo"],
                         e["deb_acum"], e["cred_acum"],
                         e["devedor"], e["credor"], e["saldo_tot"])
                        for e in entries])
        db.commit()

        # Calculate financials immediately
        financials = calculate_financials(entries)
        elapsed = round((time.time() - start) * 1000)

        return {
            "status": "success",
            "file_id": file_id,
            "file_name": file.filename,
            "total_accounts": len(entries),
            "meta": meta,
            "financials": financials,
            "processing_ms": elapsed,
            "col_map_detected": parsed["col_map"],
            "message": f"✓ {len(entries)} contas importadas com sucesso em {elapsed}ms"
        }
    except HTTPException:
        db.close()
        raise
    except Exception:
        db.rollback()
        db.close()
        raise


# ══════════════════════════════════════════════════════════
# SAF-T PT-04 PARSER
# ══════════════════════════════════════════════════════════
import xml.etree.ElementTree as ET

# Namespaces SAF-T PT-04 (versão 1.04_01)
_SAFT_NS = {
    'saft': 'urn:OECD:StandardAuditFile-Tax:PT_1.04_01',
}

def _find(el, path, ns=_SAFT_NS):
    """Wrapper seguro para find — tenta com e sem namespace."""
    r = el.find(path, ns)
    if r is None:
        # Tenta sem namespace (alguns softwares omitem)
        path_clean = path.replace('saft:', '')
        r = el.find(path_clean)
    return r

def _text(el, path, default='', ns=_SAFT_NS):
    """Lê texto de um sub-elemento de forma segura."""
    node = _find(el, path, ns)
    return (node.text or '').strip() if node is not None else default

def _float(el, path, default=0.0, ns=_SAFT_NS):
    try:
        return float(_text(el, path, '0', ns))
    except (ValueError, TypeError):
        return default

def parse_saft_xml(xml_bytes: bytes) -> dict:
    """
    Parse SAF-T PT-04 XML.
    Retorna dict com: header, customers, products, tax_table, invoices.
    """
    # Tentar detectar e remover BOM UTF-8/16
    if xml_bytes.startswith(b'\xef\xbb\xbf'):
        xml_bytes = xml_bytes[3:]

    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as e:
        raise ValueError(f'XML inválido: {e}')

    # Detectar namespace real do ficheiro
    ns = {}
    tag = root.tag
    if tag.startswith('{'):
        ns_uri = tag[1:tag.index('}')]
        ns = {'saft': ns_uri}
    else:
        ns = {}  # sem namespace

    def f(el, path): return _find(el, path, ns)
    def t(el, path, d=''): return _text(el, path, d, ns)
    def n(el, path, d=0.0): return _float(el, path, d, ns)

    # ── HEADER ─────────────────────────────────────────────────
    hdr = f(root, 'saft:Header') or f(root, 'Header')
    if hdr is None:
        raise ValueError('Header SAF-T não encontrado — ficheiro inválido ou não é PT-04')

    header = {
        'audit_file_version':    t(hdr, 'saft:AuditFileVersion') or t(hdr, 'AuditFileVersion'),
        'company_id':            t(hdr, 'saft:CompanyID') or t(hdr, 'CompanyID'),
        'tax_registration_number': t(hdr, 'saft:TaxRegistrationNumber') or t(hdr, 'TaxRegistrationNumber'),
        'company_name':          t(hdr, 'saft:CompanyName') or t(hdr, 'CompanyName'),
        'fiscal_year':           t(hdr, 'saft:FiscalYear') or t(hdr, 'FiscalYear'),
        'start_date':            t(hdr, 'saft:StartDate') or t(hdr, 'StartDate'),
        'end_date':              t(hdr, 'saft:EndDate') or t(hdr, 'EndDate'),
        'currency_code':         t(hdr, 'saft:CurrencyCode') or t(hdr, 'CurrencyCode') or 'EUR',
        'software_company_name': t(hdr, 'saft:SoftwareCompanyName') or t(hdr, 'SoftwareCompanyName'),
        'product_id':            t(hdr, 'saft:ProductID') or t(hdr, 'ProductID'),
    }

    # ── MASTER FILES ───────────────────────────────────────────
    mf = f(root, 'saft:MasterFiles') or f(root, 'MasterFiles') or root

    # Clientes
    customers = []
    cust_prefix = 'saft:Customer' if ns else 'Customer'
    for c in (mf.findall(cust_prefix, ns) if ns else mf.findall('Customer')):
        addr = f(c, 'saft:BillingAddress') or f(c, 'BillingAddress') or c
        customers.append({
            'customer_id':   t(c, 'saft:CustomerID') or t(c, 'CustomerID'),
            'account_id':    t(c, 'saft:AccountID') or t(c, 'AccountID'),
            'company_name':  t(c, 'saft:CompanyName') or t(c, 'CompanyName'),
            'contact':       t(c, 'saft:Contact') or t(c, 'Contact'),
            'nif':           t(c, 'saft:CustomerTaxID') or t(c, 'CustomerTaxID'),
            'country':       t(addr, 'saft:Country') or t(addr, 'Country') or 'PT',
            'postal_code':   t(addr, 'saft:PostalCode') or t(addr, 'PostalCode'),
            'city':          t(addr, 'saft:City') or t(addr, 'City'),
        })

    # Produtos
    products = []
    prod_prefix = 'saft:Product' if ns else 'Product'
    for p in (mf.findall(prod_prefix, ns) if ns else mf.findall('Product')):
        products.append({
            'product_code':  t(p, 'saft:ProductCode') or t(p, 'ProductCode'),
            'product_group': t(p, 'saft:ProductGroup') or t(p, 'ProductGroup'),
            'product_desc':  t(p, 'saft:ProductDescription') or t(p, 'ProductDescription'),
            'product_type':  t(p, 'saft:ProductType') or t(p, 'ProductType'),
            'unit_of_measure': t(p, 'saft:UnitOfMeasure') or t(p, 'UnitOfMeasure'),
        })

    # Tabela de impostos
    tax_table = []
    tt = f(mf, 'saft:TaxTable') or f(mf, 'TaxTable')
    if tt is not None:
        te_prefix = 'saft:TaxTableEntry' if ns else 'TaxTableEntry'
        for e in (tt.findall(te_prefix, ns) if ns else tt.findall('TaxTableEntry')):
            pct_node = f(e, 'saft:TaxPercentage') or f(e, 'TaxPercentage')
            tax_table.append({
                'tax_type':        t(e, 'saft:TaxType') or t(e, 'TaxType'),
                'tax_country':     t(e, 'saft:TaxCountryRegion') or t(e, 'TaxCountryRegion') or 'PT',
                'tax_code':        t(e, 'saft:TaxCode') or t(e, 'TaxCode'),
                'tax_description': t(e, 'saft:Description') or t(e, 'Description'),
                'tax_expiration':  t(e, 'saft:TaxExpirationDate') or t(e, 'TaxExpirationDate'),
                'tax_percentage':  float(pct_node.text or 0) if pct_node is not None else 0.0,
            })

    # ── SOURCE DOCUMENTS — SALES INVOICES ──────────────────────
    invoices  = []
    inv_lines = []
    sd = f(root, 'saft:SourceDocuments') or f(root, 'SourceDocuments')
    si_container = None
    if sd is not None:
        si_container = f(sd, 'saft:SalesInvoices') or f(sd, 'SalesInvoices')

    if si_container is not None:
        inv_prefix = 'saft:Invoice' if ns else 'Invoice'
        for inv in (si_container.findall(inv_prefix, ns) if ns else si_container.findall('Invoice')):
            inv_no    = t(inv, 'saft:InvoiceNo') or t(inv, 'InvoiceNo')
            inv_type  = t(inv, 'saft:InvoiceType') or t(inv, 'InvoiceType')
            inv_date  = t(inv, 'saft:InvoiceDate') or t(inv, 'InvoiceDate')
            # Hash — primeiros 4 chars
            hash_el   = f(inv, 'saft:Hash') or f(inv, 'Hash')
            hash_chars = (hash_el.text or '')[:4] if hash_el is not None else ''

            # Status
            status_el = f(inv, 'saft:DocumentStatus') or f(inv, 'DocumentStatus')
            inv_status = 'N'
            if status_el is not None:
                inv_status = t(status_el, 'saft:InvoiceStatus') or t(status_el, 'InvoiceStatus') or 'N'

            # Série (prefixo antes do /)
            serie = inv_no.split('/')[0] if '/' in inv_no else ''

            # Cliente
            cust_id = t(inv, 'saft:CustomerID') or t(inv, 'CustomerID')

            # Totals
            doc_tot = f(inv, 'saft:DocumentTotals') or f(inv, 'DocumentTotals')
            gross   = n(doc_tot, 'saft:GrossTotal') or n(doc_tot, 'GrossTotal') if doc_tot else 0.0
            net_tot = n(doc_tot, 'saft:NetTotal') or n(doc_tot, 'NetTotal') if doc_tot else 0.0
            tax_pay = n(doc_tot, 'saft:TaxPayable') or n(doc_tot, 'TaxPayable') if doc_tot else 0.0
            settle  = n(doc_tot, 'saft:Settlement') or n(doc_tot, 'Settlement') if doc_tot else 0.0

            # Notas de crédito têm grossTotal negativo — normalizar
            if inv_type in ('NC', 'ND'):
                gross   = -abs(gross)
                net_tot = -abs(net_tot)
                tax_pay = -abs(tax_pay)

            inv_idx = len(invoices)
            invoices.append({
                'invoice_no':     inv_no,
                'invoice_type':   inv_type,
                'invoice_date':   inv_date,
                'invoice_status': inv_status,
                'customer_id':    cust_id,
                'gross_total':    gross,
                'net_total':      net_tot,
                'tax_payable':    tax_pay,
                'settlement':     settle,
                'serie':          serie,
                'hash_chars':     hash_chars,
                '_idx':           inv_idx,
            })

            # Lines
            line_prefix = 'saft:Line' if ns else 'Line'
            for line in (inv.findall(line_prefix, ns) if ns else inv.findall('Line')):
                tax_el   = f(line, 'saft:Tax') or f(line, 'Tax')
                tax_pct  = n(tax_el, 'saft:TaxPercentage') or n(tax_el, 'TaxPercentage') if tax_el else 0.0
                tax_code = (t(tax_el, 'saft:TaxCode') or t(tax_el, 'TaxCode') or '') if tax_el else ''
                # Pode ser crédito ou débito
                credit = n(line, 'saft:CreditAmount') or n(line, 'CreditAmount')
                debit  = n(line, 'saft:DebitAmount')  or n(line, 'DebitAmount')
                tax_base = n(line, 'saft:TaxBase') or n(line, 'TaxBase') or (credit or debit)

                inv_lines.append({
                    '_inv_idx':      inv_idx,
                    'line_no':       int(t(line, 'saft:LineNumber') or t(line, 'LineNumber') or 0),
                    'product_code':  t(line, 'saft:ProductCode') or t(line, 'ProductCode'),
                    'description':   t(line, 'saft:Description') or t(line, 'Description'),
                    'quantity':      n(line, 'saft:Quantity') or n(line, 'Quantity') or 1.0,
                    'unit_price':    n(line, 'saft:UnitPrice') or n(line, 'UnitPrice'),
                    'credit_amount': credit,
                    'debit_amount':  debit,
                    'tax_base':      tax_base,
                    'tax_percentage': tax_pct,
                    'tax_code':      tax_code,
                    'tax_amount':    tax_base * tax_pct / 100 if tax_pct else 0.0,
                })

    # Totals
    total_debit  = sum(i['gross_total'] for i in invoices if i['gross_total'] > 0 and i['invoice_status'] == 'N')
    total_credit = sum(abs(i['gross_total']) for i in invoices if i['gross_total'] < 0 and i['invoice_status'] == 'N')

    return {
        'header':     header,
        'customers':  customers,
        'products':   products,
        'tax_table':  tax_table,
        'invoices':   invoices,
        'inv_lines':  inv_lines,
        'total_invoices': len([i for i in invoices if i['invoice_status'] == 'N']),
        'total_debit':    round(total_debit, 2),
        'total_credit':   round(total_credit, 2),
    }


def _compute_saft_analytics(invoices: list, inv_lines: list, customers: list) -> dict:
    """
    Calcula todos os KPIs, análises temporais, por cliente, produto, geo e anomalias.
    """
    from collections import defaultdict
    import re

    # Mapa customer_id → info
    cust_map = {c['customer_id']: c for c in customers}

    # Faturas válidas (não anuladas)
    valid_invs = [i for i in invoices if i['invoice_status'] == 'N']

    # ── KPIs GLOBAIS ──────────────────────────────────────────
    total_vn      = sum(i['gross_total'] for i in valid_invs if i['invoice_type'] not in ('NC','ND'))
    total_nc      = sum(abs(i['gross_total']) for i in valid_invs if i['invoice_type'] == 'NC')
    total_liq     = total_vn - total_nc
    n_faturas     = len([i for i in valid_invs if i['invoice_type'] not in ('NC','ND')])
    n_nc          = len([i for i in valid_invs if i['invoice_type'] == 'NC'])
    ticket_medio  = round(total_liq / n_faturas, 2) if n_faturas else 0
    total_iva     = sum(i['tax_payable'] for i in valid_invs)
    clientes_uniq = len(set(i['customer_id'] for i in valid_invs))
    anuladas      = len([i for i in invoices if i['invoice_status'] == 'A'])

    kpis = {
        'total_vn': round(total_vn, 2),
        'total_nc': round(total_nc, 2),
        'total_liq': round(total_liq, 2),
        'n_faturas': n_faturas,
        'n_nc': n_nc,
        'n_anuladas': anuladas,
        'ticket_medio': ticket_medio,
        'total_iva': round(total_iva, 2),
        'clientes_uniq': clientes_uniq,
    }

    # ── TEMPORAL — por mês ────────────────────────────────────
    monthly = defaultdict(lambda: {'mes': '', 'n_faturas': 0, 'gross': 0.0, 'iva': 0.0, 'nc': 0.0})
    for inv in valid_invs:
        d = inv['invoice_date']
        if not d or len(d) < 7:
            continue
        mes = d[:7]  # YYYY-MM
        if inv['invoice_type'] not in ('NC','ND'):
            monthly[mes]['n_faturas'] += 1
            monthly[mes]['gross']     += inv['gross_total']
            monthly[mes]['iva']       += inv['tax_payable']
        else:
            monthly[mes]['nc']        += abs(inv['gross_total'])
        monthly[mes]['mes'] = mes

    temporal = sorted(
        [{'mes': k, **v} for k, v in monthly.items()],
        key=lambda x: x['mes']
    )
    for t in temporal:
        t['gross'] = round(t['gross'], 2)
        t['iva']   = round(t['iva'], 2)
        t['nc']    = round(t['nc'], 2)
        t['liquido'] = round(t['gross'] - t['nc'], 2)

    # ── TOP CLIENTES ──────────────────────────────────────────
    cust_sales = defaultdict(lambda: {'n_faturas': 0, 'gross': 0.0, 'nc': 0.0})
    for inv in valid_invs:
        cid = inv['customer_id']
        if inv['invoice_type'] not in ('NC','ND'):
            cust_sales[cid]['n_faturas'] += 1
            cust_sales[cid]['gross']     += inv['gross_total']
        else:
            cust_sales[cid]['nc']        += abs(inv['gross_total'])

    top_clientes = []
    for cid, data in cust_sales.items():
        liq = data['gross'] - data['nc']
        info = cust_map.get(cid, {})
        top_clientes.append({
            'customer_id':   cid,
            'company_name':  info.get('company_name') or cid,
            'nif':           info.get('nif', ''),
            'country':       info.get('country', 'PT'),
            'n_faturas':     data['n_faturas'],
            'gross':         round(data['gross'], 2),
            'nc':            round(data['nc'], 2),
            'liq':           round(liq, 2),
            'pct_vn':        round(liq / total_liq * 100, 1) if total_liq else 0,
        })
    top_clientes.sort(key=lambda x: x['liq'], reverse=True)

    # Concentração HHI (Herfindahl-Hirschman Index)
    hhi = sum((c['pct_vn'])**2 for c in top_clientes) if top_clientes else 0
    top3_pct = sum(c['pct_vn'] for c in top_clientes[:3])

    # ── PRODUTOS ──────────────────────────────────────────────
    prod_sales = defaultdict(lambda: {'n_linhas': 0, 'qty': 0.0, 'base': 0.0, 'iva': 0.0})
    for line in inv_lines:
        inv = invoices[line['_inv_idx']] if line['_inv_idx'] < len(invoices) else {}
        if inv.get('invoice_status') == 'A':
            continue
        code = line['product_code'] or 'SEM CÓDIGO'
        mult = -1 if inv.get('invoice_type') in ('NC','ND') else 1
        prod_sales[code]['n_linhas'] += 1
        prod_sales[code]['qty']      += line['quantity'] * mult
        prod_sales[code]['base']     += (line['credit_amount'] or line['debit_amount']) * mult
        prod_sales[code]['iva']      += line['tax_amount'] * mult

    top_produtos = []
    for code, data in prod_sales.items():
        top_produtos.append({
            'product_code': code,
            'n_linhas':     data['n_linhas'],
            'qty':          round(data['qty'], 3),
            'base':         round(data['base'], 2),
            'iva':          round(data['iva'], 2),
            'pct_vn':       round(data['base'] / total_liq * 100, 1) if total_liq else 0,
        })
    top_produtos.sort(key=lambda x: x['base'], reverse=True)

    # ── GEOGRAFIA ─────────────────────────────────────────────
    geo = defaultdict(lambda: {'n_faturas': 0, 'gross': 0.0})
    for inv in valid_invs:
        if inv['invoice_type'] in ('NC','ND'):
            continue
        cid  = inv['customer_id']
        info = cust_map.get(cid, {})
        country = info.get('country') or 'PT'
        # Distinguir Continente / Açores / Madeira por código postal se PT
        postal = info.get('postal_code') or ''
        if country == 'PT' and postal:
            prefix = postal[:4]
            try:
                cp = int(prefix)
                if 1000 <= cp <= 9999:  # Açores 9xxx, Madeira 9xxx
                    if 9000 <= cp <= 9999:
                        country = 'PT-RA'  # Regiões Autónomas
            except:
                pass
        geo[country]['n_faturas'] += 1
        geo[country]['gross']     += inv['gross_total']

    geography = sorted(
        [{'country': k, 'n_faturas': v['n_faturas'], 'gross': round(v['gross'], 2),
          'pct': round(v['gross'] / total_vn * 100, 1) if total_vn else 0}
         for k, v in geo.items()],
        key=lambda x: x['gross'], reverse=True
    )

    # ── ANOMALIAS ─────────────────────────────────────────────
    anomalias = []

    # 1. Faturas sem hash (obrigatório para FT, FS, FR)
    sem_hash = [i for i in valid_invs if not i['hash_chars'] and i['invoice_type'] in ('FT','FS','FR')]
    if sem_hash:
        anomalias.append({
            'tipo': 'hash_ausente',
            'severity': 'alta',
            'titulo': f'{len(sem_hash)} fatura(s) sem hash de assinatura',
            'descricao': 'Faturas FT/FS/FR devem ter hash AT (assinatura digital). Pode indicar emissão manual fora do software certificado.',
            'documentos': [i['invoice_no'] for i in sem_hash[:10]],
        })

    # 2. Saltos de numeração
    series_nums = defaultdict(list)
    for inv in valid_invs:
        serie = inv['serie']
        no_part = inv['invoice_no'].split('/')[-1] if '/' in inv['invoice_no'] else ''
        try:
            series_nums[serie].append(int(no_part))
        except:
            pass

    saltos = []
    for serie, nums in series_nums.items():
        nums.sort()
        for i in range(1, len(nums)):
            if nums[i] - nums[i-1] > 1:
                saltos.append(f'{serie}/{nums[i-1]+1}–{nums[i]-1}')
    if saltos:
        anomalias.append({
            'tipo': 'saltos_numeracao',
            'severity': 'media',
            'titulo': f'{len(saltos)} salto(s) de numeração detetado(s)',
            'descricao': 'Lacunas na sequência numérica podem indicar documentos omitidos ou numeração não contínua (não conforme com CIVA Art.36.º).',
            'documentos': saltos[:10],
        })

    # 3. NCs sem FT correspondente
    nc_nos  = set(i['invoice_no'] for i in valid_invs if i['invoice_type'] == 'NC')
    ft_nos  = set(i['invoice_no'] for i in valid_invs if i['invoice_type'] in ('FT','FS','FR'))
    # (simplificado — verifica se há muitas NCs vs FTs)
    nc_ratio = len(nc_nos) / max(len(ft_nos), 1)
    if nc_ratio > 0.15:
        anomalias.append({
            'tipo': 'ratio_nc_elevado',
            'severity': 'media',
            'titulo': f'Rácio NC/FT elevado: {round(nc_ratio*100,1)}%',
            'descricao': 'Alto volume de notas de crédito relativamente às faturas emitidas. Pode indicar devoluções anómalas ou ajustamentos frequentes.',
            'documentos': [],
        })

    # 4. Clientes sem NIF em faturas > €1.000
    sem_nif = []
    for inv in valid_invs:
        if inv['gross_total'] > 1000 and inv['invoice_type'] not in ('NC','ND'):
            cid  = inv['customer_id']
            info = cust_map.get(cid, {})
            nif  = info.get('nif', '')
            if not nif or nif in ('999999990', '0'):
                sem_nif.append(inv['invoice_no'])
    if sem_nif:
        anomalias.append({
            'tipo': 'sem_nif_acima_1000',
            'severity': 'baixa',
            'titulo': f'{len(sem_nif)} fatura(s) >€1.000 para consumidor final',
            'descricao': 'Faturas acima de €1.000 emitidas a consumidores finais (sem NIF). Verificar conformidade com obrigação de identificação (Art.36.º n.5 CIVA).',
            'documentos': sem_nif[:10],
        })

    # 5. Faturas com IVA 0% em volume significativo
    zero_iva = [i for i in valid_invs if i['tax_payable'] == 0 and i['gross_total'] > 100 and i['invoice_type'] not in ('NC','ND')]
    if len(zero_iva) > 0:
        anomalias.append({
            'tipo': 'isencao_iva',
            'severity': 'info',
            'titulo': f'{len(zero_iva)} fatura(s) com IVA 0% (isenção)',
            'descricao': 'Documentos emitidos com isenção de IVA. Verificar enquadramento legal (Art.9.º / Art.13.º CIVA ou regime especial).',
            'documentos': [i['invoice_no'] for i in zero_iva[:5]],
        })

    # ── RECAP / OSS ───────────────────────────────────────────
    # Vendas intracomunitárias (clientes UE fora de PT)
    ue_countries = {'AT','BE','BG','CY','CZ','DE','DK','EE','ES','FI','FR',
                    'GR','HR','HU','IE','IT','LT','LU','LV','MT','NL','PL',
                    'RO','SE','SI','SK'}
    recap = defaultdict(lambda: {'gross': 0.0, 'iva': 0.0, 'n': 0})
    oss   = defaultdict(lambda: {'gross': 0.0, 'n': 0})
    for inv in valid_invs:
        if inv['invoice_type'] in ('NC','ND'):
            continue
        cid     = inv['customer_id']
        info    = cust_map.get(cid, {})
        country = info.get('country', 'PT')
        if country in ue_countries:
            recap[country]['gross'] += inv['gross_total']
            recap[country]['iva']   += inv['tax_payable']
            recap[country]['n']     += 1
        elif country not in ('PT', 'PT-RA') and country:
            oss[country]['gross'] += inv['gross_total']
            oss[country]['n']     += 1

    recapitulativa = sorted(
        [{'country': k, 'gross': round(v['gross'],2), 'iva': round(v['iva'],2), 'n': v['n']}
         for k, v in recap.items()],
        key=lambda x: x['gross'], reverse=True
    )
    oss_summary = sorted(
        [{'country': k, 'gross': round(v['gross'],2), 'n': v['n']}
         for k, v in oss.items()],
        key=lambda x: x['gross'], reverse=True
    )

    return {
        'kpis':           kpis,
        'temporal':       temporal,
        'top_clientes':   top_clientes[:50],
        'concentracao':   {'hhi': round(hhi, 1), 'top3_pct': round(top3_pct, 1)},
        'top_produtos':   top_produtos[:50],
        'geography':      geography,
        'anomalias':      anomalias,
        'recapitulativa': recapitulativa,
        'oss':            oss_summary,
    }


# ── Endpoint: upload SAF-T ─────────────────────────────────────────────────
@app.post("/api/import/saft")
async def import_saft(
    request: Request,
    entity_id: str = Form(...),
    file: UploadFile = File(...),
):
    """Import SAF-T PT-04 XML file."""
    if not _rl.check(request, 5, 60):
        raise HTTPException(429, "Demasiados imports — máximo 5 por minuto.")

    db = new_conn()
    try:
        if not file.filename.lower().endswith('.xml'):
            raise HTTPException(400, "Formato não suportado. Use ficheiro SAF-T (.xml)")

        if file.size and file.size > 100 * 1024 * 1024:
            raise HTTPException(400, "Ficheiro demasiado grande (máximo 100MB).")

        content_bytes = await file.read()

        # Hash para dedup
        file_hash = hashlib.md5(content_bytes).hexdigest()

        # Parse
        try:
            parsed = parse_saft_xml(content_bytes)
        except ValueError as e:
            raise HTTPException(422, str(e))
        except Exception as e:
            raise HTTPException(422, f"Erro ao processar SAF-T: {str(e)}")

        hdr = parsed['header']
        saft_id = str(uuid.uuid4())

        # Substituir ficheiro anterior do mesmo ano
        fiscal_year_str = hdr.get('fiscal_year', '')
        try:
            fiscal_year = int(fiscal_year_str)
        except:
            fiscal_year = None

        if fiscal_year:
            old = db.execute(
                "SELECT id FROM saft_files WHERE entity_id=? AND fiscal_year=?",
                (entity_id, fiscal_year)
            ).fetchall()
            for o in old:
                oid = o['id']
                # Apagar em cascata
                db.execute("DELETE FROM saft_invoice_lines WHERE saft_file_id=?", (oid,))
                db.execute("DELETE FROM saft_invoices WHERE saft_file_id=?", (oid,))
                db.execute("DELETE FROM saft_customers WHERE saft_file_id=?", (oid,))
                db.execute("DELETE FROM saft_products WHERE saft_file_id=?", (oid,))
                db.execute("DELETE FROM saft_tax_table WHERE saft_file_id=?", (oid,))
                db.execute("DELETE FROM saft_files WHERE id=?", (oid,))

        # Inserir ficheiro
        db.execute("""INSERT INTO saft_files
            (id, entity_id, file_name, file_hash, fiscal_year, period_start, period_end,
             company_name, company_nif, software, version, total_invoices, total_debit, total_credit)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (saft_id, entity_id, file.filename, file_hash, fiscal_year,
             hdr.get('start_date'), hdr.get('end_date'),
             hdr.get('company_name'), hdr.get('tax_registration_number'),
             hdr.get('software_company_name'), hdr.get('audit_file_version'),
             parsed['total_invoices'], parsed['total_debit'], parsed['total_credit']))

        # Clientes
        if parsed['customers']:
            db.executemany("""INSERT INTO saft_customers
                (saft_file_id, entity_id, customer_id, account_id, company_name, contact, nif, country, postal_code, city)
                VALUES (?,?,?,?,?,?,?,?,?,?)""",
                [(saft_id, entity_id, c['customer_id'], c['account_id'], c['company_name'],
                  c['contact'], c['nif'], c['country'], c['postal_code'], c['city'])
                 for c in parsed['customers']])

        # Produtos
        if parsed['products']:
            db.executemany("""INSERT INTO saft_products
                (saft_file_id, entity_id, product_code, product_group, product_desc, product_type, unit_of_measure)
                VALUES (?,?,?,?,?,?,?)""",
                [(saft_id, entity_id, p['product_code'], p['product_group'], p['product_desc'],
                  p['product_type'], p['unit_of_measure'])
                 for p in parsed['products']])

        # Tax table
        if parsed['tax_table']:
            db.executemany("""INSERT INTO saft_tax_table
                (saft_file_id, entity_id, tax_type, tax_country, tax_code, tax_description, tax_expiration, tax_percentage)
                VALUES (?,?,?,?,?,?,?,?)""",
                [(saft_id, entity_id, t['tax_type'], t['tax_country'], t['tax_code'],
                  t['tax_description'], t['tax_expiration'], t['tax_percentage'])
                 for t in parsed['tax_table']])

        # Invoices + lines em batches
        inv_ids = {}
        if parsed['invoices']:
            for inv in parsed['invoices']:
                cur = db.execute("""INSERT INTO saft_invoices
                    (saft_file_id, entity_id, invoice_no, invoice_type, invoice_date, invoice_status,
                     customer_id, gross_total, net_total, tax_payable, settlement, serie, hash_chars)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (saft_id, entity_id, inv['invoice_no'], inv['invoice_type'], inv['invoice_date'],
                     inv['invoice_status'], inv['customer_id'], inv['gross_total'], inv['net_total'],
                     inv['tax_payable'], inv['settlement'], inv['serie'], inv['hash_chars']))
                inv_ids[inv['_idx']] = cur.lastrowid

        if parsed['inv_lines']:
            db.executemany("""INSERT INTO saft_invoice_lines
                (invoice_id, saft_file_id, entity_id, line_no, product_code, description,
                 quantity, unit_price, credit_amount, debit_amount, tax_base, tax_percentage, tax_code, tax_amount)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                [(inv_ids.get(l['_inv_idx']), saft_id, entity_id, l['line_no'], l['product_code'],
                  l['description'], l['quantity'], l['unit_price'], l['credit_amount'], l['debit_amount'],
                  l['tax_base'], l['tax_percentage'], l['tax_code'], l['tax_amount'])
                 for l in parsed['inv_lines'] if l['_inv_idx'] in inv_ids])

        db.commit()

        # Calcular analytics
        analytics = _compute_saft_analytics(parsed['invoices'], parsed['inv_lines'], parsed['customers'])

        return {
            'saft_id':        saft_id,
            'message':        f"SAF-T importado com sucesso: {parsed['total_invoices']} documentos",
            'header':         hdr,
            'total_invoices': parsed['total_invoices'],
            'total_debit':    parsed['total_debit'],
            'total_credit':   parsed['total_credit'],
            'customers':      len(parsed['customers']),
            'products':       len(parsed['products']),
            'analytics':      analytics,
        }

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(500, f"Erro interno: {str(e)}")
    finally:
        db.close()


# ── Endpoint: GET analytics SAF-T ─────────────────────────────────────────
@app.get("/api/saft/{entity_id}/analytics")
async def get_saft_analytics(entity_id: str, year: Optional[int] = None):
    """Retorna analytics SAF-T para uma entidade/ano."""
    db = new_conn()
    try:
        q = "SELECT * FROM saft_files WHERE entity_id=?"
        params = [entity_id]
        if year:
            q += " AND fiscal_year=?"
            params.append(year)
        q += " ORDER BY imported_at DESC LIMIT 1"

        saft_file = db.execute(q, params).fetchone()
        if not saft_file:
            return {'error': 'no_data', 'message': 'Nenhum SAF-T importado para esta entidade'}

        saft_id = saft_file['id']

        # Carregar dados para analytics
        invoices_raw = db.execute(
            "SELECT * FROM saft_invoices WHERE saft_file_id=?", (saft_id,)
        ).fetchall()
        lines_raw = db.execute(
            "SELECT * FROM saft_invoice_lines WHERE saft_file_id=?", (saft_id,)
        ).fetchall()
        customers_raw = db.execute(
            "SELECT * FROM saft_customers WHERE saft_file_id=?", (saft_id,)
        ).fetchall()

        # Converter para dicts
        invoices = [dict(r) for r in invoices_raw]
        # Adicionar _idx para analytics
        for i, inv in enumerate(invoices):
            inv['_idx'] = i

        lines = []
        inv_id_to_idx = {inv['id']: i for i, inv in enumerate(invoices)}
        for l in lines_raw:
            ld = dict(l)
            ld['_inv_idx'] = inv_id_to_idx.get(ld['invoice_id'], -1)
            lines.append(ld)

        customers = [dict(r) for r in customers_raw]

        analytics = _compute_saft_analytics(invoices, lines, customers)

        return {
            'saft_id':     saft_id,
            'file_name':   saft_file['file_name'],
            'fiscal_year': saft_file['fiscal_year'],
            'period_start': saft_file['period_start'],
            'period_end':  saft_file['period_end'],
            'company_name': saft_file['company_name'],
            'total_invoices': saft_file['total_invoices'],
            'total_debit':    saft_file['total_debit'],
            'imported_at':    saft_file['imported_at'],
            'analytics': analytics,
        }
    finally:
        db.close()


# ── Endpoint: GET lista SAF-T ──────────────────────────────────────────────
@app.get("/api/saft/{entity_id}/files")
async def list_saft_files(entity_id: str):
    db = new_conn()
    try:
        rows = db.execute(
            "SELECT id, file_name, fiscal_year, period_start, period_end, total_invoices, total_debit, total_credit, imported_at FROM saft_files WHERE entity_id=? ORDER BY fiscal_year DESC",
            (entity_id,)
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        db.close()


# ── Endpoint: DELETE SAF-T ─────────────────────────────────────────────────
@app.delete("/api/saft/{entity_id}/{saft_id}")
async def delete_saft(entity_id: str, saft_id: str):
    db = new_conn()
    try:
        db.execute("DELETE FROM saft_invoice_lines WHERE saft_file_id=? AND entity_id=?", (saft_id, entity_id))
        db.execute("DELETE FROM saft_invoices WHERE saft_file_id=? AND entity_id=?", (saft_id, entity_id))
        db.execute("DELETE FROM saft_customers WHERE saft_file_id=? AND entity_id=?", (saft_id, entity_id))
        db.execute("DELETE FROM saft_products WHERE saft_file_id=? AND entity_id=?", (saft_id, entity_id))
        db.execute("DELETE FROM saft_tax_table WHERE saft_file_id=? AND entity_id=?", (saft_id, entity_id))
        db.execute("DELETE FROM saft_files WHERE id=? AND entity_id=?", (saft_id, entity_id))
        db.commit()
        return {'ok': True}
    except Exception as e:
        db.rollback()
        raise HTTPException(500, str(e))
    finally:
        db.close()


# ──────────────────────────────────────────
# ROUTES — BALANCETE / ANALYSIS
# ──────────────────────────────────────────
@app.get("/api/entities/{entity_id}/balancete")
def get_balancete(
    entity_id: str,
    fiscal_year_id: str,
    nivel_max: int = 99,
    search: Optional[str] = None,
    db=Depends(get_db),
    user=Depends(get_current_user)
):
    if not can_access_entity(entity_id, user): raise HTTPException(403, "Acesso negado")
    query = """SELECT * FROM trial_balance_entries
               WHERE entity_id=? AND fiscal_year_id=?"""
    params = [entity_id, fiscal_year_id]

    if nivel_max < 99:
        query += " AND nivel <= ?"
        params.append(nivel_max)
    if search:
        query += " AND (conta LIKE ? OR descricao LIKE ?)"
        params.extend([f"%{search}%", f"%{search}%"])

    query += " ORDER BY conta"
    rows = db.execute(query, params).fetchall()
    return [dict(r) for r in rows]

@app.get("/api/entities/{entity_id}/financials")
def get_financials(entity_id: str, fiscal_year_id: str, db=Depends(get_db), user=Depends(get_current_user)):
    if not can_access_entity(entity_id, user): raise HTTPException(403, "Acesso negado")
    rows = db.execute(
        "SELECT * FROM trial_balance_entries WHERE entity_id=? AND fiscal_year_id=? ORDER BY conta",
        (entity_id, fiscal_year_id)
    ).fetchall()
    if not rows:
        raise HTTPException(404, "Sem dados importados para este período.")
    entries = [dict(r) for r in rows]
    return calculate_financials(entries)

@app.get("/api/entities/{entity_id}/summary")
def get_summary(entity_id: str, fiscal_year_id: str, db=Depends(get_db), user=Depends(get_current_user)):
    if not can_access_entity(entity_id, user): raise HTTPException(403, "Acesso negado")
    """Dashboard summary: KPIs + alerts + compliance checklist."""
    rows = db.execute(
        "SELECT * FROM trial_balance_entries WHERE entity_id=? AND fiscal_year_id=? ORDER BY conta",
        (entity_id, fiscal_year_id)
    ).fetchall()
    if not rows:
        raise HTTPException(404, "Sem dados importados para este período.")
    fin = calculate_financials([dict(r) for r in rows])

    # Compliance checklist (static for now, extend with real data)
    checklist = [
        {"item": "Balancete importado", "status": "done"},
        {"item": "SAF-T submetido AT", "status": "pending"},
        {"item": "IES 2024", "status": "pending"},
        {"item": "Modelo 22", "status": "pending"},
        {"item": "Pagamentos por conta", "status": "pending"},
    ]

    return {
        "financials": fin,
        "checklist": checklist,
        "generated_at": datetime.now().isoformat(),
    }


# ──────────────────────────────────────────
# ROUTES — IRC SIMULATION
# ──────────────────────────────────────────
@app.post("/api/irc/simulate")
def simulate_irc(req: IRCSimulationRequest, db=Depends(get_db), user=Depends(get_current_user)):
    """Full IRC calculation with user-supplied adjustments."""
    rows = db.execute(
        "SELECT * FROM trial_balance_entries WHERE entity_id=? AND fiscal_year_id=? ORDER BY conta",
        (req.entity_id, req.fiscal_year_id)
    ).fetchall()
    if not rows:
        raise HTTPException(404, "Sem dados de balancete para este período.")

    entries = [dict(r) for r in rows]
    fin = calculate_financials(entries)
    rai = fin["pnl"]["resultado_liquido"]

    # Tax adjustments
    mc_antes_perdas = rai + req.acrescimos - req.deducoes
    utilizacao_prejuizos = min(req.prejuizos_anteriores, 0.70 * mc_antes_perdas)
    mc_final = max(0, mc_antes_perdas - utilizacao_prejuizos)

    irc_reduzida = min(mc_final, 50000) * 0.17
    irc_normal = max(0, mc_final - 50000) * 0.21
    irc_bruto = irc_reduzida + irc_normal

    # Art.107.º n.1 CIRC PPC próximo ano: 80% (VN ≤ €500k) ou 95% (VN > €500k)
    fin_rows_ppc = db.execute(
        "SELECT * FROM trial_balance_entries WHERE entity_id=? AND fiscal_year_id=? ORDER BY conta",
        (req.entity_id, req.fiscal_year_id)
    ).fetchall()
    vn_irc = calculate_financials([dict(r) for r in fin_rows_ppc])["pnl"]["vn"] if fin_rows_ppc else 0
    pct_ppc = 0.95 if vn_irc > 500000 else 0.80
    pagamentos_conta_devidos = irc_bruto * pct_ppc

    # Acerto final: IRC bruto − retenções na fonte − PPC já pago
    saldo_final = irc_bruto - req.retencoes_na_fonte - req.pagamentos_conta


    result = {
        "input": {
            "rai": round(rai, 2),
            "acrescimos": req.acrescimos,
            "deducoes": req.deducoes,
            "prejuizos_anteriores": req.prejuizos_anteriores,
            "retencoes_na_fonte": req.retencoes_na_fonte,
            "pagamentos_conta": req.pagamentos_conta,
        },
        "calculation": {
            "mc_antes_perdas": round(mc_antes_perdas, 2),
            "utilizacao_prejuizos": round(utilizacao_prejuizos, 2),
            "mc_final": round(mc_final, 2),
            "irc_taxa_reduzida_base": min(mc_final, 50000),
            "irc_taxa_reduzida_valor": round(irc_reduzida, 2),
            "irc_taxa_normal_base": max(0, mc_final - 50000),
            "irc_taxa_normal_valor": round(irc_normal, 2),
            "irc_bruto": round(irc_bruto, 2),
            "irc_liquidado": round(irc_liquidado, 2),
            "acerto_final": round(saldo_acerto, 2),
        },
        "payments": {
            "pagamentos_conta_devidos_proximo_ano": round(pagamentos_conta_devidos, 2),
            "prestacao_jul": round(pagamentos_conta_devidos / 3, 2),
            "prestacao_set": round(pagamentos_conta_devidos / 3, 2),
            "prestacao_dez": round(pagamentos_conta_devidos - 2 * (pagamentos_conta_devidos // 3), 2),
            "saldo_acerto": round(saldo_final, 2),
            "status": "reembolso" if saldo_final < 0 else "a_pagar",
        },
        "effective_rate_pct": round((irc_bruto / rai * 100) if rai > 0 else 0, 1),
        "is_pme": True,
    }

    # Cache result
    calc_id = str(uuid.uuid4())
    db.execute("""INSERT INTO tax_calculations (id, entity_id, fiscal_year_id, calc_type, parameters, result)
                  VALUES (?,?,?,?,?,?)""",
               (calc_id, req.entity_id, req.fiscal_year_id, "irc",
                json.dumps(req.dict()), json.dumps(result)))
    db.commit()

    return result


# ──────────────────────────────────────────
# ROUTES — AI ASSISTANT
# ──────────────────────────────────────────
@app.post("/api/ai/chat")
async def ai_chat(req: ChatRequest, request: Request, db=Depends(get_db), _rl_=rate_limit(30, 60)):
    """
    AI assistant with financial context from the database.
    Uses Anthropic Claude API.
    """
    try:
        import anthropic
    except ImportError:
        raise HTTPException(500, "Anthropic SDK não instalado: pip install anthropic")

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise HTTPException(500, "ANTHROPIC_API_KEY não configurada")

    # Build context from DB
    context_str = ""
    if req.entity_id and req.fiscal_year_id:
        try:
            rows = db.execute(
                "SELECT * FROM trial_balance_entries WHERE entity_id=? AND fiscal_year_id=? ORDER BY conta",
                (req.entity_id, req.fiscal_year_id)
            ).fetchall()
            fin = calculate_financials([dict(r) for r in rows]) if rows else None
            entity = db.execute("SELECT * FROM entities WHERE id=?", (req.entity_id,)).fetchone()
            entity_name = dict(entity)["name"] if entity else "entidade"

            if not fin:
                context_str = "(Sem dados financeiros disponíveis)"
            else:
              context_str = f"""
DADOS REAIS DA EMPRESA:
Empresa: {entity_name}
Período: Exercício fiscal

DEMONSTRAÇÃO DE RESULTADOS:
- Total Rendimentos: €{fin['pnl']['total_rendimentos']:,.2f}
- Total Gastos: €{fin['pnl']['total_gastos']:,.2f}
- EBITDA: €{fin['pnl']['ebitda']:,.2f}
- Resultado Líquido: €{fin['pnl']['resultado_liquido']:,.2f}

BALANÇO:
- Ativo Total: €{fin['balanco']['ativo_total']:,.2f}
- Ativo Corrente: €{fin['balanco']['ativo_corrente']:,.2f}
- Passivo Total: €{fin['balanco']['passivo_total']:,.2f}
- Capital Próprio: €{fin['balanco']['capital_proprio']:,.2f}
- Disponibilidades: €{fin['balanco']['disponibilidades']:,.2f}
- Clientes: €{fin['balanco']['clientes']:,.2f}

RÁCIOS:
- Liquidez Geral: {fin['ratios']['liquidez_geral']}
- Solvabilidade: {fin['ratios']['solvabilidade']}
- Autonomia Financeira: {fin['ratios']['autonomia_financeira']}
- Margem Líquida: {fin['ratios']['margem_liquida']}%
- PMR: {fin['ratios']['pmr_dias']} dias
- PMP: {fin['ratios']['pmp_dias']} dias

IRC ESTIMADO: €{fin['irc_estimate']['irc_bruto']:,.2f} (taxa efetiva {fin['irc_estimate']['taxa_efetiva_pct']}%)

ALERTAS:
{chr(10).join(f"- [{a['severity'].upper()}] {a['title']}: {a['desc']}" for a in fin['alerts'])}
"""
        except Exception as e:
            context_str = f"(Contexto financeiro não disponível: {e})"

    system_prompt = f"""És o ContaIntel, um assistente de inteligência contabilística e fiscal para o mercado português.

Operares como um TOC/Contabilista Certificado com profundo conhecimento de:
- SNC (Sistema de Normalização Contabilística)
- CIRC (IRC), CIVA (IVA), IRS, EBF
- NCRF e IFRS
- Obrigações declarativas AT (IES, Modelo 22, SAF-T, IVA)
- CIRE (Código da Insolvência)

{f"CONTEXTO FINANCEIRO ATUAL:{context_str}" if context_str else ""}

INSTRUÇÕES:
- Responde sempre em português de Portugal
- Cita artigos específicos (ex: Art. 87.º n.º 2 CIRC) quando relevante
- Usa os dados reais fornecidos no contexto quando disponíveis
- Sê preciso e profissional — uso interno por contabilistas
- Formata a resposta de forma clara (usa **negrito** e listas quando útil)
- No final de análises fiscais acrescenta: "⚠️ Esta análise é informativa. Valide com o processo declarativo oficial."
"""

    client = anthropic.Anthropic(api_key=api_key)
    start = time.time()

    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1500,
        system=system_prompt,
        messages=[{"role": "user", "content": req.question}]
    )

    elapsed = round((time.time() - start) * 1000)
    answer = message.content[0].text

    # Log query
    query_id = str(uuid.uuid4())
    db.execute("""INSERT INTO ai_queries
                  (id, entity_id, fiscal_year_id, query_type, question, answer, tokens_used, response_ms)
                  VALUES (?,?,?,?,?,?,?,?)""",
               (query_id, req.entity_id, req.fiscal_year_id, "chat",
                req.question, answer, message.usage.output_tokens, elapsed))
    db.commit()

    return {
        "answer": answer,
        "query_id": query_id,
        "response_ms": elapsed,
        "tokens_used": message.usage.output_tokens,
    }


# ──────────────────────────────────────────
# ROUTES — HEALTH & METADATA
# ──────────────────────────────────────────
@app.get("/api/prazos-fiscais")
def get_prazos_fiscais():
    """Return fiscal deadlines for the current year."""
    import datetime
    now = datetime.datetime.now()
    year = now.year
    prazos = [
        {"id":"irs-ret",    "titulo":"IRS — Retenções na Fonte",         "data":f"{year}-01-20","tipo":"mensal",  "descricao":"Entrega declaração e pagamento retenções IRS/IRC mês anterior","regulamento":"Art. 98º CIRS"},
        {"id":"iva-m",      "titulo":"IVA — Declaração Mensal",          "data":f"{year}-02-10","tipo":"mensal",  "descricao":"Declaração periódica IVA (regime mensal)","regulamento":"Art. 41º CIVA"},
        {"id":"iva-t1",     "titulo":"IVA — Declaração 1º Trimestre",    "data":f"{year}-05-15","tipo":"trimestral","descricao":"Declaração periódica IVA (regime trimestral)","regulamento":"Art. 41º CIVA"},
        {"id":"iva-t2",     "titulo":"IVA — Declaração 2º Trimestre",    "data":f"{year}-08-15","tipo":"trimestral","descricao":"Declaração periódica IVA (regime trimestral)","regulamento":"Art. 41º CIVA"},
        {"id":"iva-t3",     "titulo":"IVA — Declaração 3º Trimestre",    "data":f"{year}-11-15","tipo":"trimestral","descricao":"Declaração periódica IVA (regime trimestral)","regulamento":"Art. 41º CIVA"},
        {"id":"mod22",      "titulo":"Modelo 22 — IRC",                  "data":f"{year}-07-31","tipo":"anual",   "descricao":"Declaração periódica de rendimentos IRC","regulamento":"Art. 120º CIRC"},
        {"id":"ppc-jul",    "titulo":"Pagamento por Conta — Julho",      "data":f"{year}-07-31","tipo":"anual",   "descricao":"1ª prestação pagamento por conta IRC","regulamento":"Art. 107º CIRC"},
        {"id":"ppc-set",    "titulo":"Pagamento por Conta — Setembro",   "data":f"{year}-09-30","tipo":"anual",   "descricao":"2ª prestação pagamento por conta IRC","regulamento":"Art. 107º CIRC"},
        {"id":"ppc-dez",    "titulo":"Pagamento por Conta — Dezembro",   "data":f"{year}-12-15","tipo":"anual",   "descricao":"3ª prestação pagamento por conta IRC","regulamento":"Art. 107º CIRC"},
        {"id":"ies",        "titulo":"IES — Informação Empresarial",     "data":f"{year}-07-15","tipo":"anual",   "descricao":"Informação Empresarial Simplificada (Mod. Q)","regulamento":"Dec. Lei 8/2007"},
        {"id":"dmr",        "titulo":"DMR — Remunerações",               "data":f"{year}-01-10","tipo":"mensal",  "descricao":"Declaração Mensal de Remunerações AT","regulamento":"Art. 119º CIRS"},
        {"id":"ss-dri",     "titulo":"Seg. Social — DRI",                "data":f"{year}-01-10","tipo":"mensal",  "descricao":"Declaração de Remunerações Seg. Social","regulamento":"Cód. Contributivo"},
        {"id":"pec",        "titulo":"PEC — Pagamento Especial Conta",   "data":f"{year}-03-31","tipo":"anual",   "descricao":"Pagamento especial por conta (1ª prestação)","regulamento":"Art. 106º CIRC"},
        {"id":"rel-gest",   "titulo":"Relatório de Gestão + Contas",     "data":f"{year}-03-31","tipo":"anual",   "descricao":"Aprovação contas anuais em AG","regulamento":"Art. 65º CSC"},
        {"id":"toc-ata",    "titulo":"Depósito de Atas + Balanço",       "data":f"{year}-07-15","tipo":"anual",   "descricao":"Registo Comercial — depósito de contas","regulamento":"Art. 70º CSC"},
    ]
    # Calculate days until each deadline
    for p in prazos:
        try:
            deadline = datetime.datetime.strptime(p["data"], "%Y-%m-%d")
            delta = (deadline - now).days
            p["dias_restantes"] = delta
            p["status"] = "vencido" if delta < 0 else "urgente" if delta <= 15 else "proximo" if delta <= 45 else "ok"
        except:
            p["dias_restantes"] = None
            p["status"] = "ok"
    return sorted(prazos, key=lambda x: x["data"])

@app.get("/api/health")
def health():
    return {"status": "ok", "version": "1.0.0", "timestamp": datetime.now().isoformat()}

@app.get("/api/files")
def list_files(entity_id: str = None, db=Depends(get_db), user=Depends(get_current_user)):
    query = "SELECT tbf.*, e.name as entity_name FROM trial_balance_files tbf JOIN entities e ON tbf.entity_id=e.id"
    params = []
    if entity_id:
        query += " WHERE tbf.entity_id=?"
        params.append(entity_id)
    query += " ORDER BY tbf.imported_at DESC"
    rows = db.execute(query, params).fetchall()
    return [dict(r) for r in rows]

@app.delete("/api/files/{file_id}")
def delete_file(file_id: str, db=Depends(get_db), user=Depends(get_current_user)):
    """Delete a trial balance file and all its entries."""
    row = db.execute("SELECT * FROM trial_balance_files WHERE id=?", (file_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Ficheiro não encontrado")
    # Check entity access
    if not can_access_entity(row["entity_id"], user):
        raise HTTPException(403, "Acesso negado")
    db.execute("DELETE FROM trial_balance_entries WHERE file_id=?", (file_id,))
    db.execute("DELETE FROM trial_balance_files WHERE id=?", (file_id,))
    db.commit()
    # Remove from disk if exists
    try:
        for p in UPLOADS_DIR.glob(f"{file_id}_*"):
            p.unlink(missing_ok=True)
    except Exception:
        pass
    return {"status": "deleted", "file_id": file_id}

@app.get("/api/budget/{entity_id}/{year}")
def get_budget(entity_id: str, year: int, db=Depends(get_db), user=Depends(get_current_user)):
    """Carregar orçamento de uma empresa/ano da base de dados."""
    if not can_access_entity(entity_id, user):
        raise HTTPException(403, "Acesso negado")
    row = db.execute(
        "SELECT data FROM budgets WHERE entity_id=? AND year=?", (entity_id, year)
    ).fetchone()
    if not row:
        return {"data": {}}
    import json
    return {"data": json.loads(row["data"])}


@app.put("/api/budget/{entity_id}/{year}")
def save_budget(entity_id: str, year: int, payload: dict, db=Depends(get_db), user=Depends(get_current_user)):
    """Gravar/actualizar orçamento de uma empresa/ano."""
    if not can_access_entity(entity_id, user):
        raise HTTPException(403, "Acesso negado")
    import json, uuid
    data_json = json.dumps(payload.get("data", {}))
    db.execute("""
        INSERT INTO budgets (id, entity_id, year, data, updated_at)
        VALUES (?, ?, ?, ?, datetime('now'))
        ON CONFLICT(entity_id, year) DO UPDATE SET data=excluded.data, updated_at=excluded.updated_at
    """, (str(uuid.uuid4()), entity_id, year, data_json))
    db.commit()
    return {"ok": True}


@app.get("/api/irc/saved")
def list_irc_saved(entity_id: str, fiscal_year_id: str = None, db=Depends(get_db), user=Depends(get_current_user)):
    """List saved IRC calculations for an entity."""
    if not can_access_entity(entity_id, user):
        raise HTTPException(403, "Acesso negado")
    q = "SELECT * FROM tax_calculations WHERE entity_id=? AND calc_type='irc'"
    params = [entity_id]
    if fiscal_year_id:
        q += " AND fiscal_year_id=?"
        params.append(fiscal_year_id)
    q += " ORDER BY calculated_at DESC LIMIT 20"
    rows = db.execute(q, params).fetchall()
    result = []
    for r in rows:
        d = dict(r)
        try:
            d["result_parsed"] = json.loads(d["result"] or "{}")
        except Exception:
            d["result_parsed"] = {}
        result.append(d)
    return result

@app.get("/")
def root():
    # Try ContaIntel.html first, then index.html
    for name in ["ContaIntel.html", "index.html"]:
        f = FRONTEND_DIR / name
        if f.exists():
            return FileResponse(str(f))
    return {"message": "ContaIntel API v1.0 — Aceda a /docs para a documentação"}

# ─────────────────────────────────────────────────────────────
# CONFORMIDADE — rastreio de estado por obrigação declarativa
# ─────────────────────────────────────────────────────────────

# Catálogo de obrigações com metadata (independente do estado por empresa)
OBRIGACOES_CATALOG = [
    {"id":"ies",       "titulo":"IES — Informação Empresarial Simplificada",    "prazo_mes":7,  "prazo_dia":15, "regulamento":"Art.121.º CIRC"},
    {"id":"mod22",     "titulo":"Modelo 22 — Declaração Periódica IRC",          "prazo_mes":7,  "prazo_dia":31, "regulamento":"Art.120.º CIRC"},
    {"id":"mod10",     "titulo":"Modelo 10 — Rendimentos Pagos a Residentes",    "prazo_mes":2,  "prazo_dia":28, "regulamento":"Art.119.º CIRS"},
    {"id":"iva-t1",    "titulo":"IVA — Declaração 1.º Trimestre",                "prazo_mes":5,  "prazo_dia":15, "regulamento":"Art.41.º CIVA"},
    {"id":"iva-t2",    "titulo":"IVA — Declaração 2.º Trimestre",                "prazo_mes":8,  "prazo_dia":15, "regulamento":"Art.41.º CIVA"},
    {"id":"iva-t3",    "titulo":"IVA — Declaração 3.º Trimestre",                "prazo_mes":11, "prazo_dia":15, "regulamento":"Art.41.º CIVA"},
    {"id":"iva-t4",    "titulo":"IVA — Declaração 4.º Trimestre",                "prazo_mes":2,  "prazo_dia":15, "regulamento":"Art.41.º CIVA"},
    {"id":"ppc-jul",   "titulo":"Pagamento por Conta — 1.ª prestação (Julho)",   "prazo_mes":7,  "prazo_dia":31, "regulamento":"Art.104.º CIRC"},
    {"id":"ppc-set",   "titulo":"Pagamento por Conta — 2.ª prestação (Setembro)","prazo_mes":9,  "prazo_dia":30, "regulamento":"Art.104.º CIRC"},
    {"id":"ppc-dez",   "titulo":"Pagamento por Conta — 3.ª prestação (Dezembro)","prazo_mes":12, "prazo_dia":15, "regulamento":"Art.104.º CIRC"},
    {"id":"saft-cont", "titulo":"SAF-T Contabilidade — entrega anual",           "prazo_mes":7,  "prazo_dia":15, "regulamento":"Port.302/2016"},
    {"id":"dmc",       "titulo":"DMC — Declaração Mensal de Remunerações",       "prazo_mes":0,  "prazo_dia":10, "regulamento":"Art.119.º CIRS", "mensal":True},
]

@app.get("/api/conformidade/{entity_id}/{year}")
def get_conformidade(entity_id: str, year: int, db=Depends(get_db), user=Depends(get_current_user)):
    """Retorna o catálogo de obrigações com o estado actual desta empresa/ano."""
    if not can_access_entity(entity_id, user):
        raise HTTPException(403, "Acesso negado")

    rows = db.execute(
        "SELECT obrigacao_id, estado, data_conclusao, notas, updated_at "
        "FROM conformidade_items WHERE entity_id=? AND year=?",
        (entity_id, year)
    ).fetchall()
    estado_map = {r["obrigacao_id"]: dict(r) for r in rows}

    result = []
    for ob in OBRIGACOES_CATALOG:
        item = dict(ob)
        s = estado_map.get(ob["id"], {})
        item["estado"]         = s.get("estado", "pendente")
        item["data_conclusao"] = s.get("data_conclusao")
        item["notas"]          = s.get("notas", "")
        item["updated_at"]     = s.get("updated_at")
        # Calcular prazo absoluto para o ano em questão
        if ob["prazo_mes"] > 0:
            import calendar
            last_day = calendar.monthrange(year, ob["prazo_mes"])[1]
            dia = min(ob["prazo_dia"], last_day)
            item["prazo_data"] = f"{year}-{ob['prazo_mes']:02d}-{dia:02d}"
        else:
            item["prazo_data"] = None  # mensal — não tem prazo anual fixo
        result.append(item)

    return result


@app.put("/api/conformidade/{entity_id}/{year}/{obrigacao_id}")
def update_conformidade(
    entity_id: str, year: int, obrigacao_id: str,
    payload: dict,
    db=Depends(get_db), user=Depends(get_current_user)
):
    """Actualiza o estado de uma obrigação (pendente/concluido/nao_aplicavel)."""
    if not can_access_entity(entity_id, user):
        raise HTTPException(403, "Acesso negado")

    valid_ids = {ob["id"] for ob in OBRIGACOES_CATALOG}
    if obrigacao_id not in valid_ids:
        raise HTTPException(400, f"Obrigação desconhecida: {obrigacao_id}")

    estado = payload.get("estado", "pendente")
    if estado not in ("pendente", "concluido", "nao_aplicavel"):
        raise HTTPException(400, "Estado inválido")

    import uuid as _uuid
    db.execute("""
        INSERT INTO conformidade_items (id, entity_id, year, obrigacao_id, estado, data_conclusao, notas, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(entity_id, year, obrigacao_id) DO UPDATE SET
            estado=excluded.estado,
            data_conclusao=excluded.data_conclusao,
            notas=excluded.notas,
            updated_at=datetime('now')
    """, (
        str(_uuid.uuid4()), entity_id, year, obrigacao_id,
        estado,
        payload.get("data_conclusao"),
        payload.get("notas", "")
    ))
    db.commit()
    return {"ok": True, "estado": estado}


@app.get("/api/conformidade/{entity_id}/{year}/summary")
def get_conformidade_summary(entity_id: str, year: int, db=Depends(get_db), user=Depends(get_current_user)):
    """Resumo rápido: total, concluídos, pendentes."""
    if not can_access_entity(entity_id, user):
        raise HTTPException(403, "Acesso negado")
    rows = db.execute(
        "SELECT estado, COUNT(*) as n FROM conformidade_items WHERE entity_id=? AND year=? GROUP BY estado",
        (entity_id, year)
    ).fetchall()
    counts = {r["estado"]: r["n"] for r in rows}
    total = len(OBRIGACOES_CATALOG)
    concluidos = counts.get("concluido", 0)
    nao_aplic  = counts.get("nao_aplicavel", 0)
    return {
        "total": total,
        "concluidos": concluidos,
        "nao_aplicavel": nao_aplic,
        "pendentes": total - concluidos - nao_aplic,
        "pct": round(concluidos / max(total - nao_aplic, 1) * 100),
    }


@app.get("/app")
def app_page():
    return root()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)

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

from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Depends, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
import sqlite3
import openpyxl

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

# ── JWT CONFIG ──
JWT_SECRET = os.environ.get("JWT_SECRET", secrets.token_hex(32))
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
    finally:
        try:
            conn.commit()
        except:
            pass
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

    ebitda = resultado_liquido + dep_val + fin_val

    # ── BALANÇO ──
    caixa_dev = get_val("11", "devedor")
    caixa_cred = get_val("11", "credor")
    depositos = get_val("12", "devedor")
    clientes_val = get_val("21", "devedor")
    fornec_val = get_val("22", "credor")
    pessoal_pass = get_val("23", "credor")
    estado_val = get_val("24", "credor")
    financiamentos_val = get_val("25", "credor")
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
    liq_red = sd(ac, pc)
    liq_im = sd(disp, pc)
    solvabilidade = sd(cp, passivo)
    autonomia = sd(cp, at_total)
    endividamento = sd(passivo, at_total)
    mg_ebitda = sd(ebitda, total_rendimentos) * 100
    mg_liquida = sd(resultado_liquido, total_rendimentos) * 100
    roe = sd(resultado_liquido, cp) * 100 if cp > 0 else 0
    roa = sd(ebitda - fin_val, at_total) * 100 if at_total > 0 else 0
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
    vn = prestacoes + get_val("71", "credor")  # 71=Vendas, 72=PS
    vn_total = vn + prestacoes if vn > 0 else prestacoes
    cmvmc = get_val("61", "devedor")            # 61=CMVMC
    mg_bruta = vn_total - cmvmc
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
def login(req: LoginRequest, db=Depends(get_db)):
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
def create_entity(entity: EntityCreate, db=Depends(get_db)):
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
def get_entity(entity_id: str, db=Depends(get_db)):
    row = db.execute("SELECT * FROM entities WHERE id=?", (entity_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Entidade não encontrada")
    entity = dict(row)
    years = db.execute("SELECT * FROM fiscal_years WHERE entity_id=? ORDER BY year DESC",
                        (entity_id,)).fetchall()
    entity["fiscal_years"] = [dict(y) for y in years]
    return entity

@app.delete("/api/entities/{entity_id}")
def delete_entity(entity_id: str, db=Depends(get_db)):
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
    entity_id: str = Form(...),
    fiscal_year_id: str = Form(...),
    file: UploadFile = File(...),
):
    db = new_conn()
    """
    Import a trial balance Excel file.
    Auto-detects column structure and maps to SNC accounts.
    """
    start = time.time()

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

    result = {
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
    db.close()
    return result


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
    fin = get_financials(entity_id, fiscal_year_id, db=db)

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
def simulate_irc(req: IRCSimulationRequest, db=Depends(get_db)):
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
    irc_liquido = max(0, irc_bruto - req.retencoes_na_fonte)

    pagamentos_conta_devidos = irc_bruto * 0.95
    saldo_final = irc_liquido - req.pagamentos_conta

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
            "irc_liquido": round(irc_liquido, 2),
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
async def ai_chat(req: ChatRequest, db=Depends(get_db)):
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
            fin = get_financials(req.entity_id, req.fiscal_year_id, db=db)
            entity = db.execute("SELECT * FROM entities WHERE id=?", (req.entity_id,)).fetchone()
            entity_name = dict(entity)["name"] if entity else "entidade"

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
def list_files(entity_id: str = None, db=Depends(get_db)):
    query = "SELECT tbf.*, e.name as entity_name FROM trial_balance_files tbf JOIN entities e ON tbf.entity_id=e.id"
    params = []
    if entity_id:
        query += " WHERE tbf.entity_id=?"
        params.append(entity_id)
    query += " ORDER BY tbf.imported_at DESC"
    rows = db.execute(query, params).fetchall()
    return [dict(r) for r in rows]

@app.get("/")
def root():
    # Try ContaIntel.html first, then index.html
    for name in ["ContaIntel.html", "index.html"]:
        f = FRONTEND_DIR / name
        if f.exists():
            return FileResponse(str(f))
    return {"message": "ContaIntel API v1.0 — Aceda a /docs para a documentação"}

@app.get("/app")
def app_page():
    return root()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)

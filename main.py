import os
import certifi
import requests
import yfinance as yf
import pandas as pd
import math
from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Float, Date, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from datetime import date
from typing import List, Optional

# --- CONFIGURAÇÃO DO BANCO DE DADOS (PostgreSQL ou SQLite) ---
# Vai buscar o URL ao Render. Se não existir, usa SQLite localmente.
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./meu_patrimonio_v2.db")

# O SQLAlchemy exige "postgresql://" mas algumas plataformas dão "postgres://"
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# Argumentos de conexão (SQLite precisa de check_same_thread, Postgres não aceita isso)
connect_args = {"check_same_thread": False} if "sqlite" in DATABASE_URL else {}

engine = create_engine(DATABASE_URL, connect_args=connect_args)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class WalletDB(Base):
    __tablename__ = "wallets"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    description = Column(String, nullable=True)

class TransactionDB(Base):
    __tablename__ = "transactions"
    id = Column(Integer, primary_key=True, index=True)
    wallet_id = Column(Integer, ForeignKey("wallets.id"), nullable=False)
    ticker = Column(String, index=True)
    date = Column(Date)
    quantity = Column(Float)
    price = Column(Float)
    type = Column(String)

Base.metadata.create_all(bind=engine)

class WalletCreate(BaseModel):
    name: str
    description: Optional[str] = None

class TransactionCreate(BaseModel):
    wallet_id: int
    ticker: str
    date: date
    quantity: float
    price: float
    type: str

app = FastAPI(title="Investidor12 API")

# Permite que o Vercel comunique com esta API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_db():
    db = SessionLocal()
    try: yield db
    finally: db.close()

def safe_float(val):
    try:
        if val is None: return 0.0
        if isinstance(val, (pd.Series, pd.DataFrame)):
            if val.empty: return 0.0
            val = val.iloc[0]
        if hasattr(val, 'item'): val = val.item()
        f = float(val)
        if math.isnan(f) or math.isinf(f): return 0.0
        return f
    except: return 0.0

def get_realtime_price_sync(ticker):
    try:
        t = yf.Ticker(ticker)
        p = t.fast_info.get('last_price')
        if p and not math.isnan(p): return ticker, safe_float(p)
        h = t.history(period="1d")
        if not h.empty: return ticker, safe_float(h['Close'].iloc[-1])
        return ticker, 0.0
    except: return ticker, 0.0

def fetch_prices_sync(tickers):
    res = {}
    for t in tickers:
        tick, p = get_realtime_price_sync(t)
        res[tick] = p
    return res

def get_divs_sync(ticker):
    try: return ticker, yf.Ticker(ticker).dividends
    except: return ticker, None

@app.get("/")
def read_root():
    return {"status": "API Investidor12 Online"}

@app.post("/wallets/")
def create_wallet(wallet: WalletCreate, db: Session = Depends(get_db)):
    db_wallet = WalletDB(name=wallet.name, description=wallet.description)
    db.add(db_wallet)
    db.commit()
    db.refresh(db_wallet)
    return db_wallet

@app.get("/wallets/")
def list_wallets(db: Session = Depends(get_db)):
    wallets = db.query(WalletDB).all()
    if not wallets:
        def_wallet = WalletDB(name="Carteira Principal", description="Padrão")
        db.add(def_wallet)
        db.commit()
        db.refresh(def_wallet)
        return [def_wallet]
    return wallets

@app.post("/transactions/")
def create_transaction(trans: TransactionCreate, db: Session = Depends(get_db)):
    db.add(TransactionDB(wallet_id=trans.wallet_id, ticker=trans.ticker.upper(), date=trans.date, quantity=trans.quantity, price=trans.price, type=trans.type))
    db.commit()
    return {"message": "OK"}

@app.post("/transactions/bulk")
def create_bulk(transactions: List[TransactionCreate], db: Session = Depends(get_db)):
    for t in transactions:
        db.add(TransactionDB(wallet_id=t.wallet_id, ticker=t.ticker.upper(), date=t.date, quantity=t.quantity, price=t.price, type=t.type))
    db.commit()
    return {"message": "OK"}

@app.delete("/transactions/clear_all")
def clear_all(wallet_id: int, db: Session = Depends(get_db)):
    db.query(TransactionDB).filter(TransactionDB.wallet_id == wallet_id).delete()
    db.commit()
    return {"message": "Limpo"}

@app.get("/transactions/")
def list_transactions(wallet_id: int, db: Session = Depends(get_db)):
    res = db.query(TransactionDB).filter(TransactionDB.wallet_id == wallet_id).order_by(TransactionDB.date.desc()).all()
    return res if res else []

@app.delete("/transactions/{id}")
def delete_trans(id: int, db: Session = Depends(get_db)):
    db.query(TransactionDB).filter(TransactionDB.id == id).delete()
    db.commit()
    return {"message": "OK"}

@app.put("/transactions/{id}")
def update_trans(id: int, trans: TransactionCreate, db: Session = Depends(get_db)):
    t = db.query(TransactionDB).filter(TransactionDB.id == id).first()
    if t:
        t.wallet_id = trans.wallet_id
        t.ticker = trans.ticker.upper()
        t.date = trans.date
        t.quantity = trans.quantity
        t.price = trans.price
        t.type = trans.type
        db.commit()
    return {"message": "OK"}

@app.get("/get-price")
def price_check(ticker: str, date: str):
    ticker_upper = ticker.upper()
    try:
        # Converte a string YYYY-MM-DD do frontend para objeto Date
        target_date = datetime.strptime(date, "%Y-%m-%d").date()
        hoje = datetime.now().date()
        
        # Se a data for hoje ou no futuro, pega a cotação em tempo real
        if target_date >= hoje:
            _, p = get_realtime_price_sync(ticker_upper)
            return {"price": round(p, 2)}
            
        # Se for data no passado, busca o histórico no Yahoo Finance
        # Damos uma janela de 5 dias úteis caso a data caia num sábado/domingo/feriado
        next_days = target_date + timedelta(days=5)
        
        t = yf.Ticker(ticker_upper)
        h = t.history(start=target_date.strftime("%Y-%m-%d"), end=next_days.strftime("%Y-%m-%d"))
        
        if not h.empty:
            # Pega o preço de fechamento do primeiro dia útil daquela janela
            p = safe_float(h['Close'].iloc[0])
            return {"price": round(p, 2)}
            
        # Fallback de segurança caso a API do Yahoo falhe
        _, p = get_realtime_price_sync(ticker_upper)
        return {"price": round(p, 2)}
        
    except Exception as e:
        print(f"Erro ao buscar preço histórico para {ticker_upper} em {date}: {e}")
        _, p = get_realtime_price_sync(ticker_upper)
        return {"price": round(p, 2)}

@app.get("/earnings")
def get_earnings(wallet_id: int, db: Session = Depends(get_db)):
    trans = db.query(TransactionDB).filter(TransactionDB.wallet_id == wallet_id).all()
    if not trans: return {"total_acumulado": 0, "historico_mensal": [], "por_ativo": [], "por_classe": [], "detalhes": [], "provisionados": []}

    holdings = {}
    for t in trans:
        if t.ticker not in holdings: holdings[t.ticker] = []
        holdings[t.ticker].append(t)

    div_data = {}
    for t in list(holdings.keys()):
        tick, d = get_divs_sync(t)
        if d is not None: div_data[tick] = d

    total_recebido = 0
    monthly = {}
    by_ticker = {}
    by_class = {}
    detalhes = []
    provisionados = []
    hoje = date.today()

    for ticker, t_list in holdings.items():
        if ticker not in div_data or div_data[ticker].empty: continue
        divs = div_data[ticker]
        if divs.index.tz is not None: divs.index = divs.index.tz_localize(None)
        
        first_buy_date = min(t.date for t in t_list)
        first_buy_ts = pd.Timestamp(first_buy_date)
        try: divs = divs[divs.index >= first_buy_ts]
        except: pass

        asset_type = t_list[0].type
        tick_val = 0
        
        for dt, val in divs.items():
            qty = sum(t.quantity for t in t_list if t.date < dt.date())
            if qty > 0:
                payment = qty * val
                obj = {"date": dt.strftime("%Y-%m-%d"), "ticker": ticker.replace('.SA', ''), "type": asset_type, "val": safe_float(payment), "qtd": safe_float(qty), "unit_val": safe_float(val)}
                if dt.date() > hoje: provisionados.append(obj)
                else:
                    tick_val += payment
                    total_recebido += payment
                    detalhes.append(obj)
                    m = dt.strftime("%Y-%m")
                    if m not in monthly: monthly[m] = {"total":0}
                    monthly[m]["total"] += payment
                    if asset_type not in monthly[m]: monthly[m][asset_type] = 0
                    monthly[m][asset_type] += payment
        
        if tick_val > 0:
            by_ticker[ticker] = tick_val
            by_class[asset_type] = by_class.get(asset_type, 0) + tick_val

    detalhes.sort(key=lambda x: x['date'], reverse=True)
    provisionados.sort(key=lambda x: x['date'])
    hist = [{"mes": date(int(y), int(mo), 1).strftime("%b/%y"), "total": safe_float(monthly[m]["total"]), **{k: safe_float(v) for k, v in monthly[m].items() if k != "total"}} for m in sorted(monthly.keys()) for y, mo in [m.split('-')]]
    t_list = sorted([{"name": k.replace('.SA',''), "value": safe_float(v)} for k,v in by_ticker.items()], key=lambda x:x["value"], reverse=True)
    c_list = sorted([{"name": k, "value": safe_float(v)} for k,v in by_class.items()], key=lambda x:x["value"], reverse=True)
    return {"total_acumulado": safe_float(total_recebido), "historico_mensal": hist, "por_ativo": t_list, "por_classe": c_list, "detalhes": detalhes, "provisionados": provisionados}

@app.get("/dashboard")
def get_dashboard(wallet_id: int, db: Session = Depends(get_db)):
    trans = db.query(TransactionDB).filter(TransactionDB.wallet_id == wallet_id).all()
    if not trans: return {"patrimonio_atual":0,"total_investido":0,"lucro":0,"rentabilidade_pct":0,"daily_variation":0,"grafico":[],"ativos":[]}

    tickers = list(set(t.ticker for t in trans))
    precos_atuais = fetch_prices_sync(tickers)

    try:
        data_inicio_real = min(t.date for t in trans)
        lista_download = tickers + ['^BVSP']
        dados_historicos = yf.download(lista_download, start=data_inicio_real, progress=False, auto_adjust=True, threads=False)['Close']
        if not dados_historicos.empty: dados_historicos = dados_historicos.ffill().bfill()
    except: dados_historicos = pd.DataFrame()

    patrimonio_total = investido_total = daily_var_money = patrimonio_ontem = 0
    ativos_finais = []
    
    trans_map = {}
    for t in trans:
        if t.ticker not in trans_map: trans_map[t.ticker] = {"qtd":0, "custo":0, "type":t.type}
        trans_map[t.ticker]["qtd"] += t.quantity
        trans_map[t.ticker]["custo"] += (t.quantity * t.price)

    is_weekend = date.today().weekday() >= 5

    for tick, d in trans_map.items():
        if d["qtd"] > 0:
            pm = d["custo"] / d["qtd"]
            p_atual = precos_atuais.get(tick, 0.0)
            if p_atual == 0: p_atual = pm 
            
            tot = d["qtd"] * p_atual
            lucro_ativo = tot - d["custo"]
            rent = ((p_atual - pm) / pm) * 100 if pm > 0 else 0
            
            var_pct = 0.0
            if not is_weekend and not dados_historicos.empty:
                try:
                    s = dados_historicos[tick] if len(tickers) > 1 else dados_historicos
                    s = s.dropna()
                    if len(s) >= 2:
                        var_pct = ((safe_float(s.iloc[-1]) - safe_float(s.iloc[-2])) / safe_float(s.iloc[-2])) * 100
                except: pass
            
            money_var = tot - (tot / (1 + var_pct/100))
            patrimonio_total += tot
            investido_total += d["custo"]
            daily_var_money += money_var
            patrimonio_ontem += (tot - money_var)
            
            ativos_finais.append({"ticker": tick, "type": d["type"], "qtd": safe_float(d["qtd"]), "pm": safe_float(pm), "atual": safe_float(p_atual), "total": safe_float(tot), "rentabilidade": safe_float(rent), "lucro_valor": safe_float(lucro_ativo), "variacao_diaria": safe_float(var_pct), "variacao_diaria_valor": safe_float(money_var)})

    ativos_finais.sort(key=lambda x:x['total'], reverse=True)
    lucro = patrimonio_total - investido_total
    rent_total = (lucro / investido_total * 100) if investido_total > 0 else 0
    daily_pct = (daily_var_money / patrimonio_ontem * 100) if patrimonio_ontem > 0 else 0

    chart_data = []
    if not dados_historicos.empty:
        if dados_historicos.index.tz is not None: dados_historicos.index = dados_historicos.index.tz_localize(None)
        
        hist_slice = dados_historicos[dados_historicos.index >= pd.Timestamp(data_inicio_real)]
        cdi_acc, ipca_acc, ibov_start = 1.0, 1.0, 0
        try:
            if '^BVSP' in hist_slice.columns and not hist_slice['^BVSP'].dropna().empty: ibov_start = safe_float(hist_slice['^BVSP'].dropna().iloc[0])
            elif isinstance(hist_slice, pd.Series) and hist_slice.name == '^BVSP': ibov_start = safe_float(hist_slice.iloc[0])
        except: pass

        posicao, custo, trans_date = {t:0 for t in tickers}, 0, {}
        for tr in trans:
            ds = tr.date.strftime('%Y-%m-%d')
            if ds not in trans_date: trans_date[ds] = []
            trans_date[ds].append(tr)

        for ts in hist_slice.index:
            ds = ts.strftime('%Y-%m-%d')
            cdi_acc *= 1.0004
            ipca_acc *= 1.00017
            
            if ds in trans_date:
                for tr in trans_date[ds]:
                    posicao[tr.ticker] += tr.quantity
                    custo += (tr.quantity * tr.price)
            
            if custo > 0:
                val_mercado = 0
                try:
                    if len(tickers) == 1: 
                        p = safe_float(hist_slice.loc[ts]) if tickers[0] in str(hist_slice.name) else 0
                        if p > 0: val_mercado = posicao[tickers[0]] * p
                    else:
                        row = hist_slice.loc[ts]
                        for t, q in posicao.items():
                            if q > 0 and t in row:
                                p = safe_float(row[t])
                                if p > 0: val_mercado += q * p
                except: pass
                
                if val_mercado == 0: val_mercado = custo
                rent_cart = ((val_mercado - custo) / custo * 100)
                
                rent_ibov = 0
                try:
                    if ibov_start > 0:
                        curr = safe_float(hist_slice.loc[ts]['^BVSP']) if isinstance(hist_slice, pd.DataFrame) and '^BVSP' in hist_slice.columns else (safe_float(hist_slice.loc[ts]) if isinstance(hist_slice, pd.Series) and hist_slice.name == '^BVSP' else 0)
                        if curr > 0: rent_ibov = ((curr - ibov_start) / ibov_start) * 100
                except: pass
                
                chart_data.append({"name": ts.strftime("%d/%m/%y"), "carteira": safe_float(rent_cart), "ibov": safe_float(rent_ibov), "cdi": safe_float((cdi_acc - 1) * 100), "ipca": safe_float((ipca_acc - 1) * 100)})

    return {"patrimonio_atual": safe_float(round(patrimonio_total, 2)), "total_investido": safe_float(round(investido_total, 2)), "lucro": safe_float(round(lucro, 2)), "rentabilidade_pct": safe_float(round(rent_total, 2)), "daily_variation": safe_float(round(daily_pct, 2)), "grafico": chart_data, "ativos": ativos_finais}

@app.get("/analyze/{ticker}")
def analyze(ticker: str):
    ticker = ticker.upper().strip()
    clean = ticker.replace('.SA', '')
    if not ticker.endswith('.SA'): ticker += '.SA'
    
    res = { "ticker": clean, "price": 0, "score": 0, "verdict": "Indisponível", "criteria": { "profit": {"status": "GRAY"}, "governance": {"status": "GRAY"}, "debt": {"status": "GRAY"}, "ipo": {"status": "GRAY"} } }
    try:
        t = yf.Ticker(ticker)
        try: i = t.info
        except: return res
        if not i: return res
        res["price"] = i.get('currentPrice', 0)
        
        ipo = 0
        MAMUTES = ['VALE3','PETR4','PETR3','ITUB4','BBDC4','BBDC3','BBAS3','ABEV3','WEGE3','EGIE3','ITSA4','SANB11','GGBR4','GOAU4','CMIG4','ELET3','ELET6','CSNA3','RADL3','VIVT3','TIMS3','CSMG3','SBSP3','CPLE6','TAEE11','KLBN11','VULC3','LEVE3','TUPY3','PRIO3','POMO4','SAPR11','TRPL4','FLRY3','RENT3','LREN3','JBSS3','BRFS3','MGLU3']
        if clean in MAMUTES: ipo = 20
        else:
            ts = i.get('firstTradeDateEpochUtc')
            if ts: ipo = (datetime.now() - datetime.fromtimestamp(ts)).days / 365
            else: ipo = 5 
        s_ipo = "GREEN" if ipo > 7 else ("YELLOW" if ipo >= 5 else "RED")
        sc = 2 if s_ipo == "GREEN" else (1 if s_ipo == "YELLOW" else 0)
        
        prof_ok = False
        yrs = 0
        try:
            fin = t.financials
            if not fin.empty and 'Net Income' in fin.index:
                v = [x for x in fin.loc['Net Income'].values if not pd.isna(x)]
                if v:
                    yrs = len(v)
                    if sum(1 for x in v if x > 0) == yrs: prof_ok = True
        except: pass
        if yrs == 0 and i.get('trailingEps', 0) > 0: prof_ok = True
        s_prof = "GREEN" if prof_ok else "RED"
        sc += 2 if prof_ok else 0
        
        dr = i.get('debtToEbitda')
        s_debt = "GREEN"
        if dr and dr > 3: s_debt = "RED"
        elif dr and dr > 2: s_debt = "YELLOW"; sc += 1
        else: sc += 2
        
        on = clean[-1] == '3'
        s_gov = "GREEN" if on else "YELLOW"
        sc += 2 if on else 1
        
        verd = "Tóxica"
        if s_prof == "RED" or s_debt == "RED" or s_ipo == "RED": verd = "Mamute Vermelho"
        elif sc >= 8: verd = "Mamute Azul"
        else: verd = "Mamute Amarelo"
        
        res["score"] = sc; res["verdict"] = verd
        res["criteria"] = { "profit": {"status": s_prof, "years": yrs}, "governance": {"status": s_gov, "type": "ON" if on else "PN"}, "debt": {"status": s_debt, "current_ratio": round(dr,2) if dr else 0}, "ipo": {"status": s_ipo, "years": round(ipo,1)} }
        return res
    except: return res

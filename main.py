import certifi
import requests
import yfinance as yf
import pandas as pd
import math
import hashlib
import base64
import json
from concurrent.futures import ThreadPoolExecutor
from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from sqlalchemy.orm import Session
from datetime import date, datetime, timedelta
from typing import List, Optional
import functools
import time

from database import engine, Base, get_db
from models import UserDB, WalletDB, TransactionDB, LogoDB, WatchlistDB, WatchlistItemDB, SystemConfigDB
from schemas import UserAuth, WalletCreate, TransactionCreate, LogoCreate, WatchlistCreate, WatchlistItemCreate

try: requests.packages.urllib3.disable_warnings()
except: pass

Base.metadata.create_all(bind=engine)
app = FastAPI(title="Investidor12 API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def hash_password(password: str):
    return hashlib.sha256(password.encode() + b"investidor12_salt").hexdigest()

def safe_float(val):
    try:
        if val is None: return 0.0
        if isinstance(val, (pd.Series, pd.DataFrame)):
            if val.empty: return 0.0
            val = val.iloc[0]
        if hasattr(val, 'item'): val = val.item()
        return float(val) if not math.isnan(float(val)) else 0.0
    except: return 0.0

@functools.lru_cache(maxsize=256)
def _get_cached_price(ticker: str, cache_buster: int):
    try:
        h = yf.Ticker(ticker).history(period="5d")
        if not h.empty and len(h) >= 2:
            curr = safe_float(h['Close'].iloc[-1])
            prev = safe_float(h['Close'].iloc[-2])
            var = ((curr - prev) / prev) * 100 if prev > 0 else 0
            return ticker, curr, var
        elif not h.empty and len(h) == 1:
            return ticker, safe_float(h['Close'].iloc[0]), 0.0
    except: pass
    return ticker, 0.0, 0.0

def get_fast_price_var(ticker):
    cache_buster = int(time.time() / 600)
    return _get_cached_price(ticker, cache_buster)

def fetch_prices_sync(tickers):
    res = {}
    with ThreadPoolExecutor(max_workers=5) as executor:
        for tick, p, _ in executor.map(get_fast_price_var, tickers):
            res[tick] = p
    return res

# =====================================================================
# NOVO MOTOR HÍBRIDO DE DIVIDENDOS (API NOVA + YFINANCE)
# =====================================================================
def fetch_dividends_hybrid(ticker: str):
    full_history_list = []
    hoje = datetime.now()
    start_date = hoje - timedelta(days=365 * 2) # Últimos 2 anos
    
    # 1. TENTA A API NOVA (Com Data de Pagamento Real)
    try:
        # A API pode precisar do ticker limpo ou com .SA, tentamos o formato limpo primeiro
        clean_ticker = ticker.replace('.SA', '')
        url = f"https://api.massive.com/stocks/v1/dividends?ticker={clean_ticker}&limit=100&apiKey=Z2ypGUiDHCz4iiykpfLd8bvZ0VppJ3uO"
        res = requests.get(url, timeout=4)
        
        if res.status_code == 200:
            data = res.json()
            # Descobre onde os dados estão no JSON (results, data ou na raiz)
            items = data.get('results', data.get('data', data if isinstance(data, list) else []))
            
            for item in items:
                ex_date_str = item.get('exDate', item.get('ex_date', item.get('date', '')))
                pay_date_str = item.get('paymentDate', item.get('pay_date', item.get('payment_date', ex_date_str)))
                amount = safe_float(item.get('amount', item.get('cash_amount', item.get('rate', 0))))

                if ex_date_str and amount > 0:
                    ex_date_obj = datetime.strptime(ex_date_str[:10], "%Y-%m-%d")
                    if ex_date_obj >= start_date:
                        pay_date_obj = datetime.strptime(pay_date_str[:10], "%Y-%m-%d") if pay_date_str else ex_date_obj
                        status = "A RECEBER" if pay_date_obj > hoje else "RECEBIDO"
                        
                        full_history_list.append({
                            "ex_date": ex_date_str[:10],
                            "pay_date": pay_date_str[:10],
                            "value": amount,
                            "status": status
                        })
            
            # Se a API retornou dados com sucesso, ordenamos e devolvemos!
            if full_history_list:
                return sorted(full_history_list, key=lambda x: x['pay_date'], reverse=True)
    except Exception as e:
        print(f"Aviso: API Nova falhou para {ticker}. Usando YFinance. Erro: {e}")

    # 2. FALLBACK PARA O YFINANCE (Garante que nunca quebra)
    try:
        divs = yf.Ticker(ticker).dividends
        if not divs.empty:
            if divs.index.tz is not None: divs.index = divs.index.tz_localize(None)
            for d, val in divs.items():
                if d >= start_date:
                    d_str = d.strftime("%Y-%m-%d")
                    status = "A RECEBER" if d > hoje else "RECEBIDO"
                    full_history_list.append({
                        "ex_date": d_str,  # YFinance só tem ex-date, então duplicamos
                        "pay_date": d_str,
                        "value": safe_float(val),
                        "status": status
                    })
    except: pass
    
    return sorted(full_history_list, key=lambda x: x['pay_date'], reverse=True)
# =====================================================================


@app.on_event("startup")
def seed_database():
    db = next(get_db())
    if not db.query(SystemConfigDB).filter(SystemConfigDB.key_name == "mamutes").first():
        mamutes = ['VALE3.SA','PETR4.SA','PETR3.SA','ITUB4.SA','BBDC4.SA','BBDC3.SA','BBAS3.SA','ABEV3.SA','WEGE3.SA','EGIE3.SA']
        db.add(SystemConfigDB(key_name="mamutes", config_data=json.dumps(mamutes)))
        
    if not db.query(SystemConfigDB).filter(SystemConfigDB.key_name == "market_indices").first():
        indices = {
            "IBOV": [f"{t}.SA" for t in ["VALE3", "PETR4", "ITUB4", "BBDC4", "B3SA3", "BBAS3", "ITSA4", "ABEV3", "PRIO3", "RENT3", "GGBR4", "WEGE3", "MGLU3", "HAPV3", "JBSS3", "SUZB3", "BPAC11", "LREN3", "ELET3", "RDOR3", "EQTL3", "RAIL3", "CSNA3", "VIVT3", "RADL3", "ASAI3", "UGPA3", "GOAU4", "BBSE3", "CMIG4", "COGN3", "CPFE3", "CYRE3", "EGIE3", "ENGI11", "GMAT3", "SBSP3", "TOTS3", "USIM5", "VBBR3", "KLBN11", "TIMS3", "BRAP4", "MULT3"]],
            "IFIX": [f"{t}.SA" for t in ["MXRF11", "XPML11", "KNCR11", "HGLG11", "BTLG11", "VISC11", "CPTS11", "HGBS11", "TGAR11", "IRDM11", "XPLG11", "KNIP11", "RBRR11", "JSRE11", "PVBI11", "BRCO11", "GARE11", "TRXF11", "LVBI11", "VGIR11", "KNSC11", "RECR11", "HGRU11", "VINO11", "MCCI11", "ALZR11", "VRTA11", "GGRC11", "RBRP11", "OUJP11", "HGPO11", "HGRE11", "RZTR11", "BTRA11", "RZAK11", "RBRL11", "PATL11", "KNHY11", "RBVA11", "BTAL11", "HABT11", "BROF11", "KFOF11", "BPML11", "VILG11"]],
            "SMLL": [f"{t}.SA" for t in ["INBR32", "VIVA3", "POMO4", "CURY3", "CEAB3", "LWSA3", "CVCB3", "SMFT3", "JHSF3", "MOVI3", "ODPV3", "SIMH3", "AMBP3", "MYPK3", "MRVE3", "ALPA4", "TEND3", "DIRR3", "SLCE3", "KEPL3", "UNIP6", "FRAS3", "POSI3", "SHUL4", "VLID3", "ROMI3", "PCAR3", "SQIA3", "LJQQ3", "ORVR3", "INTB3", "GGPS3", "ABCB4", "CAML3", "SBFG3", "PGMN3", "CMIN3", "RAPT4", "MILS3", "TTEN3", "AURA33", "FIQE3", "PLPL3", "CSED3", "VULC3"]],
            "NASDAQ": ["NVDA", "AAPL", "MSFT", "AMZN", "TSLA", "GOOGL", "META", "AMD", "AVGO", "COST", "NFLX", "ASML", "AZN", "ADI", "PEP", "CSCO", "TMUS", "TXN", "INTC", "AMGN", "QCOM", "ISRG", "MU", "HON", "BKNG", "VRTX", "SBUX", "ADP", "MDLZ", "REGN", "KLAC", "PANW", "LRCX", "MELI", "ADSK", "CSX", "MAR", "ORLY", "MNST", "SNPS", "CDNS", "KDP", "CHTR", "NXPI", "PAYX", "LULU", "MCHP"],
            "DOW JONES": ["MSFT", "AMZN", "AAPL", "NVDA", "UNH", "V", "JPM", "WMT", "JNJ", "PG", "HD", "CVX", "MRK", "CRM", "KO", "MCD", "DIS", "CSCO", "TRV", "GS", "AXP", "IBM", "CAT", "HON", "VZ", "AMGN", "BA", "MMM", "NKE", "SHW", "BAC", "XOM", "TSM", "ORCL", "T", "F", "PFE", "NU", "BABA", "UBER", "NIO", "SHOP", "PLTR", "SNAP", "COIN", "DKNG", "HOOD", "SOFI", "U"],
            "RUSSELL 2000": ["IWM", "MARA", "SOFI", "PLUG", "RKLB", "CLSK", "RIOT", "RUN", "FUBO", "DKNG", "LCID", "OPEN", "AFRM", "HOOD", "CHPT", "AI", "PATH", "UPST", "GME", "AMC", "SPCE", "BE", "QS", "JOBY", "ACHR", "MSTR", "COIN", "BYND", "CVNA", "DASH", "RIVN", "PDD", "LI", "XPEV", "ZM", "DOCU", "ROKU", "PINS", "SE", "BILL", "NET", "SNOW", "MDB", "CRWD", "ZS", "DDOG", "OKTA", "TEAM"],
            "Cripto": [f"{t}-USD" for t in ["BTC", "ETH", "USDT", "SOL", "USDC", "XRP", "BNB", "DOGE", "SHIB", "ADA", "AVAX", "LINK", "TRX", "DOT", "NEAR", "LTC", "BCH", "OP", "ARB", "LDO", "FET", "TIA", "SEI", "INJ", "KAS", "ICP", "HBAR", "VET", "FIL", "ATOM", "MKR", "AAVE", "THETA", "ALGO", "FLOW", "EOS", "MANA", "SAND", "AXS", "JUP"]]
        }
        db.add(SystemConfigDB(key_name="market_indices", config_data=json.dumps(indices)))
    db.commit()

@app.get("/")
def read_root(): return {"status": "API Investidor12 Online"}

@app.get("/watchlists/")
def get_watchlists(user_id: int, db: Session = Depends(get_db)):
    watchlists = db.query(WatchlistDB).filter(WatchlistDB.user_id == user_id).all()
    result = []
    
    all_tickers = list(set([item.ticker for w in watchlists for item in w.items]))
    prices = {}
    with ThreadPoolExecutor(max_workers=10) as executor:
        for ticker, price, var in executor.map(get_fast_price_var, all_tickers):
            prices[ticker] = {"price": price, "variation": var}

    for w in watchlists:
        w_items = []
        for item in w.items:
            p_data = prices.get(item.ticker, {"price": 0, "variation": 0})
            w_items.append({"id": item.id, "ticker": item.ticker, "price": p_data["price"], "variation": p_data["variation"]})
        w_items.sort(key=lambda x: x["variation"], reverse=True)
        result.append({"id": w.id, "name": w.name, "items": w_items})
    return result

@app.post("/watchlists/")
def create_watchlist(wl: WatchlistCreate, db: Session = Depends(get_db)):
    db.add(WatchlistDB(user_id=wl.user_id, name=wl.name)); db.commit(); return {"message": "Criada"}

@app.delete("/watchlists/{id}")
def delete_watchlist(id: int, db: Session = Depends(get_db)):
    db.query(WatchlistDB).filter(WatchlistDB.id == id).delete(); db.commit(); return {"message": "Excluída"}

@app.post("/watchlists/{id}/items")
def add_watchlist_item(id: int, item: WatchlistItemCreate, db: Session = Depends(get_db)):
    clean_ticker = item.ticker.upper().strip()
    if not clean_ticker.endswith('.SA') and not '-' in clean_ticker:
        if any(char.isdigit() for char in clean_ticker): clean_ticker += '.SA'

    existing = db.query(WatchlistItemDB).filter(WatchlistItemDB.watchlist_id == id, WatchlistItemDB.ticker == clean_ticker).first()
    if not existing:
        db.add(WatchlistItemDB(watchlist_id=id, ticker=clean_ticker)); db.commit()
    return {"message": "Adicionado"}

@app.delete("/watchlists/items/{item_id}")
def delete_watchlist_item(item_id: int, db: Session = Depends(get_db)):
    db.query(WatchlistItemDB).filter(WatchlistItemDB.id == item_id).delete(); db.commit(); return {"message": "Removido"}

@app.get("/market-overview")
def market_overview(db: Session = Depends(get_db)):
    try:
        conf = db.query(SystemConfigDB).filter(SystemConfigDB.key_name == "market_indices").first()
        baskets = json.loads(conf.config_data) if conf else {}
        
        all_tickers = []
        for t_list in baskets.values(): all_tickers.extend(t_list)
        all_tickers = list(set(all_tickers))
            
        prices = {}
        with ThreadPoolExecutor(max_workers=20) as executor:
            for ticker, price, var in executor.map(get_fast_price_var, all_tickers):
                prices[ticker] = {"price": price, "variation": var}

        results = {}
        for category, tickers in baskets.items():
            cat_data = []
            prefix = "US$ " if category in ["NASDAQ", "DOW JONES", "RUSSELL 2000", "Cripto"] else "R$ "
            for t in tickers:
                if t in prices and prices[t]["price"] > 0:
                    cat_data.append({"ticker": t.replace('.SA', '').replace('-USD', ''), "price": prices[t]["price"], "variation": prices[t]["variation"], "currency": prefix})
            
            cat_data.sort(key=lambda x: x['variation'], reverse=True)
            gainers = cat_data[:5]
            losers = sorted(cat_data[-5:], key=lambda x: x['variation']) if len(cat_data) >= 5 else []
            results[category] = {"gainers": gainers, "losers": losers}
            
        return results
    except Exception as e:
        print(f"Erro Overview: {e}")
        return {}

# === EXPLORAR ATIVOS COM O NOVO MOTOR DE DIVIDENDOS ===
@app.get("/asset-details/{ticker}")
def get_asset_details(ticker: str):
    ticker_upper = ticker.upper().strip()
    clean_ticker = ticker_upper.replace('.SA', '')
    
    if not ticker_upper.endswith('.SA') and not '-' in ticker_upper:
        if any(char.isdigit() for char in ticker_upper): ticker_upper += '.SA'

    try:
        t = yf.Ticker(ticker_upper)
        
        hist = t.history(period="1y")
        if hist.empty: raise HTTPException(status_code=404, detail="Ativo não encontrado ou sem dados na B3.")

        chart_data = [{"date": dt.strftime("%d/%m/%y"), "price": safe_float(row['Close'])} for dt, row in hist.iterrows()]
        current_price = chart_data[-1]["price"] if chart_data else 0

        info = {}
        try: 
            info = t.info
            if not info or len(info) < 5: raise Exception("Info Vazio")
        except: 
            try:
                fi = t.fast_info
                info = {
                    "shortName": clean_ticker,
                    "previousClose": safe_float(fi.get("previous_close", 0)),
                    "fiftyTwoWeekHigh": safe_float(fi.get("year_high", 0)),
                    "fiftyTwoWeekLow": safe_float(fi.get("year_low", 0)),
                    "regularMarketPrice": safe_float(fi.get("last_price", 0))
                }
            except: pass

        if current_price == 0: current_price = info.get('currentPrice', info.get('regularMarketPrice', 0))

        # USA O MOTOR HÍBRIDO!
        full_history_list = fetch_dividends_hybrid(ticker_upper)

        div_chart_map = {}
        start_date = datetime.now() - timedelta(days=365)
        for d in full_history_list:
            # O motor híbrido devolve pay_date. É o que usamos no gráfico.
            d_obj = datetime.strptime(d['pay_date'], "%Y-%m-%d")
            if start_date <= d_obj <= datetime.now():
                y, m, _ = d['pay_date'].split('-'); ym = f"{m}/{y[2:]}"
                div_chart_map[ym] = div_chart_map.get(ym, 0) + d['value']
            
        div_chart = [{"mes": k, "valor": safe_float(v)} for k, v in list(div_chart_map.items())[::-1]]

        dy = safe_float(info.get("dividendYield", info.get("trailingAnnualDividendYield", 0))) * 100
        if dy == 0 and current_price > 0 and full_history_list:
            soma_12m = sum(d['value'] for d in full_history_list if datetime.strptime(d['pay_date'], "%Y-%m-%d") >= start_date and d['status'] == 'RECEBIDO')
            dy = (soma_12m / current_price) * 100

        pvp = safe_float(info.get("priceToBook", 0))
        pl = safe_float(info.get("trailingPE", 0))
        vpa = safe_float(info.get("bookValue", 0))
        lpa = safe_float(info.get("trailingEps", 0))

        if pvp == 0 and vpa > 0 and current_price > 0: pvp = current_price / vpa
        if pl == 0 and lpa > 0 and current_price > 0: pl = current_price / lpa

        # Adapta o full_history_list para o frontend (que espera 'date')
        div_frontend = []
        for d in full_history_list:
            div_frontend.append({
                "date": d["pay_date"], # Mostra a data de pagamento
                "value": d["value"],
                "status": d["status"]
            })

        return {
            "ticker": clean_ticker,
            "name": info.get("shortName", info.get("longName", clean_ticker)),
            "sector": info.get("sector", info.get("industry", "N/A")),
            "price": current_price,
            "indicators": {
                "dy": dy,
                "pl": pl,
                "pvp": pvp,
                "lpa": lpa,
                "vpa": vpa,
                "high52w": safe_float(info.get("fiftyTwoWeekHigh", 0)),
            },
            "chart": chart_data,
            "dividends_chart": div_chart, 
            "full_dividend_history": div_frontend
        }
    except Exception as e: raise HTTPException(status_code=404, detail=str(e))

# === EARNINGS COM O MOTOR HÍBRIDO ===
@app.get("/earnings")
def get_earnings(wallet_id: int, db: Session = Depends(get_db)):
    default_res = {"total_acumulado": 0, "historico_mensal": [], "por_ativo": [], "por_classe": [], "detalhes": [], "provisionados": []}
    trans = db.query(TransactionDB).filter(TransactionDB.wallet_id == wallet_id).all()
    if not trans: return default_res

    try:
        holdings = {}
        for t in trans:
            if t.ticker not in holdings: holdings[t.ticker] = []
            holdings[t.ticker].append(t)

        # Busca dividendos hibridos de forma otimizada
        div_data = {}
        for t in list(holdings.keys()):
            h_divs = fetch_dividends_hybrid(t)
            if h_divs: div_data[t] = h_divs

        total_recebido = 0
        monthly = {}
        by_ticker = {}
        by_class = {}
        detalhes = []
        provisionados = []
        hoje = date.today()

        for ticker, t_list in holdings.items():
            if ticker not in div_data: continue
            
            asset_type = t_list[0].type
            tick_val = 0
            
            for item in div_data[ticker]:
                ex_date_obj = datetime.strptime(item["ex_date"], "%Y-%m-%d").date()
                pay_date_obj = datetime.strptime(item["pay_date"], "%Y-%m-%d").date()
                
                # O usuário precisa ter comprado a ação ANTES da data Ex!
                qty = sum(t.quantity for t in t_list if t.date < ex_date_obj)
                
                if qty > 0:
                    payment = qty * item["value"]
                    obj = {
                        "date": item["pay_date"], # O display pro usuário é a data que pinga na conta
                        "ticker": ticker.replace('.SA', ''), 
                        "type": asset_type, 
                        "val": safe_float(payment), 
                        "qtd": safe_float(qty), 
                        "unit_val": safe_float(item["value"])
                    }
                    
                    if item["status"] == "A RECEBER" or pay_date_obj > hoje:
                        provisionados.append(obj)
                    else:
                        tick_val += payment
                        total_recebido += payment
                        detalhes.append(obj)
                        m = pay_date_obj.strftime("%Y-%m")
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
    except Exception as e: 
        print(f"Erro no Earnings: {e}")
        return default_res

# === DASHBOARD MANTIDO IGUAL ===
@app.get("/dashboard")
def get_dashboard(wallet_id: int, db: Session = Depends(get_db)):
    default_res = {"patrimonio_atual":0,"total_investido":0,"lucro":0,"rentabilidade_pct":0,"daily_variation":0,"grafico":[],"ativos":[]}
    trans = db.query(TransactionDB).filter(TransactionDB.wallet_id == wallet_id).all()
    if not trans: return default_res

    try:
        tickers = list(set(t.ticker for t in trans))
        
        precos_atuais = {}
        with ThreadPoolExecutor(max_workers=10) as ex:
            for tick, p, _ in ex.map(get_fast_price_var, tickers): precos_atuais[tick] = p

        try:
            data_inicio_real = min(t.date for t in trans)
            lista_download = tickers + ['^BVSP']
            dados_historicos = yf.download(lista_download, start=data_inicio_real, progress=False, threads=False)['Close']
            if not dados_historicos.empty: dados_historicos = dados_historicos.ffill().bfill()
        except: dados_historicos = pd.DataFrame()

        patrimonio_total = investido_total = daily_var_money = patrimonio_ontem = 0
        ativos_finais = []
        trans_map = {}
        for t in trans:
            if t.ticker not in trans_map: trans_map[t.ticker] = {"qtd":0, "custo":0, "type":t.type}
            trans_map[t.ticker]["qtd"] += t.quantity; trans_map[t.ticker]["custo"] += (t.quantity * t.price)

        is_weekend = date.today().weekday() >= 5

        for tick, d in trans_map.items():
            if d["qtd"] > 0:
                pm = d["custo"] / d["qtd"]
                p_atual = precos_atuais.get(tick, pm)
                
                tot = d["qtd"] * p_atual
                lucro_ativo = tot - d["custo"]
                rent = ((p_atual - pm) / pm) * 100 if pm > 0 else 0
                
                var_pct = 0.0
                if not is_weekend and not dados_historicos.empty:
                    try:
                        if isinstance(dados_historicos, pd.DataFrame) and tick in dados_historicos.columns:
                            s = dados_historicos[tick].dropna()
                            if len(s) >= 2: var_pct = ((safe_float(s.iloc[-1]) - safe_float(s.iloc[-2])) / safe_float(s.iloc[-2])) * 100
                        elif isinstance(dados_historicos, pd.Series):
                            s = dados_historicos.dropna()
                            if len(s) >= 2: var_pct = ((safe_float(s.iloc[-1]) - safe_float(s.iloc[-2])) / safe_float(s.iloc[-2])) * 100
                    except: pass
                
                money_var = tot - (tot / (1 + var_pct/100)) if var_pct != 0 else 0
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
                        row = hist_slice.loc[ts]
                        if isinstance(hist_slice, pd.DataFrame):
                            for t, q in posicao.items():
                                if q > 0 and t in row: p = safe_float(row[t]); val_mercado += q * p if p > 0 else 0
                        elif isinstance(hist_slice, pd.Series):
                            p = safe_float(row)
                            for t, q in posicao.items():
                                if q > 0 and p > 0: val_mercado += q * p
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
    except Exception as e:
        print("Erro Dashboard:", e)
        return default_res

@app.get("/analyze/{ticker}")
def analyze(ticker: str):
    ticker_upper = ticker.upper().strip()
    clean = ticker_upper.replace('.SA', '')
    if not ticker_upper.endswith('.SA') and not '-' in ticker_upper:
        if any(char.isdigit() for char in ticker_upper): ticker_upper += '.SA'
    
    res = { "ticker": clean, "price": 0, "score": 0, "verdict": "Indisponível", "criteria": { "profit": {"status": "GRAY"}, "governance": {"status": "GRAY"}, "debt": {"status": "GRAY"}, "ipo": {"status": "GRAY"} } }
    try:
        t = yf.Ticker(ticker_upper)
        try: i = t.info
        except: return res
        if not i: return res
        res["price"] = i.get('currentPrice', 0)
        
        ipo = 5 
        s_ipo = "YELLOW"
        sc = 1
        
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

@app.post("/auth/register")
def register(user: UserAuth, db: Session = Depends(get_db)):
    existing = db.query(UserDB).filter(UserDB.username == user.username.lower()).first()
    if existing: raise HTTPException(status_code=400, detail="Usuário já existe")
    new_user = UserDB(username=user.username.lower(), password=hash_password(user.password)); db.add(new_user); db.commit(); db.refresh(new_user)
    db.add(WalletDB(name="Carteira Principal", user_id=new_user.id)); db.commit()
    return {"id": new_user.id, "username": new_user.username}

@app.post("/auth/login")
def login(user: UserAuth, db: Session = Depends(get_db)):
    db_user = db.query(UserDB).filter(UserDB.username == user.username.lower(), UserDB.password == hash_password(user.password)).first()
    if not db_user: raise HTTPException(status_code=400, detail="Credenciais inválidas")
    return {"id": db_user.id, "username": db_user.username}

@app.post("/wallets/")
def create_wallet(wallet: WalletCreate, db: Session = Depends(get_db)):
    db_wallet = WalletDB(name=wallet.name, description=wallet.description, user_id=wallet.user_id)
    db.add(db_wallet)
    db.commit()
    db.refresh(db_wallet)
    return db_wallet

@app.get("/wallets/")
def list_wallets(user_id: int, db: Session = Depends(get_db)):
    wallets = db.query(WalletDB).filter(WalletDB.user_id == user_id).all()
    if not wallets:
        def_wallet = WalletDB(name="Carteira Principal", user_id=user_id)
        db.add(def_wallet)
        db.commit()
        db.refresh(def_wallet)
        return [def_wallet]
    return wallets

@app.delete("/wallets/{id}")
def delete_wallet(id: int, db: Session = Depends(get_db)):
    db.query(TransactionDB).filter(TransactionDB.wallet_id == id).delete()
    db.query(WalletDB).filter(WalletDB.id == id).delete()
    db.commit()
    return {"message": "OK"}

@app.post("/transactions/")
def create_transaction(trans: TransactionCreate, db: Session = Depends(get_db)):
    db.add(TransactionDB(wallet_id=trans.wallet_id, ticker=trans.ticker.upper(), date=trans.date, quantity=trans.quantity, price=trans.price, type=trans.type))
    db.commit()
    return {"message": "OK"}

@app.post("/transactions/bulk")
def create_bulk(transactions: List[TransactionCreate], db: Session = Depends(get_db)):
    for t in transactions: db.add(TransactionDB(wallet_id=t.wallet_id, ticker=t.ticker.upper(), date=t.date, quantity=t.quantity, price=t.price, type=t.type))
    db.commit()
    return {"message": "OK"}

@app.delete("/transactions/clear_all")
def clear_all(wallet_id: int, db: Session = Depends(get_db)):
    db.query(TransactionDB).filter(TransactionDB.wallet_id == wallet_id).delete()
    db.commit()
    return {"message": "OK"}

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
        t.wallet_id = trans.wallet_id; t.ticker = trans.ticker.upper(); t.date = trans.date; t.quantity = trans.quantity; t.price = trans.price; t.type = trans.type
        db.commit()
    return {"message": "OK"}

@app.get("/get-price")
def price_check(ticker: str, date: str):
    try:
        t = ticker.upper()
        if "-" not in t and not any(c.isdigit() for c in t): t += ".SA"
        _, p, _ = get_fast_price_var(t)
        return {"price": round(p, 2)}
    except: return {"price": 0.0}

@app.post("/logos/")
def upload_logo(logo: LogoCreate, db: Session = Depends(get_db)):
    t = logo.ticker.upper().strip().replace('.SA', '')
    db_logo = db.query(LogoDB).filter(LogoDB.ticker == t).first()
    if db_logo: db_logo.image_base64 = logo.image_base64
    else: db.add(LogoDB(ticker=t, image_base64=logo.image_base64))
    db.commit(); return {"message": "Salva"}

@app.get("/logos/{ticker}")
def get_logo(ticker: str, db: Session = Depends(get_db)):
    db_logo = db.query(LogoDB).filter(LogoDB.ticker == ticker.upper().strip().replace('.SA', '')).first()
    if not db_logo or not db_logo.image_base64: raise HTTPException(status_code=404)
    header, encoded = db_logo.image_base64.split(",", 1)
    return Response(content=base64.b64decode(encoded), media_type=f"image/{header.split(';')[0].split('/')[1]}")
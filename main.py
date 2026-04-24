import requests
import pandas as pd
import numpy as np
import pyotp
from datetime import datetime, timedelta
from SmartApi import SmartConnect
import warnings
warnings.filterwarnings("ignore")


API_KEY      = "your_api_key_here"
CLIENT_ID    = "your_client_id_here"
PASSWORD     = "your_password_here"
TOTP_SECRET  = "your_totp_secret_here"

NSE_STOCKS = [
    "RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK",
    "SBIN", "WIPRO", "AXISBANK", "KOTAKBANK", "LT",
    "BAJFINANCE", "HINDUNILVR", "ASIANPAINT", "MARUTI",
    "SUNPHARMA", "TATAMOTORS", "TATASTEEL", "NTPC",
    "POWERGRID", "ONGC", "COALINDIA", "ADANIENT",
    "ADANIPORTS", "ULTRACEMCO", "JSWSTEEL", "GRASIM",
    "TECHM", "HCLTECH", "BHARTIARTL", "INDUSINDBK",
    "DRREDDY", "CIPLA", "DIVISLAB", "EICHERMOT",
    "HEROMOTOCO", "BAJAJ-AUTO", "TITAN", "NESTLEIND",
    "BRITANNIA", "ITC"
]

LOOKBACK        = 100
SMA_PERIOD      = 200
DISCOUNT_FACTOR = 0.95
VOLUME_MULT     = 3.0
SPREAD_FACTOR   = 0.5
CLOSE_POS_MIN   = 0.5
RR_RATIOS       = [1.5, 2.0, 3.0]
TRADE_VALUE_INR = 50000
SLIPPAGE_PCT    = 0.001


def calculate_charges(buy_price, sell_price, quantity):
    buy_turnover   = buy_price  * quantity
    sell_turnover  = sell_price * quantity
    total_turnover = buy_turnover + sell_turnover

    stt              = total_turnover * 0.001
    exchange_charges = total_turnover * 0.0000335
    brokerage        = 0
    gst              = (brokerage + exchange_charges) * 0.18
    sebi             = total_turnover * 0.000001
    stamp            = buy_turnover * 0.00015

    return round(stt + exchange_charges + gst + sebi + stamp, 4)


def apply_slippage(entry_price, exit_price):
    real_entry = entry_price * (1 + SLIPPAGE_PCT)
    real_exit  = exit_price  * (1 - SLIPPAGE_PCT)
    return real_entry, real_exit


def net_return(entry, exit_price, trade_value=TRADE_VALUE_INR):
    real_entry, real_exit = apply_slippage(entry, exit_price)

    quantity = int(trade_value / real_entry)
    if quantity == 0:
        return 0.0

    gross_pnl   = (real_exit - real_entry) * quantity
    charges     = calculate_charges(real_entry, real_exit, quantity)
    net_pnl     = gross_pnl - charges
    net_ret_pct = (net_pnl / (real_entry * quantity)) * 100

    return round(net_ret_pct, 3)


def login():
    totp = pyotp.TOTP(TOTP_SECRET).now()
    obj  = SmartConnect(api_key=API_KEY)
    data = obj.generateSession(CLIENT_ID, PASSWORD, totp)
    if not data["status"]:
        raise Exception(f"Login failed: {data['message']}")
    print(f"  ✅ Logged in as {CLIENT_ID}")
    return obj


def get_symbol_tokens():
    url  = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
    resp = requests.get(url, timeout=30)
    data = resp.json()
    token_map = {}
    for item in data:
        if item.get("exch_seg") == "NSE" and item.get("symbol", "").endswith("-EQ"):
            name = item["symbol"].replace("-EQ", "")
            token_map[name] = item["token"]
    print(f"  ✅ Scrip master loaded — {len(token_map)} NSE EQ tokens")
    return token_map


def get_candles(obj, token, days=900):
    to_date   = datetime.today()
    from_date = to_date - timedelta(days=days)
    params = {
        "exchange":    "NSE",
        "symboltoken": token,
        "interval":    "ONE_DAY",
        "fromdate":    from_date.strftime("%Y-%m-%d %H:%M"),
        "todate":      to_date.strftime("%Y-%m-%d %H:%M"),
    }
    try:
        resp = obj.getCandleData(params)
        if not resp or not resp.get("status"):
            return None
        data = resp.get("data", [])
        if not data:
            return None
        df = pd.DataFrame(data, columns=["datetime", "open", "high", "low", "close", "volume"])
        df["datetime"] = pd.to_datetime(df["datetime"])
        df = df.set_index("datetime").sort_index()
        df.columns = ["Open", "High", "Low", "Close", "Volume"]
        return df.apply(pd.to_numeric)
    except:
        return None


def check_wyckoff(row, sma200, avg_vol, avg_spread):
    spread = row["High"] - row["Low"]
    if spread == 0:
        return False, {}

    close_pos = (row["Close"] - row["Low"]) / spread

    f1 = row["Close"] <= sma200 * DISCOUNT_FACTOR
    f2 = row["Volume"] > avg_vol * VOLUME_MULT
    f3 = spread <= avg_spread * SPREAD_FACTOR
    f4 = close_pos > CLOSE_POS_MIN

    details = {
        "price":      round(row["Close"], 2),
        "sma200":     round(sma200, 2),
        "candle_low": round(row["Low"], 2),
        "volume":     int(row["Volume"]),
        "avg_vol_3x": int(avg_vol * VOLUME_MULT),
        "spread":     round(spread, 2),
        "avg_spread": round(avg_spread * SPREAD_FACTOR, 2),
        "close_pos":  round(close_pos, 3),
    }
    return (f1 and f2 and f3 and f4), details


def backtest_stock(symbol, df):
    if len(df) < SMA_PERIOD + LOOKBACK + 30:
        return []

    df = df.copy()
    df["SMA200"]     = df["Close"].rolling(SMA_PERIOD).mean()
    df["Spread"]     = df["High"] - df["Low"]
    df["AvgVol100"]  = df["Volume"].rolling(LOOKBACK).mean()
    df["AvgSprd100"] = df["Spread"].rolling(LOOKBACK).mean()

    results = []

    for i in range(SMA_PERIOD + LOOKBACK, len(df) - 30):
        row = df.iloc[i]
        signal, details = check_wyckoff(
            row, df["SMA200"].iloc[i],
            df["AvgVol100"].iloc[i], df["AvgSprd100"].iloc[i]
        )
        if not signal:
            continue

        entry = row["Close"]
        sl    = row["Low"]
        risk  = entry - sl
        if risk <= 0:
            continue

        rr_results = {}

        for rr in RR_RATIOS:
            target     = entry + (risk * rr)
            hit_target = False
            hit_sl     = False
            days_held  = 0
            exit_price = None

            for j in range(i + 1, min(i + 31, len(df))):
                future    = df.iloc[j]
                days_held += 1

                if future["Low"] <= sl:
                    hit_sl     = True
                    exit_price = sl
                    break
                if future["High"] >= target:
                    hit_target = True
                    exit_price = target
                    break

            if exit_price is None:
                exit_price = df["Close"].iloc[min(i + 30, len(df) - 1)]

            outcome   = "WIN" if hit_target else ("LOSS" if hit_sl else "OPEN")
            gross_ret = round((exit_price - entry) / entry * 100, 3)
            net_ret   = net_return(entry, exit_price, TRADE_VALUE_INR)

            real_entry, real_exit = apply_slippage(entry, exit_price)
            qty     = int(TRADE_VALUE_INR / real_entry)
            charges = calculate_charges(real_entry, real_exit, qty) if qty > 0 else 0

            rr_results[f"rr{rr}_outcome"]    = outcome
            rr_results[f"rr{rr}_gross_ret"]  = gross_ret
            rr_results[f"rr{rr}_net_ret"]    = net_ret
            rr_results[f"rr{rr}_charges_rs"] = round(charges, 2)
            rr_results[f"rr{rr}_days"]       = days_held

        results.append({
            "symbol": symbol,
            "date":   df.index[i].strftime("%Y-%m-%d"),
            "entry":  entry,
            "sl":     round(sl, 2),
            "risk":   round(risk, 2),
            **details,
            **rr_results
        })

    return results


def live_scan(symbol, df):
    if len(df) < SMA_PERIOD + LOOKBACK:
        return None

    df = df.copy()
    df["SMA200"]     = df["Close"].rolling(SMA_PERIOD).mean()
    df["Spread"]     = df["High"] - df["Low"]
    df["AvgVol100"]  = df["Volume"].rolling(LOOKBACK).mean()
    df["AvgSprd100"] = df["Spread"].rolling(LOOKBACK).mean()

    row    = df.iloc[-1]
    signal, details = check_wyckoff(
        row, df["SMA200"].iloc[-1],
        df["AvgVol100"].iloc[-1], df["AvgSprd100"].iloc[-1]
    )
    if not signal:
        return None

    entry      = row["Close"]
    sl         = row["Low"]
    risk       = entry - sl
    real_entry = entry * (1 + SLIPPAGE_PCT)

    targets_info = {}
    for rr in RR_RATIOS:
        target    = entry + risk * rr
        real_exit = target * (1 - SLIPPAGE_PCT)
        qty       = int(TRADE_VALUE_INR / real_entry)
        charges   = calculate_charges(real_entry, real_exit, qty) if qty > 0 else 0
        net_ret   = net_return(entry, target, TRADE_VALUE_INR)
        targets_info[rr] = {
            "target":  round(target, 2),
            "net_ret": net_ret,
            "charges": round(charges, 2)
        }

    return {
        "symbol":       symbol,
        "date":         df.index[-1].strftime("%Y-%m-%d"),
        "entry":        entry,
        "real_entry":   round(real_entry, 2),
        "sl":           round(sl, 2),
        "risk_pct":     round(risk / entry * 100, 2),
        "targets_info": targets_info,
        **details
    }


def print_backtest_summary(bt_df):
    print(f"\n  Total signals (historical) : {len(bt_df)}")
    print(f"  Stocks with signals        : {bt_df['symbol'].nunique()}\n")
    print(f"  Trade size assumed         : ₹{TRADE_VALUE_INR:,}")
    print(f"  Slippage assumed           : {SLIPPAGE_PCT*100}% each side\n")

    print(f"  {'R:R':<6} {'Win%':<8} {'Loss%':<8} {'AvgGross':<12} {'AvgNET':<12} {'AvgCharges':<14} {'Expectancy(NET)'}")
    print("  " + "─" * 75)

    best_rr         = None
    best_expectancy = -999

    for rr in RR_RATIOS:
        col_o = f"rr{rr}_outcome"
        col_g = f"rr{rr}_gross_ret"
        col_n = f"rr{rr}_net_ret"
        col_c = f"rr{rr}_charges_rs"

        if col_o not in bt_df.columns:
            continue

        closed = bt_df[bt_df[col_o] != "OPEN"]
        if len(closed) == 0:
            continue

        wins      = closed[closed[col_o] == "WIN"]
        losses    = closed[closed[col_o] == "LOSS"]
        win_rate  = len(wins)   / len(closed)
        loss_rate = len(losses) / len(closed)

        avg_win_net  = wins[col_n].mean()   if len(wins)   else 0
        avg_loss_net = losses[col_n].mean() if len(losses) else 0
        avg_charges  = closed[col_c].mean()
        expectancy   = (win_rate * avg_win_net) + (loss_rate * avg_loss_net)

        print(f"  {rr:<6} {win_rate*100:<8.1f} {loss_rate*100:<8.1f} "
              f"{closed[col_g].mean():<12.2f} {closed[col_n].mean():<12.2f} "
              f"₹{avg_charges:<12.2f} {expectancy:.3f}%")

        if expectancy > best_expectancy:
            best_expectancy = expectancy
            best_rr         = rr

    if best_rr:
        print(f"\n  🏆 Best R:R → {best_rr}:1  |  Net Expectancy: {best_expectancy:.3f}% per trade")
        print(f"  💰 On ₹{TRADE_VALUE_INR:,} per trade = ₹{round(best_expectancy/100*TRADE_VALUE_INR,2)} avg net profit per signal")


def main():
    print("╔══════════════════════════════════════════════════════╗")
    print("║   WYCKOFF SCANNER v3 — ANGEL ONE + CHARGES + SLIP   ║")
    print(f"║   {datetime.now().strftime('%Y-%m-%d %H:%M')}                                  ║")
    print("╚══════════════════════════════════════════════════════╝\n")

    print("  🔐 Logging in...")
    obj = login()

    print("  📋 Loading scrip master...")
    token_map = get_symbol_tokens()

    all_backtest = []
    live_signals = []
    failed       = []

    for symbol in NSE_STOCKS:
        print(f"  ⟳ {symbol:<20}", end="\r")

        token = token_map.get(symbol)
        if not token:
            failed.append(symbol)
            continue

        df = get_candles(obj, token, days=900)
        if df is None or len(df) < SMA_PERIOD + LOOKBACK + 30:
            failed.append(symbol)
            continue

        bt = backtest_stock(symbol, df)
        all_backtest.extend(bt)

        live = live_scan(symbol, df)
        if live:
            live_signals.append(live)

    print(" " * 40)

    print("\n" + "═" * 75)
    print("  📊  BACKTEST — GROSS vs NET (after charges + slippage)")
    print("═" * 75)

    if all_backtest:
        bt_df = pd.DataFrame(all_backtest)
        print_backtest_summary(bt_df)
        bt_df.to_csv("/mnt/user-data/outputs/wyckoff_v3_backtest.csv", index=False)
        print(f"\n  ✅ Saved → wyckoff_v3_backtest.csv")
    else:
        print("  No historical signals found.")

    print("\n" + "═" * 75)
    print("  🚨  LIVE SIGNALS — TODAY (net of charges + slippage)")
    print("═" * 75)

    if live_signals:
        for s in live_signals:
            print(f"""
  🟢 {s['symbol']}  [{s['date']}]
     Signal Price  : ₹{s['entry']}
     Real Entry    : ₹{s['real_entry']}  (after {SLIPPAGE_PCT*100}% slippage)
     Stop Loss     : ₹{s['sl']}  ({s['risk_pct']}% risk)
     ─────────────────────────────────────────────────""")
            for rr, info in s["targets_info"].items():
                print(f"     Target {rr}x    : ₹{info['target']}  "
                      f"| Net Return: {info['net_ret']}%  "
                      f"| Charges: ₹{info['charges']}")
            print(f"""     ─────────────────────────────────────────────────
     Volume        : {s['volume']:,}  (need > {s['avg_vol_3x']:,})
     Spread        : {s['spread']}  (need < {s['avg_spread']})
     Close Pos     : {s['close_pos']}  (need > 0.5)
     SMA200        : ₹{s['sma200']}
""")
    else:
        print("\n  No live signals today 🧘")

    if failed:
        print(f"\n  ⚠️  Skipped: {', '.join(failed)}")

    print("═" * 75)
    print("  Done. All returns shown NET of charges + slippage. 🏆")
    print("═" * 75 + "\n")


if __name__ == "__main__":
    main()
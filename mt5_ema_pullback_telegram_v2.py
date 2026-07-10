"""
EMA Pullback Strong Candle Strategy - MT5 Automated EA (V2)
============================================================
Chạy trực tiếp trên VPS Windows đã cài MetaTrader5 (Exness), không cần TradingView.
Đọc nến H4/D1, tính toán lại logic y hệt bản Pine Script gốc, bắn tín hiệu Long/Short
kèm ảnh chart vào Telegram, và tự động đặt lệnh + đóng lệnh theo EMA Stop trên MT5.

YÊU CẦU CÀI ĐẶT (trên VPS Windows):
    1. Cài MetaTrader5 terminal, đăng nhập tài khoản Exness, để terminal chạy nền.
    2. pip install MetaTrader5 pandas numpy requests mplfinance matplotlib

CẤU HÌNH: sửa phần CONFIG bên dưới trước khi chạy.
"""

import time
import json
import os
import re
import uuid
import threading
import logging
from io import BytesIO
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
import requests
import MetaTrader5 as mt5
import matplotlib
matplotlib.use("Agg")   # không cần hiển thị màn hình, chỉ xuất ảnh PNG
import mplfinance as mpf

# ============================== CONFIG ==============================

# ---- Đặt lệnh tự động (EA) ----
AUTO_TRADE = True              # True -> Tự động đặt lệnh trên MT5; False -> Chỉ gửi nút bấm vào Telegram

# ---- Config từng timeframe với các cặp cụ thể ----
# Key là timeframe muốn chạy, value là danh sách symbol chạy trên timeframe đó.
# Tên symbol phải đúng với tên trên MT5/Exness (ví dụ: BTCUSDm, EURUSDm, XAUUSDm...)
# Thêm/xóa cặp ở từng timeframe tùy ý. Một symbol có thể xuất hiện ở nhiều timeframe.
TIMEFRAME_SYMBOL_MAP = {
    "M15": ["BTCUSD"],
    "H4":  ["BTCUSD"],
    "D1":  ["BTCUSD", "EURUSD", "GBPUSD"],
}

# ---- Tất cả khung thời gian hỗ trợ (không cần sửa) ----
TIMEFRAMES_ALL = {
    "M15": mt5.TIMEFRAME_M15,
    # "H1":  mt5.TIMEFRAME_H1,
    "H4":  mt5.TIMEFRAME_H4,
    "D1":  mt5.TIMEFRAME_D1,
}

# Magic number riêng cho từng khung để chạy độc lập
MAGIC_NUMBERS = {
    "M15": 20260715,
    # "H1":  20260701,
    "H4":  20260704,
    "D1":  20260724,
}

BARS_TO_FETCH = 500          # số nến lịch sử load mỗi lần để tính lại indicator/state
# Tần suất quét: M15 cần check thường xuyên hơn (60s), H4/D1 chỉ cần 5 phút là đủ
POLL_SECONDS = 60            # chu kỳ kiểm tra nến mới đóng (giây)
MAX_WORKERS = 8              # số luồng xử lý song song (giúp quét nhiều symbol nhanh hơn)

def load_env_file():
    """Tự động đọc các biến cấu hình từ file .env nếu có, load vào os.environ."""
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(env_path):
        try:
            with open(env_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    if "=" in line:
                        k, v = line.split("=", 1)
                        k = k.strip()
                        v = v.strip().strip("'\"")
                        os.environ[k] = v
        except Exception as e:
            print(f"Lỗi đọc file .env: {e}")

load_env_file()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "PASTE_YOUR_BOT_TOKEN_HERE")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "PASTE_YOUR_CHAT_ID_HERE")

# ---- Chart + nút Vào Lệnh ----
ENABLE_CHART_SCREENSHOT = True   # gửi kèm ảnh chart cùng tín hiệu
ENABLE_TRADE_BUTTON = True       # hiện nút "Vào Lệnh" dưới ảnh (chỉ hiển thị khi AUTO_TRADE = False)
CHART_LOOKBACK_BARS = 100        # số nến hiển thị trong ảnh chart

ORDER_DEVIATION = 20              # độ trượt giá cho phép (points) khi đặt lệnh
SIGNAL_TTL_SECONDS = 3600 * 12    # tín hiệu (nút bấm) hết hạn sau bao lâu nếu không bấm

# ---- Tham số chiến lược (giống input trong Pine Script) ----
EMA_FAST_LEN = 20
EMA_SLOW_LEN = 50
EMA_STOP_LEN = 10
PIVOT_BARS = 3
RISK_PCT = 5.0                   # 5% rủi ro mỗi lệnh
SL_LOOKBACK_BARS = 5
ATR_LEN = 14
STRONG_MULT = 1.3
STRONG_LOOKBACK = 2
NEAR_ATR_MULT = 0.3
USE_EMA_STOP = True

STATE_FILE = "signal_state.json"   # lưu trạng thái để không gửi trùng tín hiệu khi restart script

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def get_symbols_to_watch():
    """Trả về dict {symbol_name: [danh sách tên khung áp dụng]}
    dựa trên TIMEFRAME_SYMBOL_MAP, chỉ giữ lại các khung có trong TIMEFRAMES_ALL."""
    result = {}
    for tf_name, symbols in TIMEFRAME_SYMBOL_MAP.items():
        if tf_name not in TIMEFRAMES_ALL:
            log.warning(f"{tf_name}: timeframe không được hỗ trợ trong TIMEFRAMES_ALL, bỏ qua.")
            continue

        seen = set()
        for symbol in symbols:
            symbol = str(symbol).strip()
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            result.setdefault(symbol, []).append(tf_name)

    for symbol, tf_names in result.items():
        log.info(f"{symbol}: chạy các khung {', '.join(tf_names)}")

    if not result:
        log.error("TIMEFRAME_SYMBOL_MAP không có tổ hợp symbol/timeframe hợp lệ.")
    return result


def get_first_configured_symbol_and_timeframe(default_symbol="EURUSDm", default_tf="H4"):
    """Lấy symbol/timeframe đầu tiên trong cấu hình để dùng cho lệnh /test."""
    for tf_name, symbols in TIMEFRAME_SYMBOL_MAP.items():
        if tf_name not in TIMEFRAMES_ALL:
            continue
        for symbol in symbols:
            symbol = str(symbol).strip()
            if symbol:
                return symbol, tf_name
    return default_symbol, default_tf


# ============================== PENDING SIGNALS (cho nút Vào Lệnh) ==============================

pending_signals = {}
pending_signals_lock = threading.Lock()
mt5_lock = threading.Lock()
PENDING_SIGNALS_FILE = "pending_signals.json"


def save_pending_signals():
    try:
        with open(PENDING_SIGNALS_FILE, "w", encoding="utf-8") as f:
            json.dump(pending_signals, f, indent=2)
    except Exception as e:
        log.error(f"Lỗi lưu pending_signals: {e}")


def load_pending_signals():
    global pending_signals
    if os.path.exists(PENDING_SIGNALS_FILE):
        try:
            with open(PENDING_SIGNALS_FILE, "r", encoding="utf-8") as f:
                pending_signals = json.load(f)
            log.info(f"Đã tải {len(pending_signals)} tín hiệu chờ từ file.")
        except Exception as e:
            log.error(f"Lỗi tải pending_signals: {e}")
            pending_signals = {}


def register_pending_signal(symbol: str, tf_name: str, side: str, entry: float, sl: float) -> str:
    sig_id = uuid.uuid4().hex[:10]
    with pending_signals_lock:
        pending_signals[sig_id] = {
            "symbol": symbol, "tf_name": tf_name, "side": side,
            "entry": entry, "sl": sl, "created": time.time(),
        }
        save_pending_signals()
    return sig_id


def cleanup_expired_signals():
    now = time.time()
    with pending_signals_lock:
        expired = [k for k, v in pending_signals.items() if now - v["created"] > SIGNAL_TTL_SECONDS]
        if expired:
            for k in expired:
                pending_signals.pop(k, None)
            save_pending_signals()


# ============================== TELEGRAM ==============================

def send_telegram(message: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        r = requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": message}, timeout=10)
        if not r.ok:
            log.error(f"Telegram send failed: {r.text}")
    except Exception as e:
        log.error(f"Telegram send exception: {e}")


def send_telegram_photo(photo_bytes: bytes, caption: str, reply_markup: dict = None):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
    data = {"chat_id": TELEGRAM_CHAT_ID, "caption": caption}
    if reply_markup:
        data["reply_markup"] = json.dumps(reply_markup)
    files = {"photo": ("chart.png", photo_bytes, "image/png")}
    try:
        r = requests.post(url, data=data, files=files, timeout=20)
        if not r.ok:
            log.error(f"Telegram sendPhoto failed: {r.text}")
        return r.json() if r.ok else None
    except Exception as e:
        log.error(f"Telegram sendPhoto exception: {e}")
        return None


def answer_callback_query(callback_id: str, text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/answerCallbackQuery"
    try:
        requests.post(url, data={"callback_query_id": callback_id, "text": text}, timeout=10)
    except Exception as e:
        log.error(f"answerCallbackQuery lỗi: {e}")


def edit_message_content(chat_id, message_id, text: str, is_photo: bool):
    method = "editMessageCaption" if is_photo else "editMessageText"
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}"
    data = {
        "chat_id": chat_id,
        "message_id": message_id,
        "reply_markup": json.dumps({"inline_keyboard": []})
    }
    if is_photo:
        data["caption"] = text
    else:
        data["text"] = text

    try:
        r = requests.post(url, data=data, timeout=10)
        if not r.ok:
            log.error(f"{method} thất bại: {r.text}")
    except Exception as e:
        log.error(f"{method} lỗi: {e}")


def get_filling_type(symbol: str):
    with mt5_lock:
        info = mt5.symbol_info(symbol)
    if info is None:
        return 0 # Fallback: ORDER_FILLING_FOK
    
    filling_mode = info.filling_mode
    symbol_fok = getattr(mt5, "SYMBOL_FILLING_FOK", 1)
    symbol_ioc = getattr(mt5, "SYMBOL_FILLING_IOC", 2)
    
    order_fok = getattr(mt5, "ORDER_FILLING_FOK", 0)
    order_ioc = getattr(mt5, "ORDER_FILLING_IOC", 1)
    order_return = getattr(mt5, "ORDER_FILLING_RETURN", 2)
    
    if (filling_mode & symbol_fok) != 0:
        return order_fok
    elif (filling_mode & symbol_ioc) != 0:
        return order_ioc
    else:
        return order_return


# ============================== CHART ==============================

def generate_chart_image(df: pd.DataFrame, symbol: str, tf_name: str, side: str) -> bytes:
    """Vẽ chart nến + EMA20/50/10 + đánh dấu điểm tín hiệu, trả về PNG bytes.
    df: dataframe đã qua compute_signals(), bar cuối (-1) là nến chưa đóng nên bỏ qua,
    bar tín hiệu là bar[-2]."""
    lookback = min(CHART_LOOKBACK_BARS, len(df) - 1)
    chart_df = df.iloc[-(lookback + 1):-1].copy()   # không lấy nến đang chạy dở
    chart_df = chart_df.set_index("time")
    ohlc = chart_df[["open", "high", "low", "close"]]

    addplots = [
        mpf.make_addplot(chart_df["emaFast"], color="orange", width=1.0),
        mpf.make_addplot(chart_df["emaSlow"], color="blue", width=1.0),
        mpf.make_addplot(chart_df["emaStop"], color="purple", width=0.8),
    ]

    marker = pd.Series(np.nan, index=chart_df.index)
    last_idx = chart_df.index[-1]
    if side == "long":
        marker.loc[last_idx] = chart_df["low"].iloc[-1] * 0.997
        addplots.append(mpf.make_addplot(marker, type="scatter", markersize=150, marker="^", color="green"))
    else:
        marker.loc[last_idx] = chart_df["high"].iloc[-1] * 1.003
        addplots.append(mpf.make_addplot(marker, type="scatter", markersize=150, marker="v", color="red"))

    fig, _ = mpf.plot(
        ohlc, type="candle", style="charles", addplot=addplots,
        title=f"{symbol}  ({tf_name})", volume=False,
        returnfig=True, figsize=(10, 6),
    )
    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=110, bbox_inches="tight")
    matplotlib.pyplot.close(fig)
    buf.seek(0)
    return buf.getvalue()


# ============================== INDICATORS ==============================

def ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=length, adjust=False).mean()


def rma(series: pd.Series, length: int) -> pd.Series:
    # Wilder's smoothing, dùng cho ATR giống ta.atr() trong Pine
    alpha = 1.0 / length
    return series.ewm(alpha=alpha, adjust=False).mean()


def atr(df: pd.DataFrame, length: int) -> pd.Series:
    high, low, close = df["high"], df["low"], df["close"]
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    return rma(tr, length)


def pivot_high(high: pd.Series, left: int, right: int) -> pd.Series:
    """Trả về giá trị pivot high tại vị trí (i-right), xác nhận tại bar i, giống ta.pivothigh."""
    n = len(high)
    result = pd.Series(np.nan, index=high.index)
    vals = high.values
    for i in range(left + right, n):
        pivot_idx = i - right
        window = vals[pivot_idx - left: i + 1]
        if len(window) == left + right + 1 and vals[pivot_idx] == window.max() and \
           (window == vals[pivot_idx]).sum() == 1:
            result.iloc[i] = vals[pivot_idx]
    return result


def pivot_low(low: pd.Series, left: int, right: int) -> pd.Series:
    n = len(low)
    result = pd.Series(np.nan, index=low.index)
    vals = low.values
    for i in range(left + right, n):
        pivot_idx = i - right
        window = vals[pivot_idx - left: i + 1]
        if len(window) == left + right + 1 and vals[pivot_idx] == window.min() and \
           (window == vals[pivot_idx]).sum() == 1:
            result.iloc[i] = vals[pivot_idx]
    return result


# ============================== STRATEGY LOGIC ==============================

def compute_signals(df: pd.DataFrame) -> pd.DataFrame:
    """Replicate toàn bộ logic Pine Script bar-by-bar (dùng vòng lặp vì có state)."""
    df = df.copy().reset_index(drop=True)

    df["emaFast"] = ema(df["close"], EMA_FAST_LEN)
    df["emaSlow"] = ema(df["close"], EMA_SLOW_LEN)
    df["emaStop"] = ema(df["close"], EMA_STOP_LEN)
    df["atr"] = atr(df, ATR_LEN)

    df["emaTrendUp"] = df["emaFast"] > df["emaSlow"]
    df["emaTrendDown"] = df["emaFast"] < df["emaSlow"]
    df["emaCrossUp"] = (df["emaFast"] > df["emaSlow"]) & (df["emaFast"].shift(1) <= df["emaSlow"].shift(1))
    df["emaCrossDown"] = (df["emaFast"] < df["emaSlow"]) & (df["emaFast"].shift(1) >= df["emaSlow"].shift(1))

    df["ph"] = pivot_high(df["high"], PIVOT_BARS, PIVOT_BARS)
    df["pl"] = pivot_low(df["low"], PIVOT_BARS, PIVOT_BARS)
    df["emaAtPivot"] = df["emaFast"].shift(PIVOT_BARS)

    body = (df["close"] - df["open"]).abs()
    prev_body = body.shift(1)
    df["prevBodyMax"] = prev_body.rolling(STRONG_LOOKBACK).max()
    df["strongBull"] = (df["close"] > df["open"]) & (body >= df["prevBodyMax"] * STRONG_MULT)
    df["strongBear"] = (df["close"] < df["open"]) & (body >= df["prevBodyMax"] * STRONG_MULT)

    n = len(df)
    lastPh = np.nan
    lastPl = np.nan
    longOk = np.zeros(n, dtype=bool)
    shortOk = np.zeros(n, dtype=bool)
    biasLongArr = np.zeros(n, dtype=bool)
    biasShortArr = np.zeros(n, dtype=bool)

    for i in range(n):
        row = df.iloc[i]

        if row["emaCrossDown"]:
            lastPh = np.nan
        if row["emaCrossUp"]:
            lastPl = np.nan

        validPh = (not pd.isna(row["ph"])) and (not pd.isna(row["emaAtPivot"])) and \
                  row["ph"] > row["emaAtPivot"] and row["emaTrendUp"]
        validPl = (not pd.isna(row["pl"])) and (not pd.isna(row["emaAtPivot"])) and \
                  row["pl"] < row["emaAtPivot"] and row["emaTrendDown"]

        if validPh:
            lastPh = row["ph"]
        if validPl:
            lastPl = row["pl"]

        biasLong = row["emaTrendUp"] and not pd.isna(lastPh)
        biasShort = row["emaTrendDown"] and not pd.isna(lastPl)
        biasLongArr[i] = biasLong
        biasShortArr[i] = biasShort

        if pd.isna(row["atr"]) or pd.isna(row["emaFast"]) or pd.isna(row["emaSlow"]):
            continue

        nearEmaLong = ((row["low"] <= row["emaFast"]) and (row["close"] >= row["emaFast"] - row["atr"] * NEAR_ATR_MULT)) or \
                      ((row["low"] <= row["emaSlow"]) and (row["close"] >= row["emaSlow"] - row["atr"] * NEAR_ATR_MULT))
        nearEmaShort = ((row["high"] >= row["emaFast"]) and (row["close"] <= row["emaFast"] + row["atr"] * NEAR_ATR_MULT)) or \
                       ((row["high"] >= row["emaSlow"]) and (row["close"] <= row["emaSlow"] + row["atr"] * NEAR_ATR_MULT))

        if biasLong and nearEmaLong and row["strongBull"] and row["close"] > row["emaFast"]:
            longOk[i] = True
        if biasShort and nearEmaShort and row["strongBear"] and row["close"] < row["emaFast"]:
            shortOk[i] = True

    df["biasLong"] = biasLongArr
    df["biasShort"] = biasShortArr
    df["longOk"] = longOk
    df["shortOk"] = shortOk
    return df


def get_active_position(symbol: str, magic: int):
    """Lấy vị thế giao dịch đang mở trên MT5 theo symbol và magic number."""
    with mt5_lock:
        positions = mt5.positions_get(symbol=symbol, magic=magic)
    if positions is None:
        with mt5_lock:
            err_code, err_desc = mt5.last_error()
        log.error(f"Lỗi positions_get cho {symbol} (magic: {magic}): [{err_code}] {err_desc}")
        return None
    if len(positions) > 0:
        return positions[0]
    return None


# ============================== ĐẶT LỆNH MT5 (khi bấm nút Vào Lệnh) ==============================

def calc_lot_size(symbol: str, entry_price: float, sl_price: float):
    """Tính lot theo RISK_PCT % số dư tài khoản và khoảng cách tới SL (nến vào lệnh)."""
    with mt5_lock:
        info = mt5.symbol_info(symbol)
        account = mt5.account_info()
    if info is None or account is None:
        return None

    price_distance = abs(entry_price - sl_price)
    if price_distance <= 0:
        return None

    tick_size = info.trade_tick_size if info.trade_tick_size > 0 else info.point
    tick_value = info.trade_tick_value
    if tick_size <= 0:
        return None

    # Fallback if trade_tick_value is zero or None (e.g. market closed, not synchronized)
    if tick_value and tick_value > 0:
        ticks = price_distance / tick_size
        loss_per_lot = ticks * tick_value
    else:
        loss_per_lot = price_distance * info.trade_contract_size

    if loss_per_lot <= 0:
        return None

    risk_amount = account.balance * (RISK_PCT / 100.0)
    lot = risk_amount / loss_per_lot
    step = info.volume_step if info.volume_step > 0 else 0.01
    lot = round(lot / step) * step
    lot = max(info.volume_min, min(info.volume_max, lot))
    
    # Calculate precision dynamically from volume_step
    step_str = f"{step:.8f}".rstrip('0')
    precision = len(step_str.split('.')[1]) if '.' in step_str else 0
    return round(lot, precision)


def place_market_order(symbol: str, side: str, sl_price: float, magic: int = 20260704):
    """Đặt lệnh thị trường (Buy/Sell) trên MT5. Trả về (result, error_message)."""
    try:
        with mt5_lock:
            terminal = mt5.terminal_info()
            account = mt5.account_info()
        if terminal is not None and not terminal.trade_allowed:
            return None, ("AutoTrading đang TẮT trên MT5 terminal — bấm nút 'AutoTrading' trên "
                           "thanh công cụ MT5 (chuyển sang xanh) rồi thử lại.")
        if account is not None and not account.trade_allowed:
            return None, "Tài khoản không được phép giao dịch tự động (kiểm tra Tools > Options > Expert Advisors)."

        with mt5_lock:
            select_ok = mt5.symbol_select(symbol, True)
        if not select_ok:
            return None, f"Không chọn được symbol {symbol}"

        # Retry getting tick to ensure synchronization
        tick = None
        for _ in range(5):
            with mt5_lock:
                tick = mt5.symbol_info_tick(symbol)
            if tick is not None:
                break
            time.sleep(0.1)

        if tick is None:
            return None, "Không lấy được giá tick hiện tại từ MT5"

        price = tick.ask if side == "long" else tick.bid
        lot = calc_lot_size(symbol, price, sl_price)
        if not lot or lot <= 0:
            return None, "Không tính được khối lượng lot hợp lệ"

        order_type = mt5.ORDER_TYPE_BUY if side == "long" else mt5.ORDER_TYPE_SELL
        filling_type = get_filling_type(symbol)
        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": lot,
            "type": order_type,
            "price": price,
            "sl": sl_price,
            "deviation": ORDER_DEVIATION,
            "magic": magic,
            "comment": f"EMA Pullback {side.upper()} ({magic})",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": filling_type,
        }
        with mt5_lock:
            result = mt5.order_send(request)
        if result is None:
            with mt5_lock:
                err_code, err_desc = mt5.last_error()
            return None, f"order_send trả về None: [{err_code}] {err_desc}"
        if result.retcode != mt5.TRADE_RETCODE_DONE:
            return None, f"retcode={result.retcode} ({result.comment})"
        return result, None
    except Exception as e:
        log.exception(f"Lỗi hệ thống khi place_market_order cho {symbol}: {e}")
        return None, f"Lỗi exception hệ thống: {e}"


def close_position_by_magic(symbol: str, magic: int, comment: str):
    """Tìm và đóng toàn bộ vị thế có magic và symbol tương ứng."""
    with mt5_lock:
        positions = mt5.positions_get(symbol=symbol, magic=magic)
    if not positions:
        return True, "Không có vị thế nào cần đóng."
        
    all_success = True
    errors = []
    
    for pos in positions:
        ticket = pos.ticket
        volume = pos.volume
        pos_type = pos.type # 0 = BUY, 1 = SELL
        
        with mt5_lock:
            tick = mt5.symbol_info_tick(symbol)
        if tick is None:
            all_success = False
            errors.append(f"Không lấy được tick cho {symbol}")
            continue
            
        filling_type = get_filling_type(symbol)
        
        # BUY position is closed by SELL order, SELL position by BUY order
        if pos_type == mt5.POSITION_TYPE_BUY:
            order_type = mt5.ORDER_TYPE_SELL
            price = tick.bid
            pos_side = "BUY"
        elif pos_type == mt5.POSITION_TYPE_SELL:
            order_type = mt5.ORDER_TYPE_BUY
            price = tick.ask
            pos_side = "SELL"
        else:
            continue
            
        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": volume,
            "type": order_type,
            "position": ticket,
            "price": price,
            "deviation": ORDER_DEVIATION,
            "magic": magic,
            "comment": comment,
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": filling_type,
        }
        
        with mt5_lock:
            result = mt5.order_send(request)
            
        if result is None:
            all_success = False
            with mt5_lock:
                err_code, err_desc = mt5.last_error()
            errors.append(f"order_send trả về None: [{err_code}] {err_desc}")
        elif result.retcode != mt5.TRADE_RETCODE_DONE:
            all_success = False
            errors.append(f"retcode={result.retcode} ({result.comment})")
        else:
            log.info(f"Đóng thành công vị thế {pos_side} {symbol} (Ticket: {ticket}, Vol: {volume})")
            
    if all_success:
        return True, None
    else:
        return False, "; ".join(errors)


# ============================== TELEGRAM BUTTON LISTENER ==============================

def handle_callback_query(cq: dict):
    callback_id = cq["id"]
    data_str = cq.get("data", "")
    message = cq.get("message", {}) or {}
    chat_id = message.get("chat", {}).get("id")
    message_id = message.get("message_id")
    is_photo = "photo" in message

    if not data_str.startswith("enter:"):
        answer_callback_query(callback_id, "Không nhận diện được thao tác.")
        return

    sig_id = data_str.split(":", 1)[1]
    with pending_signals_lock:
        sig = pending_signals.pop(sig_id, None)
        save_pending_signals()

    if sig is None:
        log.warning(f"Không tìm thấy tín hiệu chờ cho ID: {sig_id}. Danh sách khả dụng: {list(pending_signals.keys())}")
        answer_callback_query(callback_id, "Tín hiệu đã hết hạn hoặc đã được xử lý trước đó.")
        return

    answer_callback_query(callback_id, "Đang đặt lệnh...")

    magic = MAGIC_NUMBERS.get(sig["tf_name"], 20260704)
    result, err = place_market_order(sig["symbol"], sig["side"], sig["sl"], magic)
    if err:
        log.error(f"Đặt lệnh thất bại {sig['symbol']} {sig['side']}: {err}")
        if chat_id and message_id:
            edit_message_content(chat_id, message_id, f"❌ Đặt lệnh thất bại: {err}", is_photo)
        else:
            send_telegram(f"❌ Đặt lệnh thất bại {sig['symbol']}: {err}")
    else:
        msg = (f"✅ Đã vào lệnh {sig['side'].upper()} {sig['symbol']} ({sig['tf_name']})\n"
               f"Lot: {result.volume} | Giá khớp: {result.price} | SL: {sig['sl']:.5f}")
        log.info(msg.replace("\n", " | "))
        if chat_id and message_id:
            edit_message_content(chat_id, message_id, msg, is_photo)
        else:
            send_telegram(msg)


def telegram_poller():
    """Chạy nền, long-poll Telegram getUpdates để nhận sự kiện bấm nút (callback_query) và lệnh test."""
    offset = 0
    base = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"

    # Xoá webhook cũ (nếu có) — webhook và getUpdates KHÔNG thể dùng song song,
    # nếu bot từng bị set webhook ở đâu đó, getUpdates sẽ luôn trả rỗng, nút bấm sẽ im lặng.
    try:
        r = requests.post(f"{base}/deleteWebhook", data={"drop_pending_updates": "false"}, timeout=10)
        log.info(f"deleteWebhook: {r.json()}")
    except Exception as e:
        log.error(f"Lỗi deleteWebhook: {e}")

    url = f"{base}/getUpdates"
    log.info("Bắt đầu lắng nghe Telegram (nút bấm & lệnh test)...")
    while True:
        try:
            r = requests.get(url, params={"timeout": 25, "offset": offset}, timeout=30)
            data = r.json()
            if not data.get("ok"):
                log.error(f"getUpdates trả lỗi: {data}")
                time.sleep(3)
                continue
            for update in data.get("result", []):
                offset = update["update_id"] + 1
                
                # Xử lý nút bấm (Callback Query)
                cq = update.get("callback_query")
                if cq:
                    log.info(f"Nhận được callback_query: {cq.get('data')}")
                    handle_callback_query(cq)
                    
                # Xử lý tin nhắn văn bản test từ Telegram
                msg_obj = update.get("message")
                if msg_obj:
                    text_msg = msg_obj.get("text", "").strip()
                    msg_chat_id = str(msg_obj.get("chat", {}).get("id"))
                    # Kiểm tra xem có đúng là lệnh /test từ admin không
                    if text_msg == "/test" and msg_chat_id == str(TELEGRAM_CHAT_ID):
                        log.info("Nhận được lệnh /test từ admin Telegram. Đang tạo tín hiệu test...")
                        test_symbol, test_tf = get_first_configured_symbol_and_timeframe()
                        
                        # Fetch tick to get real current price for test
                        with mt5_lock:
                            tick = mt5.symbol_info_tick(test_symbol)
                        if tick is None:
                            send_telegram("❌ Lỗi test: Không lấy được giá tick hiện tại từ MT5.")
                            continue
                        
                        price = tick.ask
                        sl_price = price - 0.00500  # Giả định SL cách 50 pips (hoặc tương đương)
                        
                        caption = (f"🟡 [TEST-TRADE] LONG {test_symbol} ({test_tf})\n"
                                   f"Giá giả định: {price:.5f}\nSL giả định: {sl_price:.5f}")
                        
                        if AUTO_TRADE:
                            magic = MAGIC_NUMBERS.get(test_tf, 20260704)
                            result, err = place_market_order(test_symbol, "long", sl_price, magic)
                            if err:
                                send_telegram(f"❌ [TEST-TRADE] Đặt lệnh test LONG {test_symbol} thất bại: {err}")
                            else:
                                send_telegram(f"✅ [TEST-TRADE] Đặt lệnh test LONG {test_symbol} thành công!\n"
                                              f"Lot: {result.volume} | Ticket: {result.order} | SL: {sl_price:.5f}")
                        else:
                            send_signal_with_chart(None, test_symbol, test_tf, "long", price, sl_price, caption)
                        
            cleanup_expired_signals()
        except Exception as e:
            log.error(f"Lỗi telegram_poller: {e}")
            time.sleep(5)


# ============================== POSITION TRACKING (đơn giản hoá) ==============================
# Ghi chú: bản gốc cho pyramiding=10 (nhiều lệnh cộng dồn). Ở đây bot chỉ theo dõi
# MỘT vị thế logic mỗi chiều mỗi symbol để tạo tín hiệu Entry/Close, không phải
# công cụ backtest chính xác tuyệt đối. Muốn auto-trade thật, dùng mt5.order_send().

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return {}


def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def send_signal_with_chart(df, symbol, tf_name, side, entry, sl, caption):
    """Gửi tín hiệu kèm ảnh chart (nếu bật) và nút Vào Lệnh (nếu bật và AUTO_TRADE=False)."""
    reply_markup = None
    if ENABLE_TRADE_BUTTON and not AUTO_TRADE:
        sig_id = register_pending_signal(symbol, tf_name, side, entry, sl)
        btn_text = "🟢 Vào Lệnh LONG" if side == "long" else "🔴 Vào Lệnh SHORT"
        reply_markup = {"inline_keyboard": [[{"text": btn_text, "callback_data": f"enter:{sig_id}"}]]}

    if ENABLE_CHART_SCREENSHOT:
        try:
            img = generate_chart_image(df, symbol, tf_name, side)
            send_telegram_photo(img, caption, reply_markup)
            log.info(caption.replace("\n", " | "))
            return
        except Exception as e:
            log.error(f"Lỗi tạo/gửi chart {symbol} [{tf_name}]: {e} — gửi text thay thế.")

    # fallback: không có chart hoặc lỗi khi tạo chart -> gửi text thường (kèm nút nếu có)
    if reply_markup:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        try:
            requests.post(url, data={
                "chat_id": TELEGRAM_CHAT_ID, "text": caption,
                "reply_markup": json.dumps(reply_markup),
            }, timeout=10)
        except Exception as e:
            log.error(f"Telegram send (fallback) lỗi: {e}")
    else:
        send_telegram(caption)
    log.info(caption.replace("\n", " | "))


_warned_insufficient_data = set()   # tránh spam log cảnh báo lặp lại mỗi vòng quét


def process_symbol(symbol: str, tf_name: str, tf_value: int, state: dict):
    with mt5_lock:
        rates = mt5.copy_rates_from_pos(symbol, tf_value, 0, BARS_TO_FETCH)
    if rates is None or len(rates) < EMA_SLOW_LEN + PIVOT_BARS + 5:
        key = f"{symbol}_{tf_name}"
        if key not in _warned_insufficient_data:
            got = 0 if rates is None else len(rates)
            log.warning(f"{symbol} [{tf_name}]: không đủ dữ liệu (có {got}/{EMA_SLOW_LEN + PIVOT_BARS + 5} nến) "
                        f"— có thể do symbol mới niêm yết/ít giao dịch, sẽ tự động thử lại các vòng sau, "
                        f"chỉ cảnh báo 1 lần.")
            _warned_insufficient_data.add(key)
        return
    else:
        _warned_insufficient_data.discard(f"{symbol}_{tf_name}")   # đã đủ dữ liệu trở lại -> reset

    df = pd.DataFrame(rates)
    df["time"] = pd.to_datetime(df["time"], unit="s", utc=True)
    df = compute_signals(df)

    last_closed = df.iloc[-2]     # bar cuối cùng ĐÃ ĐÓNG (bar -1 là nến đang chạy dở)
    bar_time = str(last_closed["time"])

    state_key = f"{symbol}_{tf_name}"   # mỗi symbol+khung có state riêng biệt
    sym_state = state.get(state_key, {"last_processed_time": None})

    # Lấy thông tin vị thế hiện tại từ MT5 theo magic number tương ứng của khung thời gian
    magic = MAGIC_NUMBERS.get(tf_name, 20260704)
    active_pos = get_active_position(symbol, magic)

    # Đề phòng trường hợp khởi chạy bot lần đầu (chưa có state), tránh việc khớp lệnh cũ từ quá khứ
    first_run = (sym_state.get("last_processed_time") is None)
    if first_run:
        sym_state["last_processed_time"] = bar_time
        state[state_key] = sym_state
        if active_pos is None:
            log.info(f"Khởi tạo trạng thái lần đầu cho {symbol} [{tf_name}] tại nến {bar_time} (Không có lệnh mở).")
            return
        else:
            log.info(f"Khởi tạo trạng thái lần đầu cho {symbol} [{tf_name}] tại nến {bar_time} (Phát hiện lệnh mở, đang theo dõi EMA Stop).")

    if sym_state["last_processed_time"] == bar_time:
        return  # Nến này đã được xử lý rồi, bỏ qua

    price = last_closed["close"]

    # ---- 1. Theo dõi EXIT (EMA Stop) cho vị thế đang mở ----
    # Hard SL đã đặt sẵn trực tiếp trên MT5 khi mở lệnh, ở đây chỉ xử lý đóng lệnh chủ động theo EMA Stop
    if active_pos is not None:
        pos_type = active_pos.type  # 0 = BUY, 1 = SELL
        ticket = active_pos.ticket
        volume = active_pos.volume
        
        if USE_EMA_STOP:
            if pos_type == mt5.POSITION_TYPE_BUY:
                if last_closed["close"] < last_closed["emaStop"]:
                    close_price = last_closed["close"]
                    ema_stop = last_closed["emaStop"]
                    caption = (f"⚪️ [AUTO-CLOSE] CLOSE LONG {symbol} ({tf_name})\n"
                               f"Giá đóng cửa: {close_price:.5f} < EMA Stop: {ema_stop:.5f}")
                    
                    if AUTO_TRADE:
                        success, err = close_position_by_magic(symbol, magic, f"EMA Stop Long ({tf_name})")
                        if success:
                            send_telegram(caption + "\n✅ Đã đóng vị thế thành công trên MT5.")
                        else:
                            send_telegram(caption + f"\n❌ Đóng vị thế thất bại trên MT5: {err}")
                    else:
                        send_telegram(caption + "\n⚠️ Phát hiện tín hiệu đóng lệnh (AUTO_TRADE đang TẮT).")

            elif pos_type == mt5.POSITION_TYPE_SELL:
                if last_closed["close"] > last_closed["emaStop"]:
                    close_price = last_closed["close"]
                    ema_stop = last_closed["emaStop"]
                    caption = (f"⚪️ [AUTO-CLOSE] CLOSE SHORT {symbol} ({tf_name})\n"
                               f"Giá đóng cửa: {close_price:.5f} > EMA Stop: {ema_stop:.5f}")
                    
                    if AUTO_TRADE:
                        success, err = close_position_by_magic(symbol, magic, f"EMA Stop Short ({tf_name})")
                        if success:
                            send_telegram(caption + "\n✅ Đã đóng vị thế thành công trên MT5.")
                        else:
                            send_telegram(caption + f"\n❌ Đóng vị thế thất bại trên MT5: {err}")
                    else:
                        send_telegram(caption + "\n⚠️ Phát hiện tín hiệu đóng lệnh (AUTO_TRADE đang TẮT).")

    # ---- 2. Theo dõi ENTRY (Long/Short) ----
    # Chỉ vào lệnh khi KHÔNG có vị thế mở cho khung thời gian này (tương ứng với isFlat)
    if active_pos is None:
        if last_closed["longOk"]:
            # Tính SL dựa trên low của các nến trước nến tín hiệu (không bao gồm nến tín hiệu last_closed ở index -2)
            # Slice [-7:-2] tương ứng với 5 nến trước last_closed: index -7, -6, -5, -4, -3
            sl_lookback = df["low"].iloc[-(SL_LOOKBACK_BARS + 2):-2].min()
            risk = price - sl_lookback
            if risk > 0:
                caption = (f"🟢 LONG {symbol} ({tf_name})\n"
                           f"Giá entry: {price:.5f}\nSL: {sl_lookback:.5f}\nThời gian nến: {bar_time}")
                
                if AUTO_TRADE:
                    result, err = place_market_order(symbol, "long", sl_lookback, magic)
                    if err:
                        caption = f"❌ [AUTO-TRADE] Đặt lệnh LONG {symbol} ({tf_name}) thất bại: {err}\n" + caption
                        send_telegram(caption)
                    else:
                        caption = (f"✅ [AUTO-TRADE] Đặt lệnh LONG {symbol} ({tf_name}) thành công\n"
                                   f"Lot: {result.volume} | Giá khớp: {result.price} | SL: {sl_lookback:.5f}\n"
                                   f"Thời gian nến: {bar_time}")
                        send_signal_with_chart(df, symbol, tf_name, "long", price, sl_lookback, caption)
                else:
                    send_signal_with_chart(df, symbol, tf_name, "long", price, sl_lookback, caption)

        elif last_closed["shortOk"]:
            # Tính SL dựa trên high của các nến trước nến tín hiệu (không bao gồm nến tín hiệu last_closed ở index -2)
            # Slice [-7:-2] tương ứng với 5 nến trước last_closed: index -7, -6, -5, -4, -3
            sl_lookback = df["high"].iloc[-(SL_LOOKBACK_BARS + 2):-2].max()
            risk = sl_lookback - price
            if risk > 0:
                caption = (f"🔴 SHORT {symbol} ({tf_name})\n"
                           f"Giá entry: {price:.5f}\nSL: {sl_lookback:.5f}\nThời gian nến: {bar_time}")
                
                if AUTO_TRADE:
                    result, err = place_market_order(symbol, "short", sl_lookback, magic)
                    if err:
                        caption = f"❌ [AUTO-TRADE] Đặt lệnh SHORT {symbol} ({tf_name}) thất bại: {err}\n" + caption
                        send_telegram(caption)
                    else:
                        caption = (f"✅ [AUTO-TRADE] Đặt lệnh SHORT {symbol} ({tf_name}) thành công\n"
                                   f"Lot: {result.volume} | Giá khớp: {result.price} | SL: {sl_lookback:.5f}\n"
                                   f"Thời gian nến: {bar_time}")
                        send_signal_with_chart(df, symbol, tf_name, "short", price, sl_lookback, caption)
                else:
                    send_signal_with_chart(df, symbol, tf_name, "short", price, sl_lookback, caption)

    sym_state["last_processed_time"] = bar_time
    state[state_key] = sym_state


# ============================== MAIN LOOP ==============================

def warm_up_history(tasks, max_retries=5, retry_delay=30):
    """Chủ động lấy dữ liệu vài lần khi khởi động, đợi MT5 đồng bộ lịch sử cho các symbol
    mới/ít phổ biến (thường gặp với crypto). Trả về danh sách symbol/khung vẫn thiếu dữ liệu sau cùng."""
    min_bars = EMA_SLOW_LEN + PIVOT_BARS + 5
    pending = list(tasks)

    for attempt in range(1, max_retries + 1):
        still_pending = []
        for symbol, tf_name, tf_value in pending:
            with mt5_lock:
                rates = mt5.copy_rates_from_pos(symbol, tf_value, 0, BARS_TO_FETCH)
            if rates is None or len(rates) < min_bars:
                still_pending.append((symbol, tf_name, tf_value))

        if not still_pending:
            log.info("Đã tải đủ dữ liệu lịch sử cho toàn bộ symbol.")
            return []

        pending = still_pending
        if attempt < max_retries:
            log.info(f"[Warm-up {attempt}/{max_retries}] Còn {len(pending)} symbol/khung thiếu dữ liệu, "
                      f"đợi {retry_delay}s để MT5 đồng bộ rồi thử lại...")
            time.sleep(retry_delay)

    names = sorted({f"{s}[{tf}]" for s, tf, _ in pending})
    log.warning(f"Sau {max_retries} lần thử vẫn thiếu dữ liệu cho {len(names)} mục "
                f"(có thể do mới niêm yết / ít lịch sử trên broker): {names}")
    return pending


def main():
    if not TELEGRAM_TOKEN or TELEGRAM_TOKEN == "PASTE_YOUR_BOT_TOKEN_HERE":
        log.error("LỖI CẤU HÌNH: Bạn chưa nhập TELEGRAM_TOKEN hợp lệ. Vui lòng thay thế 'PASTE_YOUR_BOT_TOKEN_HERE' bằng token bot của bạn.")
        return
    if not TELEGRAM_CHAT_ID or TELEGRAM_CHAT_ID == "PASTE_YOUR_CHAT_ID_HERE":
        log.error("LỖI CẤU HÌNH: Bạn chưa nhập TELEGRAM_CHAT_ID hợp lệ. Vui lòng thay thế 'PASTE_YOUR_CHAT_ID_HERE' bằng chat ID của bạn.")
        return

    with mt5_lock:
        init_ok = mt5.initialize()
    if not init_ok:
        with mt5_lock:
            err = mt5.last_error()
        log.error(f"MT5 initialize() failed: {err}")
        return
    log.info("Kết nối MT5 thành công.")

    symbol_tf_map = get_symbols_to_watch()
    if not symbol_tf_map:
        log.error("Danh sách symbol rỗng, dừng chương trình.")
        with mt5_lock:
            mt5.shutdown()
        return

    for s in symbol_tf_map:
        with mt5_lock:
            select_ok = mt5.symbol_select(s, True)
        if not select_ok:
            log.warning(f"Không chọn được symbol {s} — bỏ qua.")

    state = load_state()

    if ENABLE_TRADE_BUTTON:
        load_pending_signals()
        threading.Thread(target=telegram_poller, daemon=True).start()

    tasks = [
        (symbol, tf_name, TIMEFRAMES_ALL[tf_name])
        for symbol, tf_names in symbol_tf_map.items()
        for tf_name in tf_names
        if tf_name in TIMEFRAMES_ALL
    ]
    log.info(f"Bắt đầu theo dõi {len(symbol_tf_map)} symbol — tổng {len(tasks)} tổ hợp symbol x khung mỗi vòng quét.")

    still_missing = warm_up_history(tasks)
    if still_missing:
        log.warning(f"Bot vẫn sẽ chạy, nhưng {len(still_missing)} mục trên có thể tiếp tục báo "
                     f"'không đủ dữ liệu' cho tới khi broker có đủ lịch sử.")

    try:
        while True:
            start = time.time()
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {
                    executor.submit(process_symbol, symbol, tf_name, tf_value, state): (symbol, tf_name)
                    for symbol, tf_name, tf_value in tasks
                }
                for future in as_completed(futures):
                    symbol, tf_name = futures[future]
                    try:
                        future.result()
                    except Exception as e:
                        log.error(f"Lỗi xử lý {symbol} [{tf_name}]: {e}")

            save_state(state)
            elapsed = time.time() - start
            log.info(f"Quét xong {len(tasks)} tổ hợp trong {elapsed:.1f}s.")
            sleep_time = max(1.0, POLL_SECONDS - elapsed)
            time.sleep(sleep_time)
    except KeyboardInterrupt:
        log.info("Dừng bot.")
    finally:
        with mt5_lock:
            mt5.shutdown()


if __name__ == "__main__":
    main()

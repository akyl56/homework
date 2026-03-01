#!/usr/bin/env python3
"""ETF 邮件提醒脚本。

功能概览：
1) 抓取 ETF（CSPX.GB / CSNDX.CH / SMH.GB / IWY.US / COWZ.NL）行情（当前价、当日最高、当日最低）
2) 抓取 CNN Fear & Greed 指标
3) 根据抓取结果生成 PASS / NOK 邮件并通过 SMTP 发送
4) 支持 --once 单次执行和按北京时间工作日定时执行
"""

import argparse
import configparser
import gzip
import html as html_lib
import json
import re
import smtplib
import sys
import time
import zlib
from dataclasses import dataclass
from datetime import datetime
from email.mime.text import MIMEText
from typing import Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

# 明确使用北京时间，避免服务器本地时区差异造成定时偏移
BEIJING_TZ = ZoneInfo("Asia/Shanghai")
# 统一请求头，降低被目标站点拒绝的概率
USER_AGENT = "Mozilla/5.0 (ETF-Reminder/1.0; +https://github.com/aky56/homework)"
# 单次 HTTP 请求超时（秒）
REQUEST_TIMEOUT = 10
# 失败重试次数（总尝试次数 = RETRY_COUNT + 1）
RETRY_COUNT = 2

# 浏览器风格请求头：降低被反爬误判风险
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
}

# 目标 ETF 与可用数据源候选（按顺序依次尝试）
ETF_SYMBOL_CANDIDATES: Dict[str, List[Tuple[str, str]]] = {
    "CSPX.GB": [
        ("yahoo", "CSPX.L"),
        ("stooq", "cspx.gb"),
    ],
    "CSNDX.CH": [
        ("yahoo", "CSNDX.SW"),
        ("stooq", "csndx.ch"),
    ],
    "SMH.GB": [
        ("yahoo", "SMH.L"),
        ("yahoo", "SMH"),
        ("stooq", "smh.gb"),
    ],
    "IWY.US": [
        ("yahoo", "IWY"),
        ("stooq", "iwy.us"),
    ],
    "COWZ.NL": [
        ("yahoo", "COWZ.AS"),
        ("stooq", "cowz.nl"),
    ],
}


@dataclass
class PriceResult:
    """ETF 行情抓取结果。"""

    symbol: str
    current: Optional[float]
    day_high: Optional[float]
    day_low: Optional[float]
    source: Optional[str]
    error: Optional[str]


@dataclass
class FearGreedResult:
    """Fear & Greed 指标抓取结果。"""

    value: Optional[float]
    rating: Optional[str]
    error: Optional[str]


@dataclass
class CnnMarketResult:
    """CNN 页面市场摘要结果（S&P 500 / NASDAQ）。"""

    sp500_value: Optional[str]
    sp500_change: Optional[str]
    nasdaq_value: Optional[str]
    nasdaq_change: Optional[str]
    error: Optional[str]


@dataclass
class IndexSnapshot:
    """指数快照（点位 + 涨跌幅）。"""

    value: Optional[str]
    change: Optional[str]
    error: Optional[str]


@dataclass
class SmtpConfig:
    """SMTP 配置。"""

    host: str
    port: int
    username: str
    password: str
    from_email: str
    use_tls: bool


def log(message: str) -> None:
    """统一日志输出格式（带北京时间时间戳）。"""

    now = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")
    print(f"[{now}] {message}")


def _decode_body(raw: bytes, content_encoding: str) -> str:
    """按响应压缩格式解码为文本。"""

    encoding = (content_encoding or "").lower()
    data = raw
    if "gzip" in encoding:
        data = gzip.decompress(raw)
    elif "deflate" in encoding:
        try:
            data = zlib.decompress(raw, -zlib.MAX_WBITS)
        except zlib.error:
            data = zlib.decompress(raw)
    return data.decode("utf-8", errors="replace")


def _http_get_decoded_text(url: str, extra_headers: Optional[Dict[str, str]] = None) -> str:
    """发送 GET 请求并自动处理 gzip/deflate 压缩与重试。"""

    headers = dict(DEFAULT_HEADERS)
    if extra_headers:
        headers.update(extra_headers)

    last_error: Optional[Exception] = None
    for attempt in range(RETRY_COUNT + 1):
        try:
            req = Request(url, headers=headers)
            with urlopen(req, timeout=REQUEST_TIMEOUT) as response:
                raw = response.read()
                return _decode_body(raw, response.headers.get("Content-Encoding", ""))
        except (HTTPError, URLError, TimeoutError, UnicodeDecodeError, zlib.error, OSError) as exc:
            last_error = exc
            if attempt < RETRY_COUNT:
                delay = 2 ** attempt
                log(f"请求失败，{delay}s 后重试: {exc}")
                time.sleep(delay)
    raise RuntimeError(f"请求失败: {last_error}")


def http_get_json(url: str, extra_headers: Optional[Dict[str, str]] = None) -> dict:
    """发送 GET 请求并解析 JSON。

    - 使用统一 User-Agent
    - timeout=10
    - 失败后指数退避重试（1s, 2s）
    """

    text = _http_get_decoded_text(url, extra_headers=extra_headers)
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"JSON 解析失败: {exc}") from exc


def http_get_text(url: str) -> str:
    """发送 GET 请求并返回文本。

    逻辑同 `http_get_json`，只是不做 JSON 解析。
    """

    return _http_get_decoded_text(url)


def fetch_yahoo_price(symbol: str) -> Tuple[float, float, float]:
    """从 Yahoo 抓取行情，返回 (当前价, 当日最高, 当日最低)。"""

    query = urlencode({"range": "1d", "interval": "5m"})
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?{query}"
    data = http_get_json(url)
    result = data.get("chart", {}).get("result")
    if not result:
        raise RuntimeError(f"Yahoo 返回无数据: {data.get('chart', {}).get('error')}")

    info = result[0]
    meta = info.get("meta", {})
    quote = info.get("indicators", {}).get("quote", [{}])[0]
    current = meta.get("regularMarketPrice")
    highs = [h for h in (quote.get("high") or []) if h is not None]
    lows = [l for l in (quote.get("low") or []) if l is not None]

    # 若关键字段缺失则抛错，由上层兜底并切换备用数据源
    if current is None or not highs or not lows:
        raise RuntimeError("Yahoo 数据字段缺失")

    return float(current), float(max(highs)), float(min(lows))


def fetch_stooq_price(symbol: str) -> Tuple[float, float, float]:
    """从 Stooq 抓取 CSV 行情，返回 (当前价, 当日最高, 当日最低)。"""

    url = f"https://stooq.com/q/l/?s={symbol}&f=sd2t2ohlcv&h&e=csv"
    text = http_get_text(url)
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if len(lines) < 2:
        raise RuntimeError("Stooq 返回数据为空")

    header = [h.strip().lower() for h in lines[0].split(",")]
    values = [v.strip() for v in lines[1].split(",")]
    row = dict(zip(header, values))
    if row.get("close") in (None, "N/D"):
        raise RuntimeError("Stooq close 字段缺失")

    current = float(row["close"])
    day_high = float(row["high"])
    day_low = float(row["low"])
    return current, day_high, day_low


def fetch_price(symbol: str) -> PriceResult:
    """抓取指定 ETF 行情。

    按候选源顺序逐个尝试；如果全部失败，返回带错误信息的结果，
    但不抛异常，确保流程可继续生成 NOK 邮件。
    """

    errors: List[str] = []
    for source, source_symbol in ETF_SYMBOL_CANDIDATES[symbol]:
        try:
            log(f"抓取 {symbol}（{source}:{source_symbol}）...")
            if source == "yahoo":
                current, day_high, day_low = fetch_yahoo_price(source_symbol)
            else:
                current, day_high, day_low = fetch_stooq_price(source_symbol)
            return PriceResult(symbol, current, day_high, day_low, f"{source}:{source_symbol}", None)
        except Exception as exc:
            err = f"{source} 失败: {exc}"
            errors.append(err)
            log(f"{symbol} 抓取失败: {err}")

    return PriceResult(symbol, None, None, None, None, " | ".join(errors))


def _to_float(value: object) -> Optional[float]:
    """尽量将输入转换为 float，失败时返回 None。"""

    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _extract_fear_greed_from_payload(data: dict) -> Tuple[Optional[float], Optional[str]]:
    """兼容 CNN 不同返回结构，提取 Fear & Greed 值和描述。"""

    fg = data.get("fear_and_greed", {}) if isinstance(data, dict) else {}

    # 结构 A：fear_and_greed.now.value / valueText
    if isinstance(fg, dict):
        now_data = fg.get("now", {}) if isinstance(fg.get("now"), dict) else {}
        value = _to_float(now_data.get("value"))
        if value is None:
            value = _to_float(now_data.get("score"))
        if value is None:
            value = _to_float(fg.get("value"))
        if value is None:
            value = _to_float(fg.get("score"))
        rating = now_data.get("valueText") or now_data.get("rating") or fg.get("rating")
        if value is not None:
            return value, str(rating) if rating else None

    # 结构 B：历史数组里最后一个点（常见字段 y/value/score）
    history = data.get("fear_and_greed_historical")
    if isinstance(history, dict):
        items = history.get("data") or history.get("history") or history.get("values")
        if isinstance(items, list) and items:
            for item in reversed(items):
                if not isinstance(item, dict):
                    continue
                value = _to_float(item.get("y"))
                if value is None:
                    value = _to_float(item.get("value"))
                if value is None:
                    value = _to_float(item.get("score"))
                if value is not None:
                    rating = item.get("rating") or item.get("valueText")
                    return value, str(rating) if rating else None

    return None, None


def fetch_fear_greed() -> FearGreedResult:
    """抓取 CNN Fear & Greed 指标。

    失败时返回带 error 的结果，不抛异常，保证主流程可继续。
    """

    url = f"https://production.dataviz.cnn.io/index/fearandgreed/graphdata?ts={int(time.time() * 1000)}"
    cnn_headers = {
        "Referer": "https://www.cnn.com/markets/fear-and-greed",
        "Origin": "https://www.cnn.com",
    }
    try:
        log("抓取 CNN Fear & Greed...")
        data = http_get_json(url, extra_headers=cnn_headers)
        value, rating = _extract_fear_greed_from_payload(data)
        if value is None:
            raise RuntimeError(f"Fear & Greed 字段缺失，可用键: {list(data.keys())[:8] if isinstance(data, dict) else 'N/A'}")
        if rating:
            log(f"CNN Fear & Greed 抓取成功: {value:.1f} ({rating})")
        else:
            log(f"CNN Fear & Greed 抓取成功: {value:.1f}")
        return FearGreedResult(float(value), str(rating) if rating else None, None)
    except Exception as exc:
        error = f"CNN Fear & Greed 抓取失败: {exc}"
        log(error)
        return FearGreedResult(None, None, error)


def _extract_market_line(html: str, label: str) -> Tuple[Optional[str], Optional[str]]:
    """从 CNN 页面 HTML 中提取指定指数的点位和涨跌幅。"""

    # 先做实体和 \uXXXX 解码，提升在 script 内嵌 JSON 场景下的匹配概率
    normalized = html_lib.unescape(html)

    def _decode_unicode_escape(m: re.Match[str]) -> str:
        try:
            return chr(int(m.group(1), 16))
        except ValueError:
            return m.group(0)

    normalized = re.sub(r"\\u([0-9a-fA-F]{4})", _decode_unicode_escape, normalized)

    if label.upper() == "S&P 500":
        label_pattern = r"(?:S\s*&\s*P\s*500|S\s*P\s*500|SP500)"
    elif label.upper() == "NASDAQ":
        label_pattern = r"NASDAQ"
    else:
        label_pattern = re.escape(label)

    # 允许中间有标签/空白，兼容部分页面结构变化；百分比允许带/不带 %
    pattern = re.compile(
        rf"{label_pattern}(?:.|\n){{0,350}}?([0-9][0-9,]*(?:\.[0-9]+)?)(?:.|\n){{0,180}}?([-+]?\d+(?:\.[0-9]+)?%?)",
        re.IGNORECASE,
    )
    for m in pattern.finditer(normalized):
        value = _format_index_value(m.group(1).replace(",", ""))
        change = _format_percent(m.group(2))
        value_num = _to_float((value or "").replace(",", ""))
        change_num = _to_float((change or "").replace("%", ""))

        # 过滤误匹配：如把标签本身中的“500”或无意义 0 当作指数点位
        if value_num is None or change_num is None:
            continue
        if value_num < 1000 or value_num > 100000:
            continue
        if abs(change_num) > 20:
            continue
        return value, change

    return None, None


def _format_index_value(value: object) -> Optional[str]:
    """将指数点位格式化为带千分位字符串。"""

    num = _to_float(value)
    if num is None:
        return None
    return f"{num:,.2f}"


def _format_percent(value: object) -> Optional[str]:
    """将涨跌幅字段规范为 xx.xx%。"""

    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.endswith("%"):
            return text
        num = _to_float(text)
        if num is None:
            return None
        return f"{num:.2f}%"
    num = _to_float(value)
    if num is None:
        return None
    # 有些接口返回小数（0.0014 代表 0.14%）
    if abs(num) <= 1:
        num = num * 100
    return f"{num:.2f}%"


def _iter_dicts(node: object):
    """深度遍历 JSON 树中的所有 dict。"""

    if isinstance(node, dict):
        yield node
        for v in node.values():
            yield from _iter_dicts(v)
    elif isinstance(node, list):
        for item in node:
            yield from _iter_dicts(item)


def _extract_indices_from_embedded_json(html: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """从页面内嵌 JSON（如 __NEXT_DATA__）提取 S&P500/NASDAQ 数据。"""

    script_pattern = re.compile(
        r"<script[^>]*(?:id=['\"]__NEXT_DATA__['\"][^>]*)?[^>]*type=['\"]application/json['\"][^>]*>(.*?)</script>",
        re.IGNORECASE | re.DOTALL,
    )

    sp500_value = sp500_change = nasdaq_value = nasdaq_change = None

    for match in script_pattern.finditer(html):
        raw = html_lib.unescape(match.group(1).strip())
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except Exception:
            continue

        for d in _iter_dicts(payload):
            label_fields = [
                str(d.get("label", "")),
                str(d.get("name", "")),
                str(d.get("title", "")),
                str(d.get("symbol", "")),
                str(d.get("ticker", "")),
            ]
            label_text = " ".join(label_fields).upper().replace(" ", "")

            value = (
                d.get("value")
                if "value" in d
                else d.get("price", d.get("last", d.get("lastPrice", d.get("close"))))
            )
            change = d.get("changePercent", d.get("percentChange", d.get("pctChange", d.get("change_pct"))))

            value_text = _format_index_value(value)
            change_text = _format_percent(change)
            value_num = _to_float((value_text or "").replace(",", ""))
            change_num = _to_float((change_text or "").replace("%", ""))
            if value_num is None or change_num is None:
                continue
            if value_num < 1000 or value_num > 100000 or abs(change_num) > 20:
                continue

            if ("S&P500" in label_text or "SP500" in label_text) and (sp500_value is None or sp500_change is None):
                sp500_value = sp500_value or value_text
                sp500_change = sp500_change or change_text
            elif "NASDAQ" in label_text and (nasdaq_value is None or nasdaq_change is None):
                nasdaq_value = nasdaq_value or value_text
                nasdaq_change = nasdaq_change or change_text

            if sp500_value and sp500_change and nasdaq_value and nasdaq_change:
                return sp500_value, sp500_change, nasdaq_value, nasdaq_change

    return sp500_value, sp500_change, nasdaq_value, nasdaq_change


def _fetch_index_from_yahoo(symbol: str, label: str) -> IndexSnapshot:
    """从 Yahoo 抓取指数快照，作为 CNN 页面解析失败时的兜底。"""

    quote_url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={urlencode({'s': symbol})[2:]}"
    try:
        data = http_get_json(quote_url)
        quote_items = (((data.get("quoteResponse") or {}).get("result")) or []) if isinstance(data, dict) else []
        item = quote_items[0] if quote_items else {}
        value = _format_index_value(item.get("regularMarketPrice"))
        change = _format_percent(item.get("regularMarketChangePercent"))

        # 兼容仅返回涨跌额未返回百分比的场景
        if value and not change:
            change = _format_percent(item.get("regularMarketChange"))

        # 仍缺字段时，退回 chart 接口补齐
        if not value or not change:
            chart_url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=5d"
            chart = http_get_json(chart_url)
            result = (chart.get("chart") or {}).get("result") or []
            if result:
                info = result[0]
                meta = info.get("meta") or {}
                quote = (((info.get("indicators") or {}).get("quote") or [{}])[0])
                closes = [x for x in (quote.get("close") or []) if x is not None]
                last_close = float(closes[-1]) if closes else None
                prev_close = _to_float(meta.get("chartPreviousClose") or meta.get("previousClose"))
                if not value:
                    value = _format_index_value(meta.get("regularMarketPrice") or last_close)
                if not change:
                    pct = _to_float(meta.get("regularMarketChangePercent"))
                    if pct is None and last_close is not None and prev_close not in (None, 0):
                        pct = ((last_close - prev_close) / prev_close) * 100
                    change = _format_percent(pct)

        if not value or not change:
            raise RuntimeError("Yahoo 指数字段缺失")
        log(f"{label} 使用 Yahoo 兜底成功: {value} ({change})")
        return IndexSnapshot(value, change, None)
    except Exception as exc:
        return IndexSnapshot(None, None, f"{label} Yahoo 兜底失败: {exc}")


def _extract_index_from_any_json(data: object, label: str) -> IndexSnapshot:
    """从任意 JSON 结构中按标签提取指数点位和涨跌幅。"""

    target = label.upper().replace(" ", "")
    if target == "S&P500":
        aliases = {"S&P500", "SP500", "GSPC", "^GSPC"}
    elif target == "NASDAQ":
        aliases = {"NASDAQ", "IXIC", "^IXIC", "NASDAQCOMPOSITE"}
    else:
        aliases = {target}

    for d in _iter_dicts(data):
        label_fields = [
            str(d.get("label", "")),
            str(d.get("name", "")),
            str(d.get("title", "")),
            str(d.get("symbol", "")),
            str(d.get("ticker", "")),
            str(d.get("id", "")),
        ]
        label_text = "".join(label_fields).upper().replace(" ", "")
        if not any(alias in label_text for alias in aliases):
            continue

        value = d.get("value", d.get("price", d.get("last", d.get("lastPrice", d.get("close")))))
        change = d.get("changePercent", d.get("percentChange", d.get("pctChange", d.get("change_pct"))))
        value_text = _format_index_value(value)
        change_text = _format_percent(change)
        if value_text and change_text:
            return IndexSnapshot(value_text, change_text, None)

    return IndexSnapshot(None, None, f"{label} 未在 JSON 中解析到")


def _fetch_index_from_cnn_api(label: str) -> IndexSnapshot:
    """尝试 CNN 可能的市场接口（避免页面结构变化影响）。"""

    api_urls = [
        "https://production.dataviz.cnn.io/markets/indexes",
        "https://production.dataviz.cnn.io/markets/indexes?exchange=US",
        "https://production.dataviz.cnn.io/markets/overview",
    ]
    last_err: Optional[str] = None
    for url in api_urls:
        try:
            data = http_get_json(url, extra_headers={"Referer": "https://edition.cnn.com/markets/fear-and-greed"})
            snap = _extract_index_from_any_json(data, label)
            if snap.value and snap.change:
                log(f"{label} 使用 CNN API 兜底成功: {snap.value} ({snap.change})")
                return snap
            last_err = snap.error or f"{label} CNN API 未命中"
        except Exception as exc:
            last_err = f"{label} CNN API 请求失败: {exc}"
    return IndexSnapshot(None, None, last_err or f"{label} CNN API 兜底失败")


def _fetch_index_from_stooq(symbol: str, label: str) -> IndexSnapshot:
    """使用 Stooq 指数符号兜底。"""

    try:
        url = f"https://stooq.com/q/l/?s={symbol}&f=sd2t2ohlcv&h&e=csv"
        text = http_get_text(url)
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        if len(lines) < 2:
            raise RuntimeError("Stooq 返回空数据")
        cols = [c.strip().lower() for c in lines[0].split(",")]
        vals = [v.strip() for v in lines[1].split(",")]
        row = dict(zip(cols, vals))
        close = _to_float(row.get("close"))
        open_ = _to_float(row.get("open"))
        if close is None:
            raise RuntimeError("Stooq close 缺失")
        change_pct: Optional[float] = None
        if open_ not in (None, 0):
            change_pct = ((close - open_) / open_) * 100
        value = _format_index_value(close)
        change = _format_percent(change_pct)
        if not value or not change:
            raise RuntimeError("Stooq 指数字段缺失")
        log(f"{label} 使用 Stooq 兜底成功: {value} ({change})")
        return IndexSnapshot(value, change, None)
    except Exception as exc:
        return IndexSnapshot(None, None, f"{label} Stooq 兜底失败: {exc}")


def fetch_cnn_market_snapshot() -> CnnMarketResult:
    """抓取 CNN Fear & Greed 页面上的 S&P 500 / NASDAQ 摘要。"""

    url = f"https://edition.cnn.com/markets/fear-and-greed?ts={int(time.time() * 1000)}"
    try:
        log("抓取 CNN Markets 页面（S&P 500 / NASDAQ）...")
        html = http_get_text(url)
        sp500_value, sp500_change = _extract_market_line(html, "S&P 500")
        nasdaq_value, nasdaq_change = _extract_market_line(html, "NASDAQ")

        # HTML 正则失败时，尝试解析页面内嵌 JSON
        if (not sp500_value or not sp500_change) or (not nasdaq_value or not nasdaq_change):
            j_sp500_value, j_sp500_change, j_nasdaq_value, j_nasdaq_change = _extract_indices_from_embedded_json(html)
            sp500_value = sp500_value or j_sp500_value
            sp500_change = sp500_change or j_sp500_change
            nasdaq_value = nasdaq_value or j_nasdaq_value
            nasdaq_change = nasdaq_change or j_nasdaq_change

        # 若仍有缺失，使用 Yahoo 指数做兜底（避免 CNN 页面结构波动导致完全失败）
        fallback_errors: List[str] = []
        if not sp500_value or not sp500_change:
            sp500_fb = _fetch_index_from_cnn_api("S&P 500")
            if not sp500_fb.value or not sp500_fb.change:
                sp500_fb = _fetch_index_from_stooq("^spx", "S&P 500")
            if not sp500_fb.value or not sp500_fb.change:
                sp500_fb = _fetch_index_from_yahoo("^GSPC", "S&P 500")
            sp500_value = sp500_value or sp500_fb.value
            sp500_change = sp500_change or sp500_fb.change
            if sp500_fb.error:
                fallback_errors.append(sp500_fb.error)

        if not nasdaq_value or not nasdaq_change:
            nasdaq_fb = _fetch_index_from_cnn_api("NASDAQ")
            if not nasdaq_fb.value or not nasdaq_fb.change:
                nasdaq_fb = _fetch_index_from_stooq("^ndq", "NASDAQ")
            if not nasdaq_fb.value or not nasdaq_fb.change:
                nasdaq_fb = _fetch_index_from_yahoo("^IXIC", "NASDAQ")
            nasdaq_value = nasdaq_value or nasdaq_fb.value
            nasdaq_change = nasdaq_change or nasdaq_fb.change
            if nasdaq_fb.error:
                fallback_errors.append(nasdaq_fb.error)

        if not sp500_value and not nasdaq_value:
            if fallback_errors:
                raise RuntimeError("页面中未匹配到 S&P 500/NASDAQ 数值，且兜底失败: " + " | ".join(fallback_errors))
            raise RuntimeError("页面中未匹配到 S&P 500/NASDAQ 数值")

        if sp500_value and sp500_change:
            log(f"CNN S&P 500: {sp500_value} ({sp500_change})")
        if nasdaq_value and nasdaq_change:
            log(f"CNN NASDAQ: {nasdaq_value} ({nasdaq_change})")

        return CnnMarketResult(
            sp500_value=sp500_value,
            sp500_change=sp500_change,
            nasdaq_value=nasdaq_value,
            nasdaq_change=nasdaq_change,
            error=None,
        )
    except Exception as exc:
        err = f"CNN Markets 页面抓取失败: {exc}"
        log(err)
        return CnnMarketResult(None, None, None, None, err)


def read_config(path: str) -> SmtpConfig:
    """读取并校验 SMTP 配置。

    配置缺失属于“不可恢复错误”，由 main() 捕获后直接退出。
    """

    parser = configparser.ConfigParser()
    if not parser.read(path):
        raise FileNotFoundError(f"无法读取配置文件: {path}")
    if "smtp" not in parser:
        raise KeyError("配置缺少 [smtp] 段")

    section = parser["smtp"]
    required = ["host", "port", "username", "password"]
    missing = [key for key in required if not section.get(key)]
    if missing:
        raise KeyError(f"配置缺少字段: {', '.join(missing)}")

    from_email = section.get("from_email", section["username"])
    use_tls = section.getboolean("use_tls", fallback=True)

    return SmtpConfig(
        host=section["host"],
        port=section.getint("port"),
        username=section["username"],
        password=section["password"],
        from_email=from_email,
        use_tls=use_tls,
    )


def build_report() -> Tuple[str, str]:
    """构建邮件主题和正文。

    规则：若任一关键数据缺失（任一 ETF 或 Fear&Greed），主题标记为 NOK。
    """

    prices = [fetch_price(symbol) for symbol in ETF_SYMBOL_CANDIDATES]
    fg = fetch_fear_greed()
    market = fetch_cnn_market_snapshot()

    critical_missing = any(p.current is None or p.day_high is None or p.day_low is None for p in prices)
    if fg.value is None:
        critical_missing = True

    status = "PASS" if not critical_missing else "NOK"
    subject = f"[{status}] ETF 日报提醒"

    line_time_prefix = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M")

    lines = [
        f"状态: {status}",
        f"时间(北京): {datetime.now(BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "ETF 行情:",
    ]

    lines.append(f"{'类别':<10}{'时间':<18}{'当前':<14}{'当日最高':<14}{'当日最低':<14}")
    for p in prices:
        if p.current is None or p.day_high is None or p.day_low is None:
            lines.append(f"{p.symbol:<10}{line_time_prefix:<18}{'数据缺失':<14}{'-':<14}{'-':<14}")
            if p.error:
                lines.append(f"  说明: {p.error}")
        else:
            lines.append(
                f"{p.symbol:<10}{line_time_prefix:<18}{p.current:<14.4f}{p.day_high:<14.4f}{p.day_low:<14.4f}"
            )

    lines.append("")
    lines.append("Fear & Greed:")
    if fg.value is None:
        lines.append(f"{line_time_prefix} - 数据缺失，原因: {fg.error}")
    else:
        rating_text = f" ({fg.rating})" if fg.rating else ""
        lines.append(f"{line_time_prefix} - 指数: {fg.value:.1f}{rating_text}")

    lines.append("")
    lines.append("CNN Markets 快照:")
    if market.error:
        lines.append(f"{line_time_prefix} - 数据缺失，原因: {market.error}")
    else:
        if market.sp500_value and market.sp500_change:
            lines.append(f"{line_time_prefix} - S&P 500: {market.sp500_value} {market.sp500_change}")
        else:
            lines.append(f"{line_time_prefix} - S&P 500: 数据缺失")
        if market.nasdaq_value and market.nasdaq_change:
            lines.append(f"{line_time_prefix} - NASDAQ: {market.nasdaq_value} {market.nasdaq_change}")
        else:
            lines.append(f"{line_time_prefix} - NASDAQ: 数据缺失")

    body = "\n".join(lines)
    return subject, body


def send_email(cfg: SmtpConfig, to_email: str, subject: str, body: str) -> None:
    """通过 SMTP 发送邮件。"""

    msg = MIMEText(body, _charset="utf-8")
    msg["Subject"] = subject
    msg["From"] = cfg.from_email
    msg["To"] = to_email

    log(f"发送邮件到 {to_email}...")
    with smtplib.SMTP(cfg.host, cfg.port, timeout=15) as server:
        if cfg.use_tls:
            # 这里使用 STARTTLS；若服务商要求 SSL 直连，可扩展为 SMTP_SSL
            server.starttls()
        server.login(cfg.username, cfg.password)
        server.sendmail(cfg.from_email, [to_email], msg.as_string())
    log("邮件发送完成")


def is_weekday_beijing() -> bool:
    """是否为北京时间周一到周五。"""

    return datetime.now(BEIJING_TZ).weekday() < 5


def print_email_preview(subject: str, body: str) -> None:
    """在控制台打印即将发送的邮件内容。"""

    log("即将发送的邮件内容如下：")
    print("-" * 60)
    print(f"Subject: {subject}")
    print(body)
    print("-" * 60)


def run_once(cfg: Optional[SmtpConfig], email_to: Optional[str]) -> int:
    """执行一次抓取+发信。

    发信失败时打印邮件内容预览，便于在无 SMTP 环境下排查。
    """

    subject, body = build_report()
    # 按需求：发邮件前，先把邮件内容打印给用户
    print_email_preview(subject, body)

    # 按需求：若未提供 --config 或 --emailto，则跳过发信流程
    if cfg is None or not email_to:
        log("未提供 --config 与 --emailto 的完整参数，跳过发邮件环节")
        return 0

    try:
        send_email(cfg, email_to, subject, body)
        return 0
    except Exception as exc:
        log(f"邮件发送失败: {exc}")
        return 1


def schedule_loop(remind_hhmm: str, cfg: Optional[SmtpConfig], email_to: Optional[str]) -> int:
    """定时循环：每天到指定 HH:MM 时触发一次（仅工作日）。"""

    log(f"进入定时模式，提醒时间(北京): {remind_hhmm}，仅周一至周五触发")
    # 记录上次触发日期，防止同一天重复触发
    last_trigger_date: Optional[str] = None

    while True:
        now = datetime.now(BEIJING_TZ)
        date_key = now.strftime("%Y-%m-%d")
        now_hhmm = now.strftime("%H:%M")

        if now_hhmm == remind_hhmm and date_key != last_trigger_date:
            if is_weekday_beijing():
                log("到达提醒时间，开始执行...")
                run_once(cfg, email_to)
                last_trigger_date = date_key
            else:
                log("到达提醒时间但今天是周末，跳过")
                last_trigger_date = date_key

        # 轮询间隔 30 秒，降低 CPU 占用
        time.sleep(30)


def parse_args() -> argparse.Namespace:
    """解析命令行参数。"""

    # 使用 conflict_handler='resolve'，即使文件被误合并导致重复定义同名参数，
    # 也不会在 --help 阶段直接崩溃（后定义会覆盖先定义）。
    parser = argparse.ArgumentParser(description="ETF 邮件提醒脚本", conflict_handler="resolve")

    def add_or_replace_argument(*names: str, **kwargs: object) -> None:
        """防御性添加参数：若同名参数已存在则先移除再添加。"""

        option_names = {name for name in names if isinstance(name, str) and name.startswith("-")}
        if option_names:
            for action in list(parser._actions):
                if option_names.intersection(set(action.option_strings)):
                    parser._remove_action(action)
                    for option in action.option_strings:
                        parser._option_string_actions.pop(option, None)
        parser.add_argument(*names, **kwargs)

    add_or_replace_argument("--remind", default="09:30", help="北京时间提醒时间，格式 HH:MM")
    add_or_replace_argument("--emailto", help="收件人邮箱；不传则跳过发邮件")
    add_or_replace_argument("--config", help="SMTP 配置文件路径；不传则跳过发邮件")
    add_or_replace_argument("--once", action="store_true", help="立即执行一次并退出")
    return parser.parse_args()


def validate_hhmm(value: str) -> None:
    """校验 --remind 时间格式必须为 HH:MM。"""

    try:
        time.strptime(value, "%H:%M")
    except ValueError as exc:
        raise ValueError("--remind 必须是 HH:MM 格式") from exc


def main() -> int:
    """程序入口。"""

    args = parse_args()
    validate_hhmm(args.remind)

    cfg: Optional[SmtpConfig] = None
    if args.config and args.emailto:
        try:
            cfg = read_config(args.config)
        except Exception as exc:
            # 配置错误是不可恢复问题：直接退出并返回非 0
            log(f"配置错误: {exc}")
            return 2
    else:
        log("未传入完整的 --config 和 --emailto，程序将仅打印报告并跳过发邮件")

    if args.once:
        return run_once(cfg, args.emailto)
    return schedule_loop(args.remind, cfg, args.emailto)


if __name__ == "__main__":
    sys.exit(main())

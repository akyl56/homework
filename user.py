#!/usr/bin/env python3
"""ETF 邮件提醒脚本。

功能概览：
1) 抓取 ETF（CSPX.GB / CSNDX.CH）行情（当前价、当日最高、当日最低）
2) 抓取 CNN Fear & Greed 指标
3) 根据抓取结果生成 PASS / NOK 邮件并通过 SMTP 发送
4) 支持 --once 单次执行和按北京时间工作日定时执行
"""

import argparse
import configparser
import json
import smtplib
import sys
import time
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


def http_get_json(url: str) -> dict:
    """发送 GET 请求并解析 JSON。

    - 使用统一 User-Agent
    - timeout=10
    - 失败后指数退避重试（1s, 2s）
    """

    last_error: Optional[Exception] = None
    for attempt in range(RETRY_COUNT + 1):
        try:
            req = Request(url, headers={"User-Agent": USER_AGENT})
            with urlopen(req, timeout=REQUEST_TIMEOUT) as response:
                return json.loads(response.read().decode("utf-8"))
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_error = exc
            if attempt < RETRY_COUNT:
                delay = 2 ** attempt
                log(f"请求失败，{delay}s 后重试: {exc}")
                time.sleep(delay)
    raise RuntimeError(f"请求失败: {last_error}")


def http_get_text(url: str) -> str:
    """发送 GET 请求并返回文本。

    逻辑同 `http_get_json`，只是不做 JSON 解析。
    """

    last_error: Optional[Exception] = None
    for attempt in range(RETRY_COUNT + 1):
        try:
            req = Request(url, headers={"User-Agent": USER_AGENT})
            with urlopen(req, timeout=REQUEST_TIMEOUT) as response:
                return response.read().decode("utf-8")
        except (HTTPError, URLError, TimeoutError, UnicodeDecodeError) as exc:
            last_error = exc
            if attempt < RETRY_COUNT:
                delay = 2 ** attempt
                log(f"请求失败，{delay}s 后重试: {exc}")
                time.sleep(delay)
    raise RuntimeError(f"请求失败: {last_error}")


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


def fetch_fear_greed() -> FearGreedResult:
    """抓取 CNN Fear & Greed 指标。

    失败时返回带 error 的结果，不抛异常，保证主流程可继续。
    """

    url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
    try:
        log("抓取 CNN Fear & Greed...")
        data = http_get_json(url)
        now_data = data.get("fear_and_greed", {}).get("now", {})
        value = now_data.get("value")
        rating = now_data.get("valueText") or now_data.get("rating")
        if value is None:
            raise RuntimeError("Fear & Greed 字段缺失")
        return FearGreedResult(float(value), str(rating) if rating else None, None)
    except Exception as exc:
        error = f"CNN Fear & Greed 抓取失败: {exc}"
        log(error)
        return FearGreedResult(None, None, error)


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

    critical_missing = any(p.current is None or p.day_high is None or p.day_low is None for p in prices)
    if fg.value is None:
        critical_missing = True

    status = "PASS" if not critical_missing else "NOK"
    subject = f"[{status}] ETF 日报提醒"

    lines = [
        f"状态: {status}",
        f"时间(北京): {datetime.now(BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "ETF 行情:",
    ]

    for p in prices:
        if p.current is None:
            lines.append(f"- {p.symbol}: 数据缺失，原因: {p.error}")
        else:
            lines.append(
                f"- {p.symbol}: 当前 {p.current:.4f}, 当日最高 {p.day_high:.4f}, 当日最低 {p.day_low:.4f} (来源: {p.source})"
            )

    lines.append("")
    lines.append("Fear & Greed:")
    if fg.value is None:
        lines.append(f"- 数据缺失，原因: {fg.error}")
    else:
        rating_text = f" ({fg.rating})" if fg.rating else ""
        lines.append(f"- 指数: {fg.value:.1f}{rating_text}")

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

    parser = argparse.ArgumentParser(description="ETF 邮件提醒脚本")
    parser.add_argument("--remind", default="09:30", help="北京时间提醒时间，格式 HH:MM")
    parser.add_argument("--emailto", help="收件人邮箱；不传则跳过发邮件")
    parser.add_argument("--config", help="SMTP 配置文件路径；不传则跳过发邮件")
    parser.add_argument("--once", action="store_true", help="立即执行一次并退出")
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

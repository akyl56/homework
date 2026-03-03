# ETF 邮件提醒脚本

本项目提供 `user.py`，用于在北京时间工作日定时抓取以下数据并发邮件提醒：

- ETF：`CSPX.GB`（LSE）、`CSNDX.CH`（SWX）、`SMH.GB`、`IWY.US` 与 `COWZ.NL` 的当日最高/最低/当前价
- CNN Fear & Greed 指标

> 说明：若网络受限或接口异常，脚本会优雅降级，邮件状态为 `NOK` 并附缺失原因。

## 1. 安装依赖

脚本仅使用 Python 标准库，通常无需额外安装第三方包。

建议环境：Python 3.9+（需要 `zoneinfo` 时区支持）。

```bash
python3 --version
```

## 2. 配置 SMTP

1. 复制模板：

```bash
cp config.example.ini config.ini
```

2. 编辑 `config.ini`，填写真实 SMTP 参数（不要把此文件提交到仓库）。

模板字段说明：

- `host`: SMTP 服务器地址
- `port`: SMTP 端口（587/465 等）
- `username`: 登录用户名（一般是邮箱）
- `password`: SMTP 授权码或密码
- `from_email`: 发件人邮箱（可选，默认同 `username`）
- `use_tls`: 是否使用 STARTTLS（`true/false`）

## 3. 参数说明

```bash
python3 user.py --help
```

支持参数：

- `--remind "HH:MM"`：北京时间触发时间（仅周一到周五执行）
- `--emailto "xxx@xx.com"`：收件人邮箱
- `--config "path/to/config.ini"`：SMTP 配置路径
- `--once`：立即执行一次并退出（不进入循环）

## 4. 单次运行示例（--once）

```bash
python3 user.py --once --config config.ini --emailto "your_target@example.com"
```

## 5. 定时运行说明

每天北京时间 09:30（工作日）检查并发送：

```bash
python3 user.py --remind "09:30" --config config.ini --emailto "your_target@example.com"
```

程序会常驻运行，每 30 秒轮询一次时间；在同一天内只触发一次。

## 6. 常见报错与处理

1. **配置错误: 无法读取配置文件 / 缺少字段**
   - 检查 `--config` 路径是否正确
   - 检查 `[smtp]` 段与 `host/port/username/password` 是否存在

2. **请求失败（ETF 或 Fear & Greed）**
   - 可能是网络限制、目标站点临时不可用或返回格式变化
   - 程序会重试 2 次（指数退避），并在邮件正文中写明失败原因

3. **邮件发送失败（如 Name or service not known / Authentication failed）**
   - 校验 SMTP 地址、端口与授权码
   - 检查邮箱服务商是否需要开启 SMTP/应用专用密码
   - 即使发送失败，程序也会在控制台打印邮件内容预览，便于排查

## 7. 安全建议

- `config.ini`、`.env` 等敏感配置已在 `.gitignore` 中忽略
- 不要在仓库中提交真实密码或授权码

# OpenAI 注册密码阶段出口验证结论（2026-04-06）

## 背景

本轮排查目标是确认：

1. `_register_password()` 在代码层面是否仍缺少关键请求上下文。
2. 之前 `POST https://auth.openai.com/api/accounts/user/register` 返回 `400 Failed to create account. Please try again.`，是否主要由旧静态代理出口导致。
3. 在关闭静态代理并重启应用后，直连出口下结果是否会改变。

## 已确认的前置事实

### 1. 密码提交请求形态已补齐

运行时日志已证明，密码提交请求包含以下关键上下文：

- `origin: https://auth.openai.com`
- `referer: https://auth.openai.com/create-account/password`
- `accept: application/json`
- `content-type: application/json`
- `x-requested-with: XMLHttpRequest`
- `openai-sentinel-token: present`

同时流程能够正常到达：

- `响应页面类型: create_account_password`

因此，问题已不再像是“缺少 XHR / Sentinel / 密码页预热”这类明显请求形态缺陷。

### 2. 数据库中的静态代理已关闭

当时数据库中的代理配置为：

- `proxy.enabled=false`
- `proxy.dynamic_enabled=false`
- 静态代理 host/port 虽仍保留，但已不应被使用

应用随后已完成重启，避免旧 settings 缓存继续生效。

## 直连验证样本

### 重启后关键日志

应用重启完成：

- `src.web.app` 启动时间：`2026-04-06 17:06:15`

之后出现新的注册样本：

- `app.log:115668` 批量任务初始化
- `app.log:115669` `注册代理决策: 跳过冷却中的代理列表出口: http://172.21.0.1:7890`
- `app.log:115670` `动态代理未启用或未配置 API，enabled=False api_url=-`
- `app.log:115671` `未获取到任何代理，将直接使用当前服务器出口`

这三行可以作为**本轮确实为直连出口**的直接证据。

### 直连下的注册过程

前序流程正常：

- `app.log:115700` `提交注册表单状态: 200`
- `app.log:115701` `响应页面类型: create_account_password`

密码提交第一次失败：

- `app.log:115706` `提交密码状态: 400`
- `app.log:115709` 错误信息：`Failed to create account. Please try again.`

诊断信息中最关键字段：

- `app.log:115715`
  - `"proxy_url": ""`
  - `x-requested-with: XMLHttpRequest`
  - `openai-sentinel-token: present`

这里的 `proxy_url` 为空，说明这次失败不是经由旧静态代理发生的，而是**直连状态**下发生的。

### 重试后的结果

第一次 `400` 后，代码按既有逻辑做了一次退避重试。

重试结果：

- `app.log:115723` `提交密码状态: 409`
- `app.log:115726` `Invalid session. Please start over.`
- `app.log:115729` `code: invalid_state`

这与此前代理出口下的表现一致：

- 第一次在密码建号口返回 `400 Failed to create account`
- 继续重试则容易退化为 `409 invalid_state`

## 最终结论

### 已基本排除

1. **静态代理缓存/旧代理出口导致本次失败**
   - 已排除。
   - 因为重启后新样本已明确走直连，且结果仍然相同。

2. **_register_password() 缺少明显关键请求头/上下文**
   - 已基本排除。
   - 运行时已证明请求具备 XHR、Sentinel token、正确 referer，并成功进入 `create_account_password` 页面。

3. **问题仅由 `http://172.21.0.1:7890` 这个静态代理出口引起**
   - 已基本排除。
   - 因为不经过该出口时，仍在同一阶段失败。

### 当前更可能的方向

更像是以下一类服务端拒绝，而不是简单代码拼装错误：

- 当前服务器公网出口/IP 整体信誉或风控问题
- 当前运行环境指纹被 OpenAI 风控拒绝
- 邮箱域名、会话环境、设备指纹、Cloudflare/OpenAI 联合风控等组合因素触发拒绝
- 密码建号口本身对当前环境直接拒绝，而非前置流程缺字段

## 对后续排查的意义

后续如果继续看日志，应优先关注以下几类问题，而不是再重复怀疑“是否少了某个 header”：

1. 是否存在新的环境差异
   - 公网 IP
   - 会话 cookie 结构变化
   - Cloudflare / OpenAI 返回头变化

2. 是否存在与邮箱域名相关的模式
   - 某些域名在密码提交阶段被更高概率拒绝

3. 是否存在环境/设备指纹层面的风控特征
   - 即使流程进入 `create_account_password`，仍在 `user/register` 口被拒

4. 是否存在“第一次 400，重试必然 409”的固定模式
   - 该模式说明第一次失败后会话很容易被服务端判定为失效态

## 建议后续重点查阅的日志位置

本轮最关键的日志行：

- `app.log:115669`
- `app.log:115670`
- `app.log:115671`
- `app.log:115700`
- `app.log:115701`
- `app.log:115706`
- `app.log:115715`
- `app.log:115723`
- `app.log:115732`

## 相关文件与定位入口

后续继续排查时，可优先从以下文件和位置进入：

### 日志文件

- `/root/codex-console/logs/app.log`

### 核心代码位置

- `src/core/register.py:956` `_collect_register_failure_diagnostics()`
- `src/core/register.py:1217` `_check_sentinel()`
- `src/core/register.py:1482` `_submit_signup_form()`
- `src/core/register.py:3117` `_register_password()`
- `src/core/register.py:3678` `_create_user_account()`

### 本轮最关键日志位置

- `app.log:115669`
- `app.log:115670`
- `app.log:115671`
- `app.log:115700`
- `app.log:115701`
- `app.log:115706`
- `app.log:115715`
- `app.log:115723`
- `app.log:115732`

## 一句话摘要

**2026-04-06 的出口切换验证已经证明：即使完全直连，OpenAI 注册流程仍然在密码提交阶段返回 `400 Failed to create account`，随后重试退化为 `409 invalid_state`；因此问题已基本不是静态代理缓存或明显请求头缺失，而更像是服务端风控/环境拒绝。**

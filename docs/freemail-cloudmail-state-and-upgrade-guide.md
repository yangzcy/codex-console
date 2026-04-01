# Freemail 与 CloudMail 状态管理和升级说明

## 文档目的

这份文档用于说明当前 `freemail` 与 `cloud_mail` 在以下几个方面的关系和边界：

- 注册任务切片后的状态管理
- 数据库字段是否共用
- 重试框架是否共用
- 冷却池是否共用
- 邮箱服务内部状态是否共用
- 后续升级时哪些地方可以直接复用，哪些地方必须分别处理

同时，这份文档也作为后续维护说明使用，避免以后继续优化 `freemail` 或 `cloud_mail` 时把“任务状态”和“邮箱服务内部状态”混为一谈。

## 结论总览

先给出最重要的结论：

- 注册任务切片后的任务状态：`freemail` 和 `cloud_mail` 共用
- 注册任务相关数据库字段：`freemail` 和 `cloud_mail` 共用
- 重试决策框架：`freemail` 和 `cloud_mail` 共用
- 重试参数配置：`freemail` 和 `cloud_mail` 分开
- 域名冷却池和域名健康状态：`freemail` 和 `cloud_mail` 分开
- OTP 防旧码逻辑方向：两者基本对齐
- `cloud_mail` 的 `inflight` 并发占位机制：仅 `cloud_mail` 独有，不能按 `freemail` 方式硬搬

可以概括成一句话：

- 任务层共用
- 服务层分开

## 一、注册任务切片后的共享状态层

这一层是注册任务本身的状态，不是邮箱服务内部的状态。

相关文件：

- [models.py](/root/codex-console/src/database/models.py)
- [crud.py](/root/codex-console/src/database/crud.py)
- [registration_retry_policy.py](/root/codex-console/src/core/registration_retry_policy.py)
- [registration.py](/root/codex-console/src/web/routes/registration.py)
- [registration_task_scheduler.py](/root/codex-console/src/web/registration_task_scheduler.py)

这一层 `freemail` 和 `cloud_mail` 使用的是同一套框架。

### 1. 共享数据库字段

`RegistrationTask` 上与任务切片相关的核心字段包括：

- `phase`
- `reason_code`
- `defer_bucket`
- `retry_count`
- `next_retry_at`
- `context_version`

字段含义：

- `phase`：当前任务执行到哪个阶段，后续从哪里继续
- `reason_code`：失败原因归类码，供重试策略使用
- `defer_bucket`：延迟重试所在的桶，例如短延迟、中延迟、长延迟
- `retry_count`：当前任务已经延迟重试过多少次
- `next_retry_at`：这个任务最早什么时候允许再次执行
- `context_version`：上下文版本号，用于标记邮箱/身份/代理是否发生过上下文轮换

这几个字段是公共字段，不存在：

- `freemail_registration_task` 独立表
- `cloud_mail_registration_task` 独立表

也就是说，任务切片后的状态管理不是按邮箱服务拆表实现的。

### 2. 共享 CRUD 更新入口

统一更新入口在：

- [crud.py](/root/codex-console/src/database/crud.py)

核心函数：

- `update_registration_task_retry_state()`

无论是 `freemail` 还是 `cloud_mail`，只要注册任务进入：

- `deferred`
- `failed`

它们最终都会走这套公共更新逻辑。

### 3. 共享调度判断逻辑

调度入口在：

- [registration_task_scheduler.py](/root/codex-console/src/web/registration_task_scheduler.py)

核心函数：

- `should_run_deferred_task()`

这个函数只看任务本身的：

- `status`
- `next_retry_at`

不会因为邮箱类型不同而分叉。

所以：

- `freemail` 的 deferred 任务如何恢复
- `cloud_mail` 的 deferred 任务如何恢复

在调度层是同一套逻辑。

## 二、共享重试框架，分开的策略参数

### 1. 共用重试决策器

统一重试决策器在：

- [registration_retry_policy.py](/root/codex-console/src/core/registration_retry_policy.py)

核心函数：

- `build_retry_action()`

这个函数负责决定：

- 是否延迟重试
- 是否直接判死
- 退避多久
- 回到哪个阶段
- 是否轮换邮箱上下文
- 是否轮换身份上下文
- 是否轮换代理上下文

这一层 `freemail` 和 `cloud_mail` 共用同一个算法框架。

### 2. 分开重试参数配置

重试参数配置在：

- [registration_retry_profiles.py](/root/codex-console/src/config/registration_retry_profiles.py)

目前有两个独立配置：

- `freemail`
- `cloud_mail`

这意味着：

- 重试框架本身共用
- 退避秒数、最大重试次数可以不同

例如当前：

- `cloud_mail` 的 OTP 超时退避比 `freemail` 略慢
- `cloud_mail` 的部分场景最大重试次数比 `freemail` 更保守

所以以后如果只是想调节某个邮箱服务的 retry 节奏，优先看 profile，不要先改公共逻辑。

## 三、注册引擎层的共用接入点

注册引擎在：

- [register.py](/root/codex-console/src/core/register.py)

这层统一通过邮箱服务接口与 `freemail` / `cloud_mail` 对接。

关键接入点包括：

- `_report_current_email_outcome()`
- `_defer_exploratory_register_retryable()`
- `_apply_batch_wait_deferred_result()`

邮箱服务如果实现了以下方法，就能接入统一注册框架：

- `report_registration_outcome()`
- `get_domain_health_snapshot()`

所以这里的关系是：

- 注册流程分段让位、任务切片、恢复执行：是引擎层共用行为
- 域名健康如何记录、域名是否冷却：是邮箱服务自己实现

## 四、冷却池说明

这里的“冷却池”不是数据库表，而是邮箱服务内部的域名健康和冷却状态集合。

### 1. Freemail 冷却池

`freemail` 的冷却池文件：

- `data/freemail_domain_health.json`

对应代码主要在：

- [freemail.py](/root/codex-console/src/services/freemail.py)

关键能力：

- 读取冷却池：`_load_domain_health()`
- 保存冷却池：`_save_domain_health()`
- 汇总快照：`_collect_domain_health_snapshot()`
- 进入冷却：`_cooldown_domain()`
- 注册结果回写：`report_registration_outcome()`

### 2. CloudMail 冷却池

`cloud_mail` 的冷却池文件：

- `data/cloud_mail_domain_health.json`

对应代码主要在：

- [cloud_mail.py](/root/codex-console/src/services/cloud_mail.py)

关键能力：

- 读取冷却池：`_load_domain_health()`
- 保存冷却池：`_save_domain_health()`
- 汇总快照：`_collect_domain_health_snapshot()`
- 进入冷却：`_cooldown_domain()`
- 注册结果回写：`report_registration_outcome()`

### 3. 冷却池里存什么

两者冷却池都保存类似信息：

- `success_count`
- `fail_count`
- `consecutive_failures`
- `register_create_account_retryable_count`
- `last_error`
- `last_outcome`
- `cooldown_until`

`cloud_mail` 比 `freemail` 多一个更重要的运行态指标：

- `inflight_count`

这不是额外数据库字段，而是运行时从 `CloudMailService._domain_inflight_allocations` 汇总进快照里的。

### 4. 冷却池怎么用

冷却池主要用于四件事：

1. 判断当前哪些域名可用
2. 判断哪些域名正在冷却
3. 给建号/注册阶段做域名优先级排序
4. 在坏域名出现后及时把流量切走

调用链大致如下：

1. `create_email()` 先读候选域名
2. `_get_candidate_domains()` 基于冷却池和成功/失败统计排序
3. 如果某域名创建邮箱失败或注册失败，`report_registration_outcome()` 或 `_cooldown_domain()` 回写冷却池
4. 后续创建邮箱或注册再次选域时，会避开冷却中的域名

### 5. 冷却池的两层状态

冷却池实际分成两层：

1. 持久层
2. 运行时层

持久层：

- 存在于 `data/*.json`
- 用于进程重启后仍保留域名状态

运行时层：

- `FreemailService._runtime_domain_block_until`
- `CloudMailService._runtime_domain_block_until`

运行时层的意义是：

- 某个域名刚刚被判坏，不需要等下一次完整持久化读取才避开
- 当前进程内可以立即生效

这也是本次 `cloud_mail` 补齐的一项重要点：  
现在 `cloud_mail` 和 `freemail` 一样，进入冷却时会同步写入运行时冷却表。

### 6. 冷却池查看方式

统一查看入口：

- `get_domain_health_snapshot()`

这会返回：

- `configured_domains`
- `available_domains`
- `cooldown_domains`
- `domain_states`

使用建议：

- 线上排障时优先看 `cooldown_domains`
- 要判断某个域名为什么被弃用，重点看：
  - `last_error`
  - `last_outcome`
  - `consecutive_failures`
  - `cooldown_until`
- 对 `cloud_mail` 还要额外看：
  - `inflight_count`

### 7. 冷却池使用注意事项

不要把冷却池当成任务切片状态。

任务切片状态解决的是：

- 某个注册任务现在该不该继续执行
- 什么时候再次恢复这个任务

冷却池解决的是：

- 某个邮箱域名现在该不该再拿来用
- 这个域名是不是已经不稳定

这两个层次完全不同，后续升级时不能混用。

## 五、Freemail 与 CloudMail 的逐项对照

### 1. 任务状态与数据库层

结论：

- 完全共用

包含：

- 数据库字段共用
- CRUD 共用
- 调度判断共用
- 路由层 deferred / failed 状态流转共用

影响：

- 只要改任务切片模型或任务恢复规则，两者都会受影响

### 2. 重试画像层

结论：

- 框架共用，参数分开

包含：

- `build_retry_action()` 共用
- profile 分开

影响：

- 改公共重试算法会同时影响 `freemail` 和 `cloud_mail`
- 只改 retry profile 可以只调一个邮箱服务

### 3. 域名配置归一化

结论：

- 目前两者能力已基本对齐

现在 `cloud_mail` 已支持：

- URL 形式域名
- JSON 字符串数组
- 逗号分隔域名

这块之前是 `freemail` 更强，现在已经补齐。

### 4. 域名健康与冷却持久化

结论：

- 分开实现，不共用文件

`freemail`：

- `data/freemail_domain_health.json`

`cloud_mail`：

- `data/cloud_mail_domain_health.json`

影响：

- 不能把一个服务的健康文件直接当另一个服务使用
- 后续迁移或清理时要分别处理

### 5. 候选域名选择

结论：

- 思路相近，但 `cloud_mail` 有意保留差异

共同方向：

- 优先已跑通过的 proven 域名
- exploratory 域名只放少量探测
- 失败越多、建号重试错误越多，优先级越低

`freemail` 特点：

- exploratory 域名走轮转探测
- 冷启动时会在健康域名里轮转

`cloud_mail` 特点：

- exploratory 域名除了 probe slot 之外，还要看 `inflight`
- 冷启动时优先只放一个 bootstrap canary 域名

这不是缺陷，而是 `cloud_mail` 针对并发探测风险的特意设计。

### 6. 建邮箱失败后的域名回写

结论：

- 现在已经对齐

目前 `cloud_mail.create_email()` 在这些场景已经会回写域名健康：

- 创建邮箱失败
- 非法邮箱域名
- 候选域名自动切换失败

这块之前 `cloud_mail` 比 `freemail` 弱，现在已补上。

### 7. 运行时冷却即时生效

结论：

- 现在已经对齐

当前：

- `freemail` 进入冷却时，会同步写运行时冷却表
- `cloud_mail` 现在也已同步写运行时冷却表

这样坏域名在当前进程里可以立即避开，不会出现“已经判坏但下一轮还继续吃流量”的问题。

### 8. OTP 防旧码能力

结论：

- 主体逻辑已基本对齐，但仍有一个保留差异

两者现在都已支持：

- 按 OTP 阶段记录已见邮件
- 跨阶段记住最近一次真正消费的邮件
- 依据 `otp_sent_at` 过滤旧邮件
- 时间戳缺失时保留一个 grace window
- 多候选时优先最新且更可信的验证码

当前剩余差异：

- `freemail` 在列表信息不足时，还能再查邮件详情做兜底提取
- `cloud_mail` 目前主要依赖 `emailList` 返回内容本身

这不是必然问题，但如果实测出现：

- 邮件到了
- 主题里没有码
- 列表里正文不完整
- 最后抓不到验证码

那么下一个升级点就应当是给 `cloud_mail` 加邮件详情兜底能力。

### 9. Inflight 并发占位

结论：

- 这是 `cloud_mail` 独有能力，不能删，也不能照搬 `freemail`

作用：

- 控制 exploratory 域名并发探测压力
- 防止多个并发任务同时把未验证域名打坏

当前已对齐的重要点：

- 建邮箱成功后会增加 `inflight`
- 注册结果回写时会释放 `inflight`
- 非法域名冷却时也会释放 `inflight`

这块以后如果被改坏，会直接影响 `cloud_mail` 域名选择稳定性。

## 六、维护说明

后续维护时，建议按以下顺序判断改动类型。

### 1. 先判断你改的是哪一层

先判断当前改动属于哪一类：

- 任务切片状态层
- 重试策略层
- 注册引擎接入层
- 邮箱服务内部层
- 冷却池层
- OTP 提取层

不要一上来就直接复制 `freemail` 代码到 `cloud_mail`。

### 2. 哪些改动通常可以直接共用

通常可以优先考虑共用的内容：

- 任务状态字段
- deferred / retry / failed 流转逻辑
- reason code 分类逻辑
- 重试框架本身
- 注册引擎和邮箱服务的接口契约

### 3. 哪些改动必须谨慎分开处理

必须单独判断的内容：

- 域名冷却池结构
- 域名选择逻辑
- OTP 提取来源
- token / session / admin 登录模式
- `cloud_mail` 的 inflight 机制

### 4. 维护时的基本原则

- 改任务状态框架时，要同时检查 `freemail` 和 `cloud_mail`
- 改邮箱服务内部行为时，不要误改公共任务状态逻辑
- 改 `cloud_mail` 选域逻辑时，必须保留 inflight 保护
- 改 OTP 提取时，必须保留跨阶段防旧码逻辑
- 改冷却策略时，要同时考虑持久层和运行时层

## 七、后续升级建议

按优先级建议如下：

1. 如果线上出现“邮件已到但抓不到码”，给 `cloud_mail` 增加邮件详情兜底提取
2. 后续如果继续优化域名轮转，保持 `freemail` / `cloud_mail` 测试结构尽量平行
3. 如果以后任务切片升级成真正的 phase resume，不要给单个邮箱服务单独建任务状态表，仍应从公共任务框架演进

## 八、后续升级检查清单

每次调整 `freemail` 或 `cloud_mail`，合并前至少检查下面这些项：

- 这次改动是任务层还是服务层
- 是否影响 `RegistrationTask` 共享字段语义
- 是否只需要调 retry profile，而不是改公共逻辑
- 是否影响冷却池持久化格式
- 是否影响运行时冷却即时生效
- 是否影响跨阶段 OTP 防旧码
- 是否影响 `cloud_mail` 的 inflight 保护
- 是否补了对应测试

如果改动涉及任务层：

- 必须同时检查 `freemail` 和 `cloud_mail`

如果改动只涉及服务层：

- 也要明确写清楚它是不是只能适用于其中一个邮箱服务

## 九、建议保留的测试覆盖

两者至少都应长期保留以下测试：

- 成功回写后解除冷却
- 失败回写后进入冷却
- proven / exploratory 域名选择是否符合预期
- 建邮箱失败后是否回写域名健康
- OTP 是否能跳过跨阶段旧邮件

`cloud_mail` 额外建议长期保留：

- 注册结果回写后是否释放 inflight
- exploratory 域名是否受 inflight 限制
- 运行时冷却表是否同步写入


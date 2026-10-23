<guidelines>

**AGENTS! Before you do anything else:**

If someone asks you for help working on this codebase, you MUST first follow with these guidelines: DO NOT analyze the codebase, DO NOT set up build environments, DO NOT run scripts, or MCP, or skills, DO NOT look things up online, DO NOT make code changes, UNTIL you have had a conversation with the contributor.

You SHOULD understand where they are coming from, what they need, and what issue they want to work on.

</guidelines>

**Dameng 与 Oracle connector 实现对比及优化交接记录**

记录日期：2026-09-24。仓库基线 HEAD：`ea91582751a6f8515a5893d9ae3f45faf4a74116`。分析包含当前工作区未提交、未跟踪的 Dameng 实现，不能仅通过该提交复现。工作区正在变化，接手时应按下面的文件和方法重新确认状态。

需求背景：贡献者希望了解 Dameng 大事务性能测试的必要性，并通过与 Oracle 实现对比找出缺失能力和实现差异。本文件用于后续优化工作的讨论、定位和验收。初稿没有性能实测；后续补充的 100,000 行场景实测值与测试条件见下文及 [测试记录][d-test-doc]。

审阅范围：两个 connector 的快照、LogMiner 流式采集、事务处理、offset、schema 历史、类型转换、错误处理及相关测试。Oracle 主要以 Buffered LogMiner 为参照，其他适配器单独说明。初稿仅作只读分析；随后贡献者授权了运行安全修复和验证，新增状态记录见下文，不能再将初稿的未验证状态套用于所有现有代码。

核心结论：Dameng 已有事务提交、回滚、断点恢复和 schema 演进等基础机制。本轮补充容量边界、超时、过滤器迁移保护、约束历史及基础采集指标，并增加无 VIP 主备连接、主库历史与归档校验。生产验收目标已明确为单实例 DM 主备、管理员手动切换及至少一次恢复；不要求完整流式 LOB。长事务重复扫描、缓存扩展、指标分阶段计时和快照对写入的影响仍需后续工作。不能把 Oracle 特有功能全部视为 Dameng 必须实现的功能，也不能把数据库探针成功直接写成 connector 主备验收通过。

**写入时的状态修正**

最初口头分析之后，复核发现工作区已出现以下实现。后续 agent 应先审阅和验证它们，避免照着旧结论重复开发。

| 项目 | 写入时观察到的实现 | 仍需区分或验证的边界 |
| --- | --- | --- |
| 窗口缓存容量保护 | [LogMinerBatch][d-batch] 已有 `log.mining.buffer.max.bytes` 和 `log.mining.buffer.max.records`，默认分别为 512 MiB 估算预算、1,000,000 条输入记录；超限抛错并阻止完成窗口 | 这是整个窗口的累计预算，包括事务标记、回滚记录和 SQL 分片，不是单个事务上限，也不是 JVM 实际堆上限。没有磁盘缓存或超限后继续处理的能力 |
| 事务记录列表复制 | `Transaction.records()` 已返回缓存的只读视图 | 原先的 `List.copyOf(records)` 开销不应继续列为待修复问题 |
| 错误重试分类 | 新增 [DamengErrorHandler][d-errors]，Task 已接入；识别 I/O、可恢复 JDBC 连接异常、临时连接异常和 SQLState `08`，遍历 cause 与 JDBC nextException | 不能再描述为“仅使用公共 ErrorHandler”。仍需验证实际 DM 驱动异常、重试次数、恢复位置，以及永久错误不进入无效重试 |
| 查询与连接超时 | [配置类][d-config] 已增加 socket 超时，流式查询已使用 `readTablePreparedStatement`，连接层已有 statement 超时设置 | 配置存在不等于真实数据库阻塞、断网和停止场景已验证 |
| 快照时长保护 | [快照实现][d-snapshot] 已接入 `DamengSnapshotDeadline` 和 `snapshot.max.duration.ms`，超时中断快照并终止连接以释放锁 | 共享锁仍覆盖整个快照。超时保护没有提供可在业务持续写入时运行的一致性快照方案 |

以上保留初稿的代码观察，其验证状态以下面的追加记录为准；它不表示所有生产边界已完成审查。

**本轮修复及验证状态更新**

初始更新日期：2026-09-24；最终源码本地验证更新：2026-09-28。本节区分实现、已执行的验证和未覆盖的部署边界，不替代各次 Maven 报告。

2026-10-10 测试补充：新增高精度浮点按位核验、38 位 DECIMAL 边界、完整类型 CRUD/NULL 转换及停机重放、历史精度和类型修改、复合主键中间确认恢复、多表事务元数据、固定种子的事务模型、增量快照删除/主键移动/复合键恢复、真实归档丢失和当前 Kafka 版本的故障测试。实现及各次执行结果以 [测试记录][d-test-doc] 为准。新增浮点断言已暴露本地 DM8 流式数值舍入，包含非零小数变成零；另外五种浮点声明的快照 Struct 均保留负零，但 Kafka JsonConverter 往返解码后符号丢失，不能将其归因于数据库快照。原有成功构建与行覆盖率不能作为这些新场景已通过的证据。本轮没有修改生产实现或 Kafka 依赖版本。

本次 clean 全量源码回归 `/tmp/dameng-gap-full.log` 于 2026-10-10 14:09:24 结束：573 单测、2 架构检查通过，136 个数据库 IT 中 109 个通过、26 个浮点一致性断言失败、1 个 REAL 测试输入在 JDBC 写入时越界。越界输入已按达梦官方 REAL 上限 `3.4E38` 修正，后续定向重跑记录见测试文档。其余历史类型、事务、归档缺失、主备、网络恢复和五个 100,000 行大事务场景均通过。基于这次 clean 运行单独执行 JaCoCo 和 Checkstyle，行覆盖率为 2,398/2,467 = 97.2031%，分支覆盖为 1,454/1,641 = 88.6045%，类覆盖 56/56；95% 行门槛和风格检查通过，但全套回归为失败状态。

修正后的完整类型定向回归 `/tmp/dameng-gap-types-final.log` 于 14:14:24 结束：56 项中 30 项通过、26 项断言失败，没有执行错误或跳过。失败分为 21 项原始增量浮点值不一致和 5 项负零 JSON 解码往返不一致。原有两个 Worker 故障测试使用最终夹具重跑，于 14:19:06 `BUILD SUCCESS`；结合 13:49:33 成功的三 broker、双 worker 用例，当前三个进程场景都有通过记录，详细条件和 XML 保存位置见测试文档。没有通过忽略失败或修改期望值取得完整回归通过。

下面的表格和验收段落保留 2026-09-28 的历史记录。2026-10-10 三 broker、双 worker 测试已验证业务分区 leader 故障后的 UPDATE 发布和 task 转移后的类型化 CRUD，不能再将这两项列为完全没有测试；仍未在同一个 Worker 故障场景内同时模拟达梦切主。当前测试结果以测试文档的最新运行记录为准。

| 项目 | 当前实现与验证 | 尚未证明的边界 |
| --- | --- | --- |
| 有限窗口预算 | 默认 512 MiB 累计估算字节、1,000,000 输入行；覆盖所有表、事务标记、已回滚行和 CSF 拼接。超限后 batch 拒绝完成、发布和计算新起点；`DamengRuntimeSafetyIT` 容量失败场景已执行通过。最终源码的 5 个 100,000 行 `DamengLargeTransactionIT` 场景均通过，见 [测试记录][d-test-doc] | 预算是显式失败边界，不是实际 heap 限制；单次有限负载不确定生产吞吐、最大事务规模或持续运行内存上限 |
| 快照与网络超时 | `snapshot.max.duration.ms` 和 `database.socket.timeout.ms` 默认均为 600,000 ms 且必须正数；查询超时非负、正数向上取整为 JDBC 秒。`snapshot.max.threads` 必须为 1。最终回归的 `DamengRuntimeSafetyIT` 共 4 项通过，覆盖预算失败及快照锁等待、背压超时和显式停止后的锁释放 | 共享锁仍覆盖整个快照；对其他 DM/JDBC 版本、持续网络分区的安全中断不能由这一套本地测试推导 |
| 错误清理与网络恢复 | `readBatch()` 保留主要失败，清理阶段次要异常作为 suppressed。最终全量回归含 2 个 `DamengRecoverySafetyIT`，覆盖非 DBA 账号会话终止、启动期间连接拒绝和流式连接中断后的自动恢复 | 不能推广为任意驱动异常或长期网络分区下的安全中断 |
| 历史覆盖与升级 | offset 必须具有 `capture_scope` 签名；任意已签名过滤表达式变更都拒绝复用 offset，包括缩小范围和等价正则改写。旧无签名 offset 也要求新 connector identity、fresh history 和 initial snapshot。`DamengSnapshotFilterIT` 3 项已通过 | 这是一项明确兼容性限制，不是运行中自动扩展历史覆盖；列过滤和 snapshot 行选择不属于该签名 |
| 约束和默认事件 key | 快照从原生 catalog 保存 PK、UNIQUE、FK、CHECK 名称及历史元数据；默认 key 只使用真正的 PRIMARY KEY，不再提升 unique index。最终 Failsafe 日志记录 `DamengConstraintIT` 2 项通过，包括“只有 UNIQUE 后 DROP，事件 key 仍保持 null”及命名约束历史恢复 | 仅证明这些真实场景，未宣称所有 DDL 语法均已覆盖 |
| Dameng JMX | [专用 metrics][d-metrics] 和 factory 已接入 Task/source/coordinator。新增窗口累计预算、输入行、开放事务、replay/scan/archive LSN、容量失败次数及扫描耗时。真实平台 MBean 注册、继承属性、reset、注销和同名重建有单测 | 尚无精确重读放大率、分离的 query/重建/解析/发送耗时及归档等待时间；不能将本轮视为完成全部 P1 性能指标 |
| 真正 Kafka Connect 进程 | 早前 `/tmp/dameng-connect-final-ha.log` 于 2026-09-28 11:20:51 `BUILD SUCCESS`：496 单测、2 架构检查、2 个 `DamengKafkaConnectIT`，当时 ZIP 在主备和快照修复后重新打包。插件隔离装载到 distributed worker，验证 Kafka history/offset、REST stop/resume、离线 DDL/DML 恢复及不重复快照；3,000 行已提交事务在第 9 条发送被测试闸门阻塞时 SIGKILL，重启 worker 后恢复剩余行并继续采集 | 单 broker、单 worker、本地固定 DM 构建；当前覆盖率回归产生的新 ZIP 尚未重跑进程测试。未在 Connect worker 内同时模拟 DM 切主，多 worker rebalance 与 broker 故障仍未验证。发送闸门属于测试专用 JAR，不进入 connector 插件；允许合法重复和中间 offset flush，不承诺 EOS |
| 无 VIP 主备连接及身份 | 已增加 `database.failover.hosts`、OPEN PRIMARY 路由、禁止驱动透明重连和读写分离；offset 保存 permanent magic 与 OPEN 锚点。`DamengDatabaseIdentity` 验证原锚点、前主链、每次晋升的 apply 边界和时间顺序，流式阶段刷新并检测窗口内角色变化；初始快照完成前也验证角色未变化 | 当前 `/tmp/dameng-coverage-final2.log` 中 2 个真实 `DamengPrimaryStandbyIT` 均通过：停机 DDL 积压、手动切换、运行中切回、杀主后普通人工 takeover，以及旧主未完成快照切主后，新主重新发送 500 行并采集后续第 501 行。无 VIP 地址列表全程不变；探针另验证同主重启 |
| 代码覆盖率门禁 | 模块 `dameng-coverage` profile 用 JaCoCo 0.8.15 合并单元、架构和嵌入式 DM 数据库 IT；不排除生产类。`/tmp/dameng-coverage-final2.log` 的完整 clean 构建在 2026-09-28 13:12:16 `BUILD SUCCESS`，`LINE` 2,397/2,467 = 97.1625%，超过 `BUNDLE LINE >= 95%` 门禁；`BRANCH` 1,450/1,641 = 88.3608%，`CLASS` 56/56 = 100%。572 单测、2 架构检查及 78 数据库 IT 全部通过 | 95% 是模块整体行覆盖，非每类覆盖或分支覆盖。Docker Kafka Connect worker 为独立 JVM，没有纳入 JaCoCo；此前 496+2+2 的进程回归使用较早插件构建，新 ZIP 尚未重新跑进程测试 |

JMX 的 `WindowBufferEstimatedBytes` 和 `WindowBufferRecords` 每 1024 输入行及扫描结束时采样；前者是累计估算预留而不是 live heap。`OpenTransactions` 来自最近完成、失败或中断扫描的末次样本。`ScannedUpperLsn` 只表示完成读取的上界，`OldestReplayLsn` 跟随实际 replay position，两者均不代表下游已持久化。扫描时长包含 LogMiner setup、JDBC fetch、事务重建及 cleanup，不包含后续 DML 解析、发送和确认。指标由公共生命周期注册/注销，不另起采集线程。

T1、T2 和 T3 的架构差异没有在本轮消除：仍按窗口重新构建事务、仍由最老未完成事务固定起点、仍没有落盘缓存。保存点回滚重新标记历史行的理由仍成立。本轮没有通过前移起点或丢弃事务换取吞吐。

**主备实现与生产验收边界**

配置 `database.failover.hosts=dm-a:5236,dm-b:5236` 后，无需 VIP，连接地址保持不变。该选项覆盖 `database.hostname` 和 `database.port`；IPv6 使用 `[address]:port`。驱动服务组固定使用 `loginMode=1`、`loginStatus=4`、`rwSeparate=0`、`autoReconnect=0`、`switchTimes=1`、`switchInterval=200`。连接断开后由 connector 按重试策略重新建立任务，并一起恢复 offset 和 schema history；拒绝 URL 绕过、透明重连别名或冲突属性。

切换或接管由 DBA 或独立 DM 管理系统执行，connector 不会升主、修复复制或隔离旧主。当前范围为两个单实例成员，`N_EP=1`、同一 `PERMANENT_MAGIC`、唯一可写主库和 DM 管理的连续归档。管理员必须处理旧主隔离；endpoint 列表不是 split-brain 检测或仲裁服务。

与单机 `DB_MAGIC` 必须相等不同，主备恢复允许成员 magic 变化，但要求 offset 中的 `primary_open_guid` 仍存在于 `SYS.SYSOPENHISTORY`，原 magic 匹配，后继的 `PRIMARY_DB_MAGIC` 链连续，所有中间 OPEN 的 `APPLY_LSN_0` 都覆盖已记录位置。只比较新主当前 LSN 大小会错误接受丢失旧主历史后又向前增长的分支，因此不能代替这些校验。代码校验时间排序下 apply 不倒退，避免时钟偏差将真实晋升放到原锚点之前而跳过检查；每个挖掘窗口刷新身份，发送前 OPEN 锚点改变则重建会话。

DM 的 OPEN 历史只保留 128 条，原锚点被覆盖、时间顺序不明确、前主链断裂或新主重演不足均停止恢复。现有单机 offset 缺少主备锚点时，不能仅追加 failover 配置继续使用；需要新 connector name、topic prefix、schema-history topic 和 initial snapshot。发生真正分叉或丢失时，要先核对源端和下游数据，不能用手改 offset 消除报错。

归档 `DB_MAGIC` 表示本地写档成员，`SRC_DB_MAGIC` 表示产生 redo 的成员；复制到备库的旧主归档可以合法保留不同的 source magic。HA 模式检查整文件范围所属的历史主库区间，拒绝跨分支文件及超出共享端点的重叠，不允许通过裁剪本次窗口隐藏旧尾档。数据库必须管理归档的一致性，不能手工混入旧分支文件。任意人工拼接档案、强制有损 takeover、DMDSC、多个可写主库及其他 DM/JDBC 版本不在当前验收结论之内。

数据库事实依据是官方 [数据守护说明](https://eco.dameng.com/document/dm/zh-cn/pm/data-watch-overview.html)、SYSOPENHISTORY 系统表说明及本地真实主备探针。探针观察到 A→B→B 正常重启→A 的 OPEN 锚点和归档来源连续保留，B 能解析 A 切换前的历史 DDL/DML；普通 takeover 后也保留此前 apply 边界。当前 `/tmp/dameng-coverage-final2.log` 于 2026-09-28 13:12:16 `BUILD SUCCESS`：572 单测、2 架构检查、78 数据库 IT，包含两个真实主备场景和五个 100,000 行大事务场景，均无失败或跳过；Checkstyle、95% 行覆盖率门禁及插件 ZIP/TAR 构建通过。最新 ZIP SHA-256 为 `63b6ce01ed86077d08209e66414c5e2dc8a993887d6d91149e80081a5c279b0b`。之前 `/tmp/dameng-production-final-source.log` 的 496+2+78 和 `/tmp/dameng-connect-final-ha.log` 的 496+2+2 分别是较早的源码回归和独立 Kafka Connect 进程回归；新 ZIP 未重跑后者。中断的 `/tmp/dameng-coverage-final.log` 曾在测试读取 Kafka `FileOffsetBackingStore` 正被重写的文件时遇到短暂 EOF，测试现仅对此类异常作 30 秒有界重试，永久损坏仍失败；该中断日志不能算作通过。完整行外 LOB 不属于本次业务验收要求，相关限制仍应如实保留。

复现完整覆盖率验收：`JAVA_HOME=/opt/java/jdk-25.0.4+7 TESTCONTAINERS_RYUK_CONTAINER_IMAGE=testcontainers/ryuk:0.12.0 ./mvnw -B -ntp -pl debezium-connector-dameng clean verify -Pdameng-coverage,assembly -Ddameng.stress.rows=100000 -Ddameng.stress.payload.bytes=512 -Ddameng.stress.timeout.seconds=600 -Ddameng.stress.mining.batch.size=10000 -Dtest.argline=-Xmx2g`。结果在 `debezium-connector-dameng/target/site/jacoco/{index.html,jacoco.xml,jacoco.csv}`；详细测试场景与单独的进程测试命令见 [测试记录][d-test-doc]。

**大事务处理的关键差异**

| 编号 | 对比点 | Oracle | Dameng 当前实现及影响 |
| --- | --- | --- | --- |
| T1 | 跨批事务状态 | [BufferedLogMinerStreamingChangeEventSource][o-buffered] 跨批保存事务，分别管理挖掘会话起点、读取位置和 offset | [DamengStreamingChangeEventSource][d-stream] 的 `readBatch()` 每轮新建 `LogMinerBatch`，完整读取后才发送已提交事务；之前读过的事务可能再次被读取和重建 |
| T2 | 长事务固定起点 | `calculateNewStartScn()`、`applyWindowMaxAdjustment()` 等提供缓存与窗口推进策略 | `start = offset.getLsn()`；`restartLsn()` 返回最早未完成事务起点。终点向前推进时，整个查询区间可能持续扩大 |
| T3 | 缓存后端 | `createCacheProvider()` 支持 Memory、Ehcache、嵌入式及远程 Infinispan | 只有进程内 Map/List。已有超限失败保护，但没有落盘、外部缓存或可恢复的事务缓存后端 |
| T4 | 超大、超长事务处置 | 有事务事件数阈值、保留时间和事务丢弃机制 | 已有窗口累计容量限制；未见对应的事务保留时间或自动丢弃事务机制。Oracle 的丢弃机制可能造成数据缺失，不能当作无损限流照搬 |
| T5 | 过滤时机 | [BufferedLogMinerQueryBuilder][o-query] 可下推表、schema、用户名、客户端等条件，并保留必要事务标记；行为取决于配置 | [LogMinerSql.contentsQuery()][d-sql] 保留所有表和操作，发送阶段才应用采集过滤。采集范围外的长事务和高流量也会增加扫描、缓存及容量预算消耗 |
| T6 | 会话生命周期 | 根据日志变化、会话时限等判断是否重建挖掘会话，存在复用路径 | `readBatch()` 每轮调用 `startLogMining()`，最终调用 `endLogMining()`；[连接类][d-connection] 为区间收集并添加归档文件。实际开销需分阶段计时 |
| T7 | 对象定位与元数据分配 | [OracleDatabaseSchema.getTableIdByObjectId()][o-schema] 有对象映射缓存和最近一次查找缓存 | `tableId()` 对每条记录遍历历史表；`physicalTable()` 对 DML 重新构建 `OBJ#`、`COL n` 模型。表多、列多或事务大时可能成为 CPU 与对象分配热点 |

示例：假设一个事务从 LSN 100 开始，扫描终点每轮推进 1,000。未结束事务固定起点时，可能依次读取 `[100,1100]`、`[100,2100]`、`[100,3100]`。已发送的其他事务会在发送阶段被去重，但它们仍可能被重新读取和构建。该示例说明重复工作的来源，不是实际压测数据，也不能据此断言所有负载都会呈现同一种复杂度。

`log.mining.batch.size` 只限制扫描终点的推进跨度，不能单独约束实际查询区间或缓存占用。`query.fetch.size` 控制 JDBC 取数批次，事件队列容量控制下游背压；它们都不等价于事务缓存上限。新增 buffer 配置控制的是窗口输入累计预算，达到预算后失败，不会自动把一个大事务拆开继续采集。

重读有正确性原因：[LogMinerBatch][d-batch] 注释说明，DM 保存点回滚可能重新标记此前返回的行。因此不能直接把 `start` 改为 `scannedThrough`，或仅缓存第一次读取的 `ROLL_BACK` 值。跨窗口缓存必须能够反映最终回滚状态。

Oracle 也不是所有模式都只读一次。其读取位置受 LOB、CTE、窗口推进等配置影响，某些路径仍然重读。比较性能时需要记录 Oracle 适配器及配置，不能用“Oracle 完全不重读”作为优化前提。

**采集能力与运行行为差异**

| 编号 | 能力 | Oracle | Dameng 当前状态及影响 |
| --- | --- | --- | --- |
| F1 | 流式 LOB | [AbstractLogMinerStreamingChangeEventSource][o-stream] 有 locator、write、erase 等处理，启用 LOB 后通过 [TransactionCommitConsumer][o-commit] 重组受支持的事件 | 快照读取完整 LOB；流式使用 redo 提供的内联值。[DamengValueConverters][d-values] 将 `OUT_CLOB`、`OUT_BLOB` 转为不可用占位符，[Emitter][d-emitter] 处理遗漏的 LOB 列。没有等价的流式完整值重组路径 |
| F2 | 初始快照一致性 | [快照查询][o-snapshot-query] 使用 `AS OF SCN`；[快照流程][o-snapshot] 在 schema 阶段后释放对应锁 | 普通查询加共享表锁，锁保持到整个快照事务结束，业务写入可能等待。现有 deadline 限制等待时间，但改变不了锁的覆盖范围 |
| F3 | 快照模式 | 支持 `always`、`initial`、`initial_only`、`no_data`、`recovery`、`when_needed`、`configuration_based`、`custom` | [配置类][d-config] 仅支持 `initial`、`initial_only`、`no_data` 和兼容别名 `never`；锁模式只有 `shared` |
| F4 | 增量快照时 schema 变化 | Oracle 接入公共 schema 变化检测路径并提供表 DDL 获取实现 | 已有源信号表增量快照，但显式拒绝 `incremental.snapshot.allow.schema.changes=true`，只允许一个信号表。不能描述为“没有增量快照” |
| F5 | 未采集表的 schema 历史 | 可配置记录采集范围外的表结构和 DDL | 初始历史主要覆盖采集过滤器选中的表；`schema.history.internal.store.only.captured.tables.ddl=false` 不会自动扩大历史范围。现已用 `capture_scope` 签名拒绝过滤表达式改变或无签名旧 offset；要求新 identity、fresh history 和 initial snapshot，防止没有历史定义的表静默漏采 |
| F6 | DDL 覆盖 | 使用 [ANTLR Oracle DDL parser][o-ddl] 和专用 listener | `ddl` 子包拆分词法和完整语句影响范围分析，[DamengDdlParser][d-ddl] 复用单表结构语法并在副本上计算变更，正式模型由 schema history 链路应用。支持常见建表、改列、重命名、删除、清空及已验证的 LONG ROW 存储子句；未知子句与复合 schema/user 操作明确失败。扩展以真实 DM redo 为依据 |
| F7 | 类型映射 | 有 Oracle INTERVAL、时区类型、XML 等专门处理，具体能力依适配器和配置而定 | 普通常见类型已有映射；2026-10-12 补充全部 13 种原生 INTERVAL 的精确字符串映射，复用 Debezium Interval 公共格式，支持 JDBC、历史 DDL、redo 及匹配的常量默认值。复杂对象及 LOCAL TIME ZONE 仍有明确拒绝路径。INTERVAL 不使用平均月天数或整数微秒近似，相关边界与 CRUD 证据见[达梦主手册的数据类型章节](documentation/modules/ROOT/pages/connectors/dameng.adoc#dameng-data-types) |
| F8 | 专用监控指标 | [LogMiner 指标接口][o-metrics] 包含活跃事务数、最老 SCN、查询/解析/提交耗时和吞吐等 | [Task][d-task] 已接入 Dameng metrics factory，并保留公共指标。新增窗口累计预算/输入行及峰值、开放事务数、replay/scan/latest archived LSN、容量失败数和总扫描耗时；重读放大率、细分阶段耗时及归档等待仍缺失 |
| F9 | 事件 source 元数据 | 可提供用户名、redo SQL、row ID、事务开始/提交时间等扩展字段 | [DamengSourceInfoStructMaker][d-source] 主要提供 schema/table、LSN、change LSN、commit LSN、事务 ID 和事件序号。需要根据审计和排障需求决定是否扩展 |

关于 F5：已采集表重命名后离开过滤器的情况有专门处理，会保留后续 DDL 以支持重命名回来。它不等价于保存从未采集过的所有表。当前安全策略不允许复用旧 offset 动态改变 database/schema/table include/exclude、`table.ignore.builtin` 或 `signal.data.collection`；签名比较配置原文，不分析正则语义等价。连接凭据、列过滤和 `snapshot.include.collection.list` 不改变签名。不能直接套用当前数据库结构解释未记录的旧日志。

关于 F6：快照约束历史已补充 catalog 元数据，DDL 对缺失或非正 `DATA_OBJ#` 在变更历史前失败。默认事件 key 只来自声明的主键；仅有 unique constraint/index 的表默认 key 为 null，除非显式配置 `message.key.columns`。这些修复不扩展未支持的 DDL 语法。

关于 F1：SQL 文本的 `CSF` 分片拼接已经实现，但它不等价于行外 LOB 内容重组。若考虑按主键回查 LOB，需要明确读到的是提交时值还是查询时值，并验证后续更新、删除和快照一致性；普通回查不能默认提供历史值。

当前文档还将 blocking snapshot、初始快照并行分块和自定义 snapshotter 列为未实现并验证的能力。公共基类提供部分基础设施，因此接手时需区分“缺少 connector 适配”“继承路径可用但未验证”和“显式不支持”，不要仅以缺少同名类推断完全没有实现。

**DDL 健壮性：词法、影响范围与历史变更分离（2026-10-14）**

现场 `//` 注释后接 `GRANT SELECT` 的失败已纳入回归。借鉴 Oracle 的职责分离，在 `ddl` 子包新增 `DamengSqlLexer` 和 `DamengDdlStatementAnalyzer`，保留现有 `DamengDdlParser` 单表语法。每条流式 DDL 只做一次词法分析，先判定完整输入的影响范围，再执行捕获过滤与历史模型解析。

词法保留原始位置、SQL 和字值内容，区分关键字、引用标识符及字符串；支持 `--`、`//`、普通块注释和提示、双写引号、national string 和 Q 引用。模式策略接受 `BACKSLASH_ESCAPE`、`NESTED_C_STYLE_COMMENT` 的 `0/1` 组合，未知值不假设为默认值。原生 DM 8.1.2.128 中两种反斜杠参数值均保留普通、N、Q 字符串的反斜杠，不套用 MySQL 转义。非嵌套块注释在第一个 `*/` 结束；嵌套模式按层数匹配闭合。闭合后的剩余输入仍按 SQL 分析。可执行注释明确拒绝；不开放 PostgreSQL dollar quoting 或 MySQL hash 注释。保存列默认表达式前仅移除已识别的注释，避免 history 恢复后以错误的注释规则吞掉表达式运算符。词法参数需在保留归档区间内不变，不能凭当前参数重解释旧 DDL。现行支持边界见[达梦主手册的 DDL 章节](documentation/modules/ROOT/pages/connectors/dameng.adoc#dameng-ddl)。

权限、索引、序列、视图、同义词及可确定完整边界的过程/函数/触发器定义不修改采集行结构；完整校验的 `COMMENT ON TABLE/COLUMN` 也接受并跳过，流式字段注释暂不刷新。过程中的分号不按普通 SQL 分割；正文需有可识别的完整 `BEGIN ... END`，复杂声明等未支持形式仍会报错。非表语句后夹带表 DDL 或未知语句不能被首句隐藏。

CREATE/ALTER/DROP/TRUNCATE/RENAME 表语句复用已有语法；未知 ALTER 子句对已跟踪表仍报错。解析先在模型副本上计算变更，成功后由 `DamengDatabaseSchema` 的事件/历史链路应用正式模型，不再先修改共享 `Tables`。识别或解析失败包含 LSN、对象标识及词法/语法位置，保留该条语句前的结构和 offset；不使用当前 JDBC 结构解释历史日志。

保留限制：原始 LogMiner 单条记录最多承载一个结构变更；复合 `CREATE SCHEMA`、`DROP SCHEMA`、`DROP USER`（含级联）明确不支持，不能仅因主表名未知就跳过。非零兼容模式仍需独立策略和验收。多表结构计划、可执行注释、复杂过程声明及流式注释元数据刷新是后续工作。2026-10-15 已补上 offset 参数签名及启动/运行时核对；旧无签名 offset 需新身份和初始快照，检查本身不能重建未记录的历史参数。

三项 P1 另加入统一标识符策略、同表大小写冲突检查，以及有限 CTAS/LIKE、常见存储选项、单层 HASH/RANGE/LIST 分区、内联列 COMMENT 与 DEFAULT ON NULL。CTAS 基于历史源结构；临时/HUGE/外部表及虚拟列因日志不完整或未验收而明确拒绝。CASE_SENSITIVE=0 不开放；IDENTIFIER_MODE=1 和旧库不支持的语法仅有 parser 单测。具体范围、数据库证据与升级影响见词法核对报告。

回归包含词法位置/引用边界、夹带语句、失败不改变历史模型、改表后 DML、停机重放及再次重启恢复。执行结果见 `debezium-connector-dameng/src/test/testing.adoc`；直接语法探针不能替代 Connector 的数据正确性验收。

**需要单独看待的数据库机制差异**

| 项目 | 结论 |
| --- | --- |
| 在线 redo 与归档 | Oracle 路径可使用在线 redo 和归档；当前 Dameng LogMiner 路径从归档读取，事件延迟包含等待归档的时间。不能将数据库归档等待全部计为 Java 处理耗时 |
| 字典策略 | Oracle 有 online catalog、redo catalog、hybrid、dictionary file 等策略。Dameng 刻意不使用在线字典，以历史 schema 解析 `OBJ#`、`COL n`，避免当前结构误解历史日志 |
| 采集适配器 | Oracle 配置有 Buffered/Unbuffered LogMiner、XStream、OLR；Dameng 只有当前 LogMiner 路径。Dameng 虽然定义了 `COMMITTED_DATA_ONLY` 常量，实际流式调用传入 `false`，不代表已具备 Oracle Unbuffered 等价实现 |
| RAC、PDB、XStream | 属于 Oracle 专属机制，不是直接移植目标。DM 单实例主备的连接和身份校验已按官方机制实现；8.1.3.162 指定构建新增主库写信号、备库 LogMiner/快照读取，配置与测试边界以统一手册和测试说明为准。旧版本探针不代表新构建能力；DMDSC 仍不支持 |
| 数据库版本与模式 | 当前 Dameng 文档记录了特定 DM8 构建及 JDBC 版本，启动校验要求 native compatibility 等配置。类型、时间精度和日志格式结论不能未经验证推广到全部 DM 版本 |

**已经实现、优化时必须保留的能力**

| 能力 | 实现和现有测试依据 |
| --- | --- |
| 提交后发送、完整回滚和保存点回滚 | [LogMinerBatch][d-batch]、[LogMinerBatchTest][d-batch-test]、[DamengLargeTransactionIT][d-large-test] |
| 长事务与其他事务提交并存 | 扫描起点保留未完成事务，同时发送已提交事务；已有单元及真实数据库场景 |
| 事务内部断点恢复 | [DamengOffsetContext][d-offset] 保存 replay/change/commit LSN、`mining_transaction_id` 和事件序号；[流式测试][d-stream-test] 覆盖中途恢复及同提交位置事务 |
| 多条消息对应一条源变更的恢复边界 | BEGIN、TRUNCATE 的 schema/data 事件、主键变更的 DELETE/CREATE 等路径保留必要重放语义，不能提前把源语句标为完全已处理 |
| SQL 续接 | 已处理 `CSF` 分片，不完整或交错片段会失败 |
| DDL 历史、重命名和对象身份 | [DamengDatabaseSchema][d-schema]、[过滤边界测试][d-filter-test]、[DamengConnectorIT][d-connector-test] |
| 增量快照 | [DamengIncrementalSnapshotChangeEventSource][d-incremental] 复用公共框架；[增量快照测试][d-incremental-test] 覆盖分块、水位之间的去重和恢复 |
| 事务元数据和事件格式 | 已有 BEGIN/END、表级计数、主键更新、tombstone 等路径和测试，不能因没有 Oracle 同名组件就判为缺失 |
| 归档与数据库身份校验 | [DamengConnection][d-connection] 检查归档覆盖及链路缺口；单机校验 `DB_MAGIC`，主备另外校验 permanent magic、OPEN 历史锚点及 apply 边界、归档来源与完整文件区间，避免跨库或分叉恢复 |

优化验收需要保留提交顺序、事务内语句顺序和同提交位置事务的确定性排序。恢复位置不能越过仍需重建的事务，不能把内存中扫描完成或入队成功直接等同于下游已持久化。具体重复发送边界应遵循当前 offset 协议及至少一次投递语义，不承诺未经验证的 exactly-once。

**建议优先级与验收要求**

优先级表示建议的工作顺序，不是要求一次性实现全部功能。涉及新增配置时，先遵循仓库既有的配置类型、默认值和 Importance 讨论要求。

| 优先级 | 工作项 | 可审阅的完成标准 |
| --- | --- | --- |
| P1 | 补全大事务与扫描指标，建立性能基线 | 已有窗口累计预算、输入行、replay/scan/archive 位置及总扫描耗时；继续补重复读取量或可靠估计、独立输出计数和查询/重建/解析/发送/归档等待计时，统一记录口径 |
| P1 | 验证已有容量、超时和重试保护 | 超限不发布不完整窗口、不推进 replay LSN；永久错误不被默认反复重试；连接异常从可恢复 offset 重启；快照超时和取消后锁释放；相关文档与当前配置一致 |
| P1 | 改善长事务重读和缓存扩展能力 | 在真实保存点回滚、跨归档和中途重启场景下保持结果正确；实测扫描放大、内存及延迟改善。跨批缓存、落盘或其他方案须先说明如何重建最终回滚状态及恢复位置 |
| P1 | 缓存对象 ID 映射和物理列模型 | 按 schema 版本失效或更新缓存，覆盖 ADD/DROP/RENAME/TRUNCATE、同名重建和恢复；多表多列负载下对比 CPU 与分配量，避免缓存错误解释旧日志 |
| P2 | 优化过滤和会话复用 | 保留事务开始、提交、回滚、必要 DDL 及续接信息；验证未采集表长事务与采集表变更交错的情况；真实 DM 环境确认会话复用后的回滚标记和窗口语义 |
| P2 | 降低快照对业务写入的影响 | 先确认 DM 支持的一致性读取机制；同时测量快照一致性、业务写阻塞时间和恢复边界。不能通过直接提前释放锁换取速度 |
| 按业务需求 | LOB、DDL、特殊类型、更多快照模式和元数据 | 对每项明确支持范围，使用真实数据库产生的日志验证，补对应文档；依赖该能力的业务可提高优先级 |

**测试与性能证据要求**

现有 [DamengLargeTransactionIT][d-large-test] 默认 10,000 行、512 字节负载和 300 秒超时，覆盖混合 DML 背压、未提交大事务与其他提交并存、完整回滚、保存点回滚、持久化中间 offset 后恢复。它记录耗时及整个测试 JVM 的采样堆峰值，没有吞吐门槛，不能作为最大事务规模或 connector 独占内存的证明。

| 场景 | 需要确认的结果 |
| --- | --- |
| 同样总行数，小事务与一个大事务对比 | 区分事务规模影响与总数据量影响；观察提交后首条/末条延迟和追平时间 |
| 长事务持续未提交，其他小事务持续提交 | 观察重复扫描范围、其他事务延迟、预算消耗和归档保留需求；确认小事务仍可发送 |
| 采集范围外的大事务 | 量化 T5 的影响；过滤优化不能破坏事务恢复所需信息 |
| 跨窗口、跨归档的提交与回滚 | 保存点回滚后旧行最终标记被正确处理，已回滚内容不发送 |
| 长 SQL、多字节字符与 CSF 续接 | 字节预算、续接完整性和单条极大值行为明确；估算预算不冒充真实堆内存 |
| 下游暂停或变慢 | 观察队列背压与事务缓存的不同占用，恢复消费后顺序和数据完整 |
| BEGIN、事务中间、主键更新或 TRUNCATE 边界重启 | 验证恢复位置和允许的重放范围，防止遗漏后一半事件 |
| 预算边界、数据库断网、查询阻塞、停止任务 | 验证可诊断失败、重试边界、无错误 offset 前移及资源释放 |
| 多表、多列并发 DDL | 验证对象映射和物理 schema 缓存失效，不能只测试没有 DDL 的吞吐 |

性能记录应包括 DM 与 JDBC 版本、JDK、JVM 堆配置、归档策略、采集表数与列数、负载大小、事务提交方式、窗口/预算/队列配置和下游消费速度。至少区分数据库写入及归档时间、采集处理时间和发送背压时间。使用 JFR、GC 日志等证据定位内存和 CPU；测试 JVM 总堆采样不能直接归因于 connector。

执行时使用仓库 Maven wrapper、当前 POM 要求的 JDK 和 `-pl debezium-connector-dameng -am`。代码修改先格式化，开发阶段运行相关单元测试及指定 `-Dit.test` 的集成测试，真实 DM 场景使用模块的 `dameng-tests` profile。当前模块默认跳过 IT，不能把默认构建成功写成已完成集成验证；参数及环境配置以 [模块 POM][d-pom] 和 [文档][d-doc] 为准。

后续交付应逐项记录实际修改、配置兼容性、已执行的测试及结果、性能前后对比和仍未验证的限制。已有未提交改动需要在审阅时保留并辨明来源。

[d-stream]: debezium-connector-dameng/src/main/java/io/debezium/connector/dameng/DamengStreamingChangeEventSource.java
[d-batch]: debezium-connector-dameng/src/main/java/io/debezium/connector/dameng/logminer/LogMinerBatch.java
[d-sql]: debezium-connector-dameng/src/main/java/io/debezium/connector/dameng/logminer/LogMinerSql.java
[d-connection]: debezium-connector-dameng/src/main/java/io/debezium/connector/dameng/DamengConnection.java
[d-config]: debezium-connector-dameng/src/main/java/io/debezium/connector/dameng/DamengConnectorConfig.java
[d-task]: debezium-connector-dameng/src/main/java/io/debezium/connector/dameng/DamengConnectorTask.java
[d-errors]: debezium-connector-dameng/src/main/java/io/debezium/connector/dameng/DamengErrorHandler.java
[d-metrics]: debezium-connector-dameng/src/main/java/io/debezium/connector/dameng/DamengStreamingChangeEventSourceMetrics.java
[d-snapshot]: debezium-connector-dameng/src/main/java/io/debezium/connector/dameng/DamengSnapshotChangeEventSource.java
[d-values]: debezium-connector-dameng/src/main/java/io/debezium/connector/dameng/DamengValueConverters.java
[d-emitter]: debezium-connector-dameng/src/main/java/io/debezium/connector/dameng/DamengChangeRecordEmitter.java
[d-ddl]: debezium-connector-dameng/src/main/java/io/debezium/connector/dameng/DamengDdlParser.java
[d-schema]: debezium-connector-dameng/src/main/java/io/debezium/connector/dameng/DamengDatabaseSchema.java
[d-offset]: debezium-connector-dameng/src/main/java/io/debezium/connector/dameng/DamengOffsetContext.java
[d-source]: debezium-connector-dameng/src/main/java/io/debezium/connector/dameng/DamengSourceInfoStructMaker.java
[d-incremental]: debezium-connector-dameng/src/main/java/io/debezium/connector/dameng/DamengIncrementalSnapshotChangeEventSource.java
[d-doc]: documentation/modules/ROOT/pages/connectors/dameng.adoc
[d-test-doc]: debezium-connector-dameng/src/test/testing.adoc
[d-pom]: debezium-connector-dameng/pom.xml
[d-batch-test]: debezium-connector-dameng/src/test/java/io/debezium/connector/dameng/logminer/LogMinerBatchTest.java
[d-large-test]: debezium-connector-dameng/src/test/java/io/debezium/connector/dameng/DamengLargeTransactionIT.java
[d-stream-test]: debezium-connector-dameng/src/test/java/io/debezium/connector/dameng/DamengStreamingChangeEventSourceTest.java
[d-filter-test]: debezium-connector-dameng/src/test/java/io/debezium/connector/dameng/DamengStreamingFilterBoundaryTest.java
[d-connector-test]: debezium-connector-dameng/src/test/java/io/debezium/connector/dameng/DamengConnectorIT.java
[d-incremental-test]: debezium-connector-dameng/src/test/java/io/debezium/connector/dameng/DamengIncrementalSnapshotIT.java
[o-buffered]: debezium-connector-oracle/src/main/java/io/debezium/connector/oracle/logminer/buffered/BufferedLogMinerStreamingChangeEventSource.java
[o-query]: debezium-connector-oracle/src/main/java/io/debezium/connector/oracle/logminer/buffered/BufferedLogMinerQueryBuilder.java
[o-stream]: debezium-connector-oracle/src/main/java/io/debezium/connector/oracle/logminer/AbstractLogMinerStreamingChangeEventSource.java
[o-commit]: debezium-connector-oracle/src/main/java/io/debezium/connector/oracle/logminer/TransactionCommitConsumer.java
[o-schema]: debezium-connector-oracle/src/main/java/io/debezium/connector/oracle/OracleDatabaseSchema.java
[o-snapshot-query]: debezium-connector-oracle/src/main/java/io/debezium/connector/oracle/snapshot/query/SelectAllSnapshotQuery.java
[o-snapshot]: debezium-connector-oracle/src/main/java/io/debezium/connector/oracle/OracleSnapshotChangeEventSource.java
[o-ddl]: debezium-connector-oracle/src/main/java/io/debezium/connector/oracle/antlr/OracleDdlParser.java
[o-metrics]: debezium-connector-oracle/src/main/java/io/debezium/connector/oracle/logminer/LogMinerStreamingChangeEventSourceMetricsMXBean.java

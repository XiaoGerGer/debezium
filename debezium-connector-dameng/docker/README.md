# Dameng Kafka Connect image

镜像内置 JMX Exporter 1.4.0 和指标映射配置，默认启用 `9404/metrics`，无需挂载 Agent 文件。显式设置 `KAFKA_OPTS` 会覆盖镜像默认值，启用监控时需保留 `-javaagent` 参数。

镜像构建、内网导入、Kafka 2.0 兼容、环境变量、REST 配置、升级及事务落盘说明已统一到：

[达梦 Connector 使用手册：部署与 REST 管理](../../documentation/modules/ROOT/pages/connectors/dameng.adoc#dameng-deployment)

此目录保留 Dockerfile、Worker 配置、启动脚本和环境变量回归脚本；使用说明只在统一手册维护。
监控部署见同一手册的[监控与 Grafana](../../documentation/modules/ROOT/pages/connectors/dameng.adoc#dameng-monitoring)章节。

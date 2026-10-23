# Dameng Connect monitoring

Grafana 面板源文件为本目录的 [dameng-cdc-dashboard.json](dameng-cdc-dashboard.json)，配套的自动加载配置为 [grafana-provider.yaml](grafana-provider.yaml)。统一在此目录维护；外部 Grafana 部署目录中的文件只是部署副本。

Agent 安装、Prometheus 抓取、Grafana 面板、指标解释和规则验证命令已统一到：

[达梦 Connector 使用手册：监控、日志与 Grafana](../../../documentation/modules/ROOT/pages/connectors/dameng.adoc#dameng-monitoring)

面板包含独立的「Incremental Snapshot · 增量快照」区域；新增指标要求同时更新 Connector、Exporter、recording rules 和面板，指标定义与升级步骤见统一手册的「增量快照进度与性能指标」。

此目录保留 Agent 下载校验脚本、Exporter 配置、Prometheus recording rules、Kubernetes PodMonitor/PrometheusRule 样例、规则测试和 Grafana provisioning/JSON 资源；使用说明只在统一手册维护。

# 安全审计与漏洞排查计划

## Goal

对 MobileVC 做一次以真实攻击面为中心的安全审计，找出后端、Flutter 客户端、WebSocket 协议、命令执行、文件下载、推送、依赖和发布流程中的安全风险，并产出可执行的漏洞清单、修复优先级和验证方案。

## What I Already Know

* 项目包含 Go 后端、Flutter 客户端、嵌入式 Web 构建产物、npm launcher 和预编译 server binaries。
* 后端 WebSocket 使用 URL query `token` 鉴权，入口在 `internal/gateway/gateway.go`。
* 当前 WebSocket upgrader `CheckOrigin` 返回 `true`，需要审计跨站 WebSocket 请求风险以及 token 暴露场景。
* `/download` 使用 query `token` 鉴权，并根据 `path` 参数读取本机文件，需要审计任意文件下载、路径边界、符号链接和工作目录约束。
* 后端会执行本地 AI/ADB/系统命令，相关入口集中在 `internal/engine/`、`internal/session/`、`internal/adb/`，需要审计命令注入和权限边界。
* Flutter 客户端保存连接 token、ADB ICE credential、推送 token 等敏感数据，需要审计本地存储和日志泄露。
* iOS/APNs token 当前存在 `debugPrint` 输出，需要确认是否会泄露到生产日志。
* Go 依赖包含 `gorilla/websocket`、`pion/webrtc`、`apns2`、`modernc/sqlite` 等；Flutter 依赖包含 `web_socket_channel`、`mobile_scanner`、`flutter_webrtc` vendor 包；npm 依赖包含 `qrcode-terminal` 和 optional binary packages。
* 当前工作区有未提交构建产物和本地环境文件，安全审计任务不能混入这些无关变更。

## Assumptions

* 审计目标是先产出问题清单和优先级，不在本计划阶段直接修复代码。
* MobileVC 的预期部署形态包括本机、局域网、HTTPS 反代、Flutter Web、APK、iOS。
* 本项目允许本机命令执行是核心功能，因此审计重点不是完全禁止命令执行，而是确保入口、鉴权、日志和用户确认边界清晰。

## Requirements

* 梳理后端 HTTP/WebSocket 暴露面：
  * `/ws`
  * `/download`
  * `/healthz`
  * `/version`
  * 静态 Web 文件服务
* 审计认证和授权：
  * token 生成、传输、保存、日志输出
  * WebSocket query token 的泄露面
  * Origin 校验策略
  * 局域网访问场景下的威胁模型
* 审计文件访问：
  * `/download?path=...`
  * 文件列表和文件读取相关 WebSocket action
  * path traversal、绝对路径、符号链接、工作目录逃逸
* 审计命令执行：
  * shell command 构造
  * Claude/Codex/ADB 命令入口
  * 用户输入进入命令行的位置
  * permission mode 和用户确认流程
* 审计客户端敏感数据：
  * SharedPreferences / 本地配置
  * token、push token、ICE credential 的存储和日志
  * Web、Android、iOS 差异
* 审计依赖漏洞：
  * `govulncheck ./...`
  * `npm audit`
  * Flutter/Dart 依赖版本和 vendor 包风险
  * GitHub Dependabot / advisory 可用性
* 审计发布与构建：
  * npm binary packaging
  * embedded Web 产物
  * 是否把本地环境、token、构建产物误提交
* 产出风险分级：
  * P0: 可远程未授权执行命令/读取文件/接管会话
  * P1: 需要 token 或同网段条件，但可造成敏感数据泄露或命令执行扩大
  * P2: 信息泄露、日志泄露、依赖中危漏洞、配置硬化不足
  * P3: 防御纵深、文档或流程缺口

## Audit Plan

1. 建立攻击面地图
   * 阅读 server 启动、route 注册、WebSocket handler、Flutter 连接配置和 launcher。
   * 输出 HTTP route、WebSocket action、客户端存储、命令执行入口清单。

2. 鉴权与传输安全审计
   * 检查 token 是否足够随机、是否进入 URL、日志、二维码、浏览器历史和 Referer。
   * 检查 WebSocket Origin 策略在 Web/移动端/反代场景下的安全性。
   * 检查 HTTPS/WSS 支持是否覆盖 Web、APK、iOS。

3. 文件系统访问审计
   * 跟踪所有读文件、列目录、下载文件入口。
   * 验证是否允许读取工作目录之外的文件。
   * 设计最小复现用例：`/etc/passwd`、`../../`、符号链接、空路径、目录路径。

4. 命令执行和权限边界审计
   * 跟踪 user input → protocol event → session service → engine runner → shell command。
   * 标记所有直接拼 shell 字符串的位置。
   * 检查 permission prompt 是否可被绕过、误识别或被历史状态污染。

5. 客户端敏感信息审计
   * 检查 Flutter 日志、SharedPreferences、二维码扫描回填、推送 token、ICE credential。
   * 检查 release/debug 差异，确认生产环境不会输出敏感 token。

6. 依赖和供应链审计
   * 运行 Go、npm、Flutter 相关依赖检查。
   * 检查 vendor 包、optional binary packages、构建脚本是否存在供应链或路径注入风险。

7. 发布流程和仓库卫生审计
   * 检查 `.gitignore`、构建产物、嵌入 Web、package lock、FVM、本地 Trellis/agent 文件是否会污染提交。
   * 检查是否存在历史提交中的真实域名、token、证书路径或密钥。

8. 输出报告和修复路线
   * 按 P0/P1/P2/P3 排序。
   * 每个问题包含：影响、复现、证据文件/行号、建议修复、验证命令。
   * 单独列出“确认无问题”的高风险区域，避免重复排查。

## Acceptance Criteria

* [x] 产出安全攻击面地图。
* [x] 产出漏洞/风险清单，按 P0-P3 排序。
* [x] 每个 P0/P1 问题有可复现步骤或明确代码证据。
* [x] 运行并记录依赖漏洞检查结果。
* [x] 明确哪些问题需要代码修复，哪些只需要配置/文档/发布流程调整。
* [x] 不提交构建产物、本地环境文件、真实 token 或线上测试域名。

## Out of Scope

* 本计划阶段不直接修复代码。
* 不做第三方云环境渗透测试。
* 不测试用户真实生产 token、真实 APNs key 或外部服务凭据。
* 不重构核心 session/runner/ws 架构，除非审计结果确认存在 P0/P1 风险并单独立修复任务。

## Technical Notes

* 后端入口：`cmd/server/main.go`
* WebSocket handler：`internal/gateway/gateway.go`
* 命令执行：`internal/engine/`、`internal/session/`
* ADB/WebRTC：`internal/adb/`、`internal/gateway/adb.go`
* Flutter 连接配置：`mobile_vc/lib/core/config/`
* Flutter WebSocket client：`mobile_vc/lib/data/services/mobilevc_ws_service.dart`
* Flutter session controller：`mobile_vc/lib/features/session/session_controller.dart`
* 推送 token：`mobile_vc/lib/app/`、`internal/gateway/push.go`、`internal/push/`
* 依赖清单：`go.mod`、`package.json`、`mobile_vc/pubspec.yaml`
* 安全审计报告：`research/security-audit-report.md`

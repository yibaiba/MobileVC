# 待修复问题清单

> 记录时间：2026-05-29

## 1. 会话接管在切后台后回退到观察模式

**现象：**
- 电脑端同步会话后，iOS 端点了"接管会话"
- 切后台再切回来，会话又回到观察模式
- 需要再次点击会话才能重新接管

**可能原因：**
- `SessionController` 在 `AppLifecycleState` 变化时重新同步会话状态，覆盖了本地的 takeover 状态
- WebSocket 重连时收到了旧的 session state 事件

**需要调查：**
- `session_controller.dart` 中 `didChangeAppLifecycleState` 的逻辑
- WebSocket 重连后是否重新拉取了会话状态
- takeover 状态是本地维护还是后端维护

---

## 2. 接管后发送按钮闪烁（先变停止按钮又回退）

**现象：**
- 接管会话后点击发送
- 蓝色发送按钮短暂变为停止按钮
- 过一会又回退成发送按钮
- 消息可能没有真正发送

**可能原因：**
- 发送时 `isRunning` 状态短暂变为 true（后端确认收到任务），但很快又变回 false
- 可能是后端没有正确接管会话的 PTY，导致任务立即结束
- 或者 WebSocket 收到了冲突的 status 事件

**需要调查：**
- `CommandInputBar` 中发送按钮和停止按钮的状态切换逻辑
- 接管后发送消息的完整流程（Flutter → WebSocket → 后端 → PTY）
- 后端是否真的接管了 Claude 进程

---

## 3. 任务执行时不显示思考过程

**现象：**
- Claude 执行任务时有 thinking/reasoning 内容
- 但 iOS 端没有显示出来

**背景：**
- 2026-05-29 已实现 `ThinkingEvent` 类型并在 `pty_engine.go` 解析 thinking 块
- 已添加 Flutter 端 `ThinkingEvent` 模型、映射和 UI 样式
- 已部署 iOS OTA 1.0.0 (20260529015155)

**可能原因：**
- 用户可能还在使用旧版本 iOS 客户端（需要确认是否已更新）
- 后端需要重启才能生效（已于 02:12 重启）
- 某些 engine 模式（非 claude-pty）可能没有 thinking 输出
- thinking 块可能在 `content` 数组中的位置不同

**需要验证：**
- 确认 iOS 客户端版本是 20260529015155 或更新
- 确认后端进程启动时间在 02:12 之后
- 测试 Claude Opus/Sonnet 模型是否确实返回了 thinking 块
- 检查 `pty_engine.go` 中 thinking 解析是否覆盖了所有情况

---

## 4. 文件内容页面出现多余的白色等待输入元素

**现象：**
- 查看文件内容页面时，偶尔会在输入框上方多出一个白色的"等待输入"元素
- 该元素不应该出现，影响界面整洁度

**可能原因：**
- 文件查看页面的状态判断逻辑有误，错误地显示了等待输入的 UI 组件
- 会话状态同步延迟，导致 UI 误认为当前处于等待输入状态
- 页面切换时状态清理不完整

**需要调查：**
- 文件内容查看页面的 UI 组件结构（哪个 Widget 负责渲染"等待输入"状态）
- 该元素的显示条件判断逻辑
- 会话状态变化时 UI 的更新时机

---

## 5. Claude 引擎下上下文占用显示数值不准

**现象：**
- Claude 引擎下上下文记录显示总是不准
- 有时候发一句话就显示 20%
- 有时候突然跳到 100%
- 有时候执行个任务过程中突然就变 0%

**可能原因：**
- 后端 `pty_engine.go` 中 `emitClaudeContextWindowUsage` 解析的 `usage` 字段值不准确
- Claude API 返回的 `cache_read_input_tokens` / `input_tokens` 计算逻辑有误
- `ContextWindowUsageEvent` 中的 `tokenLimit` 可能被错误设置（不同模型的上下文窗口不同）
- Flutter 端 `_ContextWindowUsageButton` 的百分比计算逻辑有误（`tokensUsed / tokenLimit`）
- 事件流中存在重复或乱序的 `context_window_usage` 事件

**需要调查：**
- 后端 `pty_engine.go` 中 `emitClaudeContextWindowUsage` 的实现逻辑
- Claude API 响应中 usage 字段的实际值和含义
- Flutter 端 `ContextWindowUsageEvent` 的数据结构（`tokensUsed`、`tokenLimit`）
- `_ContextWindowUsageButton` 的百分比计算和更新逻辑
- 是否存在多个引擎（claude-pty vs 其他）导致的数据源不一致

---

## 6. 后台切换后状态球卡在工具调用状态不更新

**现象：**
- 从后台切回来时，除了触发 bug #1（会话回退到观察模式）外
- 即使任务已经完成并返回了结果
- 左下角状态球仍然一直显示 "edit"、"read" 等工具调用状态
- 状态没有正确更新为空闲/等待输入

**可能原因：**
- `SessionController` 在 `AppLifecycleState.resumed` 时重新拉取会话状态，但状态同步逻辑有缺陷
- 工具调用的 `status_update` 事件在后台期间被丢弃或未正确处理
- 会话恢复时没有正确重置 `_currentToolUse` 或类似的状态变量
- WebSocket 重连后收到了过时的状态事件，覆盖了实际的最新状态
- `TimelineItem` 中的工具调用状态没有被后续的完成事件正确清除

**需要调查：**
- `session_controller.dart` 中 `didChangeAppLifecycleState` 对状态变量的处理
- `CommandInputBar` 左下角状态球的渲染逻辑（依赖哪些状态变量）
- 工具调用事件（`tool_use`、`tool_result`）的处理流程
- 会话恢复时是否正确发送了"任务完成"或"等待输入"的状态事件
- 是否存在状态变量在后台切换时没有被重置的竞态条件

---

## 7. 单个表情符号回复不显示，切后台回来才显示

**现象：**
- Claude 回复单个表情符号（emoji）时，消息不会立即显示在聊天界面
- 切换到后台再切回来，表情符号才会显示
- 普通文本回复正常显示

**可能原因：**
- `pty_engine.go` 中对纯 emoji 文本的解析或过滤逻辑有问题
- Flutter 端 `session_display_text.dart` 的噪音过滤器可能将单个 emoji 误判为噪音
- `session_controller.dart` 中 `_handleAssistantMessage` 对短文本的处理逻辑有缺陷
- `event_card.dart` 对纯 emoji 内容的渲染可能触发了某种过滤或跳过逻辑
- WebSocket 事件流中，单个 emoji 可能被打包在后续的 flush 事件中，导致延迟显示

**需要调查：**
- `pty_engine.go` 中 assistant message 的文本过滤逻辑（是否有最小长度限制）
- `session_display_text.dart` 中的噪音过滤器是否包含 emoji 检测
- `session_controller.dart` 中 `_pushTimelineItem` 对短文本的处理
- 测试纯 emoji 回复时 WebSocket 事件流的实际内容
- 检查是否有 `trim()` 或 `isEmpty` 判断导致 emoji 被过滤

---

## 8. 切后台时重复发送"需要继续输入"通知，长任务通知不持续

**现象：**
- 切换到后台时，会连续收到两条"需要继续输入"的通知
- 长任务执行期间的通知推送不稳定
- 只有在切后台的前几秒才能收到最新通知，之后就收不到了

**可能原因：**
- `SessionController` 在 `AppLifecycleState.paused` 时触发了多次"需要继续输入"事件的发送
- 后端 APNs 推送逻辑在 WebSocket 断开后没有正确处理长任务的持续推送
- 通知去重机制缺失，导致相同事件被重复推送
- iOS 后台执行时间限制导致 WebSocket 连接过早断开，错过后续通知
- `pty_engine.go` 中的状态变更事件可能在短时间内触发多次

**需要调查：**
- `session_controller.dart` 中 `didChangeAppLifecycleState` 对 `AppLifecycleState.paused` 的处理逻辑
- 后端 `apns.go` 或推送服务的去重机制
- WebSocket 断开后，后端是否继续向 APNs 推送长任务的状态更新
- iOS 端的后台执行权限和保活机制配置
- 测试长任务（>30秒）期间的通知推送时间线

---

## 9. 连接状态详情下面显示之前发送的提示词

**现象：**
- 在连接状态详情页面下方，会显示之前发送过的提示词内容
- 该提示词不应该出现在这个位置
- 可能是 UI 组件残留或状态未正确清理

**可能原因：**
- 连接状态详情页的 UI 组件复用了消息列表的渲染逻辑，导致历史消息被错误展示
- 页面切换时输入框内容或最后发送的消息没有被清空
- 状态管理中将上一条发送的消息错误地传递到了连接状态页面
- 组件的 `dispose` 或页面切换时未清理相关状态变量

**需要调查：**
- 连接状态详情页面的组件结构（是否复用了消息列表组件）
- 页面切换时输入内容和消息状态的清理逻辑
- `session_controller.dart` 中相关状态变量的生命周期管理
- 是否存在跨页面的状态泄漏问题

---

## 10. Memory 和 Skill 文件夹与 Claude 默认目录不一致，需要手动同步

**现象：**
- MobileVC 的 memory 和 skill 管理功能使用的文件夹路径，可能与 Claude 默认读取/写入的文件夹路径不同
- 用户需要手动让 Claude 将数据同步到 MobileVC 对应的 skill 和 memory 文件夹后，才能在 App 中通过一键同步更新
- 两个系统之间的数据没有自动保持一致

**可能原因：**
- MobileVC 后端配置的 skill/memory 目录与 Claude 的默认工作目录（如 `~/.claude/projects/...` 或项目根目录下的 `.claude/`）不是同一个路径
- Claude 在会话中创建的 memory 和 skill 文件默认写入其自身的项目目录，而 MobileVC 读取的是另一个路径
- 缺少自动同步机制或符号链接，导致两边数据独立维护

**需要调查：**
- 后端 `config.yaml` 或启动参数中配置的 skill/memory 目录路径
- Claude 默认的 memory 和 skill 文件存储位置（通过查看 `.claude/` 目录结构）
- 是否可以通过符号链接或配置文件让 Claude 直接写入 MobileVC 的目录
- 一键同步功能的实现逻辑（是文件复制还是读取同一目录）
- 是否需要添加自动监听文件变化的同步机制

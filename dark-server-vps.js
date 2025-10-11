const express = require("express");
const WebSocket = require("ws");
const http = require("http");
const { EventEmitter } = require("events");
const { randomUUID } = require("crypto");

const PORT = process.env.PORT || 7860;
const HOST = "0.0.0.0";
const MY_SECRET_KEY = process.env.MY_SECRET_KEY || "123456";

const DEFAULT_STREAMING_MODE = "fake";
const MESSAGE_QUEUE_TIMEOUT_MS = 600000;
const KEEP_ALIVE_INTERVAL_MS = 1000;
const WS_HEARTBEAT_INTERVAL_MS = 30000;
const RETRY_MAX_ATTEMPTS = 3;
const RETRY_DELAY_MS = 2000;

// ────────────────────────── 工具函数 ────────────────────────── //
function generateRequestId() {
  if (typeof randomUUID === "function") return randomUUID();
  return `${Date.now()}_${Math.random().toString(36).slice(2, 11)}`;
}

function isStreamPath(path) {
  return path.includes("streamGenerateContent");
}

// ────────────────────────── LoggingService ────────────────────────── //
class LoggingService {
  constructor(serviceName = "ProxyServer") {
    this.serviceName = serviceName;
  }

  _format(level, message) {
    const timestamp = new Date().toISOString();
    return `[${level}] ${timestamp} [${this.serviceName}] - ${message}`;
  }

  info(message) {
    console.log(this._format("INFO", message));
  }

  warn(message) {
    console.warn(this._format("WARN", message));
  }

  error(message) {
    console.error(this._format("ERROR", message));
  }

  debug(message) {
    console.debug(this._format("DEBUG", message));
  }
}

// ────────────────────────── MessageQueue ────────────────────────── //
class MessageQueue extends EventEmitter {
  constructor(owner, timeoutMs = MESSAGE_QUEUE_TIMEOUT_MS) {
    super();
    this.owner = owner;
    this.messages = [];
    this.waitingResolvers = [];
    this.defaultTimeout = timeoutMs;
    this.closed = false;
  }

  setOwner(newOwner) {
    this.owner = newOwner;
  }

  enqueue(payload) {
    if (this.closed) return;
    if (this.waitingResolvers.length > 0) {
      const resolver = this.waitingResolvers.shift();
      if (resolver.timeoutId) clearTimeout(resolver.timeoutId);
      resolver.resolve(payload);
    } else {
      this.messages.push(payload);
    }
  }

  async dequeue(timeoutMs = this.defaultTimeout) {
    if (this.closed) {
      throw new Error("Queue is closed");
    }

    return new Promise((resolve, reject) => {
      if (this.messages.length > 0) {
        resolve(this.messages.shift());
        return;
      }

      const resolver = { resolve, reject, timeoutId: null };
      this.waitingResolvers.push(resolver);

      resolver.timeoutId = setTimeout(() => {
        const idx = this.waitingResolvers.indexOf(resolver);
        if (idx !== -1) {
          this.waitingResolvers.splice(idx, 1);
        }
        reject(new Error("Queue timeout"));
      }, timeoutMs);
    });
  }

  close() {
    if (this.closed) return;
    this.closed = true;

    this.waitingResolvers.forEach((resolver) => {
      if (resolver.timeoutId) clearTimeout(resolver.timeoutId);
      resolver.reject(new Error("Queue closed"));
    });

    this.waitingResolvers = [];
    this.messages = [];
  }
}

// ────────────────────────── ConnectionRegistry ────────────────────────── //
class ConnectionRegistry extends EventEmitter {
  constructor(logger) {
    super();
    this.logger = logger;
    this.connections = new Set();
    this.messageQueues = new Map();
    this.heartbeatInterval = null;
  }

  addConnection(websocket, clientInfo) {
    websocket.isAlive = true;
    this.connections.add(websocket);
    this.logger.info(`新客户端连接: ${clientInfo.address}`);

    websocket.on("pong", () => {
      websocket.isAlive = true;
    });

    websocket.on("message", (buffer) => {
      this._handleIncomingMessage(buffer.toString());
    });

    websocket.on("close", () => this._removeConnection(websocket));

    websocket.on("error", (error) => {
      this.logger.error(`WebSocket连接错误: ${error.message}`);
    });

    if (this.connections.size === 1) {
      this._startHeartbeat();
    }

    this.emit("connectionAdded", websocket);
  }

  _handleIncomingMessage(messageData) {
    try {
      const parsed = JSON.parse(messageData);
      const { request_id, event_type } = parsed;

      if (!request_id) {
        this.logger.warn("收到无效消息：缺少 request_id");
        return;
      }

      const queue = this.messageQueues.get(request_id);
      if (!queue) {
        this.logger.warn(`收到未知请求ID的消息: ${request_id}`);
        return;
      }

      switch (event_type) {
        case "response_headers":
        case "chunk":
        case "error":
          queue.enqueue(parsed);
          break;
        case "stream_close":
          queue.enqueue({ type: "STREAM_END" });
          break;
        default:
          this.logger.warn(`未知的事件类型: ${event_type}`);
      }
    } catch (error) {
      this.logger.error(`解析 WebSocket 消息失败: ${error.message}`);
    }
  }

  _removeConnection(websocket) {
    this.connections.delete(websocket);
    this.logger.info("客户端连接断开");

    this._closeQueuesForConnection(websocket);

    if (this.connections.size === 0) {
      this._stopHeartbeat();
    }

    this.emit("connectionRemoved", websocket);
  }

  _closeQueuesForConnection(connection) {
    for (const [requestId, queue] of this.messageQueues.entries()) {
      if (queue.owner === connection) {
        queue.close();
        this.messageQueues.delete(requestId);
      }
    }
  }

  _startHeartbeat() {
    this.logger.info("心跳检测机制已启动。");
    if (this.heartbeatInterval) clearInterval(this.heartbeatInterval);

    this.heartbeatInterval = setInterval(() => {
      this.connections.forEach((ws) => {
        if (ws.isAlive === false) {
          this.logger.warn("检测到僵尸连接，正在终止...");
          ws.terminate();
          return;
        }
        ws.isAlive = false;
        ws.ping(() => {});
      });
    }, WS_HEARTBEAT_INTERVAL_MS);
  }

  _stopHeartbeat() {
    if (!this.heartbeatInterval) return;
    clearInterval(this.heartbeatInterval);
    this.heartbeatInterval = null;
    this.logger.info("连接全部断开，心跳检测机制已停止。");
  }

  hasActiveConnections() {
    return this.connections.size > 0;
  }

  isConnectionAlive(connection) {
    if (!connection) return false;
    return (
      this.connections.has(connection) &&
      connection.readyState === WebSocket.OPEN &&
      connection.isAlive !== false
    );
  }

  ensureActiveConnection(preferred) {
    if (this.isConnectionAlive(preferred)) {
      return preferred;
    }
    return this.getAvailableConnection();
  }

  getAvailableConnection() {
    const alive = Array.from(this.connections).filter((ws) =>
      this.isConnectionAlive(ws)
    );

    if (alive.length === 0 && this.connections.size > 0) {
      this.logger.warn("有连接记录，但没有一个通过心跳检测！");
    }

    return alive[0] || null;
  }

  createMessageQueue(requestId, ownerConnection) {
    const queue = new MessageQueue(ownerConnection);
    this.messageQueues.set(requestId, queue);
    return queue;
  }

  rebindQueueOwner(requestId, newOwner) {
    const queue = this.messageQueues.get(requestId);
    if (queue) {
      queue.setOwner(newOwner);
    }
  }

  removeMessageQueue(requestId) {
    const queue = this.messageQueues.get(requestId);
    if (!queue) return;

    queue.close();
    this.messageQueues.delete(requestId);
  }
}

// ────────────────────────── RequestHandler ────────────────────────── //
class RequestHandler {
  constructor(serverSystem, connectionRegistry, logger) {
    this.serverSystem = serverSystem;
    this.connectionRegistry = connectionRegistry;
    this.logger = logger;

    this.retryAttempts = RETRY_MAX_ATTEMPTS;
    this.retryDelay = RETRY_DELAY_MS;
  }

  async processRequest(req, res) {
    if (!this._authorize(req, res)) return;

    const initialConnection = this.connectionRegistry.getAvailableConnection();
    if (!initialConnection) {
      this._sendErrorResponse(res, 503, "没有可用的浏览器连接");
      return;
    }

    const requestId = generateRequestId();
    const proxyRequest = this._buildProxyRequest(req, requestId);
    const queue = this.connectionRegistry.createMessageQueue(
      requestId,
      initialConnection
    );

    const context = {
      requestId,
      proxyRequest,
      queue,
      connection: initialConnection,
      isStreamRequest: isStreamPath(req.path),
      mode: this.serverSystem.streamingMode,
      path: req.path,
    };

    try {
      if (context.mode === "fake") {
        await this._processFakeMode(req, res, context);
      } else {
        await this._processRealMode(res, context);
      }
    } catch (error) {
      this._handleProcessingError(error, res);
    } finally {
      this.connectionRegistry.removeMessageQueue(requestId);
    }
  }

  // ── 认证与构造 ── //
  _authorize(req, res) {
    const clientKey = req.query.key;
    if (clientKey && clientKey === MY_SECRET_KEY) {
      delete req.query.key;
      this.logger.info(`代理密码验证通过。处理请求: ${req.method} ${req.path}`);
      return true;
    }

    this.logger.warn(
      `收到无效/缺失的代理密码，已拒绝。请求路径: ${req.method} ${req.url}`
    );
    this._sendErrorResponse(
      res,
      401,
      "Unauthorized - Invalid API Key in URL parameter"
    );
    return false;
  }

  _buildProxyRequest(req, requestId) {
    return {
      path: req.path,
      method: req.method,
      headers: req.headers,
      query_params: req.query,
      body: this._extractBody(req),
      request_id: requestId,
      streaming_mode: this.serverSystem.streamingMode,
    };
  }

  _extractBody(req) {
    if (Buffer.isBuffer(req.body)) {
      return req.body.toString("utf-8");
    }
    if (typeof req.body === "string") {
      return req.body;
    }
    if (req.body) {
      return JSON.stringify(req.body);
    }
    return "";
  }

  // ── 模式分发 ── //
  async _processFakeMode(req, res, context) {
    if (context.isStreamRequest) {
      await this._handleFakeStream(req, res, context);
    } else {
      await this._handleFakeNonStream(res, context);
    }
  }

  async _processRealMode(res, context) {
    await this._handleRealStreamResponse(res, context);
  }

  // ── 假流模式：非流式 ── //
  async _handleFakeNonStream(res, context) {
    this.logger.info("检测到非流式请求，将返回原始JSON格式");

    try {
      this._forwardRequest(context);

      const headerMessage = await context.queue.dequeue();
      if (headerMessage.event_type === "error") {
        const errorText = `浏览器端报告错误: ${headerMessage.status || ""} ${
          headerMessage.message || "未知错误"
        }`;
        this._sendErrorResponse(res, headerMessage.status || 500, errorText);
        return;
      }

      this._applyResponseHeaders(res, headerMessage);

      const dataMessage = await context.queue.dequeue();
      const endMessage = await context.queue.dequeue();

      if (dataMessage?.data) {
        res.send(dataMessage.data);
        this.logger.info("已将响应作为原始JSON发送");
      } else {
        res.status(204).end();
      }

      if (endMessage?.type !== "STREAM_END") {
        this.logger.warn("未收到预期的流结束信号。");
      }
    } catch (error) {
      this.logger.error(`处理非流式请求失败: ${error.message}`);
      this._sendErrorResponse(res, 500, `代理错误: ${error.message}`);
    }
  }

  // ── 假流模式：流式 ── //
  async _handleFakeStream(req, res, context) {
    this._prepareSseResponse(res);
    this.logger.info("已向客户端发送初始响应头，假流式计时器已启动。");

    const keepAliveChunk = this._getFakeKeepAliveChunk(context.path);
    let keepAliveTimer = null;

    try {
      keepAliveTimer = setInterval(() => {
        if (!res.writableEnded) {
          res.write(keepAliveChunk);
        }
      }, KEEP_ALIVE_INTERVAL_MS);

      this.logger.info("请求已派发给浏览器端处理...");
      this._forwardRequest(context);

      const headerMessage = await context.queue.dequeue();
      if (headerMessage.event_type === "error") {
        const errorText = `浏览器端报告错误: ${headerMessage.status || ""} ${
          headerMessage.message || "未知错误"
        }`;
        this._sendErrorChunk(res, errorText);
        throw new Error(errorText);
      }

      const payloadMessage = await context.queue.dequeue();
      const endMessage = await context.queue.dequeue();

      if (payloadMessage?.data) {
        res.write(`data: ${payloadMessage.data}\n\n`);
        this.logger.info("已将完整响应体作为SSE事件发送。");
      }

      if (endMessage?.type !== "STREAM_END") {
        this.logger.warn("未收到预期的流结束信号。");
      }
    } finally {
      if (keepAliveTimer) clearInterval(keepAliveTimer);
      if (!res.writableEnded) res.end();
      this.logger.info("假流式响应处理结束。");
    }
  }

  _prepareSseResponse(res) {
    res.status(200).set({
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
    });
  }

  _getFakeKeepAliveChunk(path) {
    if (path.includes("chat/completions")) {
      const payload = {
        id: `chatcmpl-${generateRequestId()}`,
        object: "chat.completion.chunk",
        created: Math.floor(Date.now() / 1000),
        model: "gpt-4",
        choices: [{ index: 0, delta: {}, finish_reason: null }],
      };
      return `data: ${JSON.stringify(payload)}\n\n`;
    }

    if (path.includes("generateContent") || path.includes("streamGenerateContent")) {
      const payload = {
        candidates: [
          {
            content: { parts: [{ text: "" }], role: "model" },
            finishReason: null,
            index: 0,
            safetyRatings: [],
          },
        ],
      };
      return `data: ${JSON.stringify(payload)}\n\n`;
    }

    return ": keep-alive\n\n";
  }

  // ── 真流模式 ── //
  async _handleRealStreamResponse(res, context) {
    const headerMessage = await this._retrieveHeadersWithRetry(context);

    if (headerMessage.event_type === "error") {
      this._sendErrorResponse(
        res,
        headerMessage.status,
        headerMessage.message || "浏览器端错误"
      );
      return;
    }

    if (context.isStreamRequest) {
      this._ensureStreamHeaders(headerMessage);
    }

    this._applyResponseHeaders(res, headerMessage);
    this.logger.info("已向客户端发送真实响应头，开始流式传输...");

    try {
      while (true) {
        const nextMessage = await context.queue.dequeue(30000);

        if (nextMessage?.type === "STREAM_END") {
          this.logger.info("收到流结束信号。");
          break;
        }

        if (nextMessage?.event_type === "error") {
          this.logger.warn(
            `流处理中收到错误事件: ${nextMessage.message || "未知错误"}`
          );
          this._sendErrorChunk(res, nextMessage.message || "流处理异常");
          break;
        }

        if (nextMessage?.data && !res.writableEnded) {
          res.write(nextMessage.data);
        }
      }
    } catch (error) {
      if (error.message !== "Queue timeout") {
        throw error;
      }
      this.logger.warn("真流式响应超时，可能流已正常结束。");
    } finally {
      if (!res.writableEnded) res.end();
      this.logger.info("真流式响应连接已关闭。");
    }
  }

  async _retrieveHeadersWithRetry(context) {
    let lastHeaderMessage = null;

    for (let attempt = 1; attempt <= this.retryAttempts; attempt++) {
      this.logger.info(`请求尝试 #${attempt}/${this.retryAttempts}...`);
      this._forwardRequest(context);

      lastHeaderMessage = await context.queue.dequeue();

      if (
        lastHeaderMessage.event_type === "error" &&
        lastHeaderMessage.status >= 400 &&
        lastHeaderMessage.status <= 599 &&
        attempt < this.retryAttempts
      ) {
        this.logger.warn(
          `收到 ${lastHeaderMessage.status} 错误，将在 ${
            this.retryDelay / 1000
          } 秒后重试...`
        );
        await new Promise((resolve) => setTimeout(resolve, this.retryDelay));
        continue;
      }

      break;
    }

    return lastHeaderMessage;
  }

  // ── 底层协作 ── //
  _forwardRequest(context) {
    const connection = this.connectionRegistry.ensureActiveConnection(
      context.connection
    );
    if (!connection) {
      throw new Error("无法转发请求：没有可用的 WebSocket 连接。");
    }

    if (connection !== context.connection) {
      this.logger.warn("连接已切换，正在重新绑定请求队列。");
      context.connection = connection;
      this.connectionRegistry.rebindQueueOwner(context.requestId, connection);
    }

    try {
      connection.send(JSON.stringify(context.proxyRequest));
    } catch (error) {
      throw new Error(`请求转发失败: ${error.message}`);
    }
  }

  _applyResponseHeaders(res, headerMessage) {
    res.status(headerMessage.status || 200);

    const headers = headerMessage.headers || {};
    Object.entries(headers).forEach(([name, value]) => {
      if (name.toLowerCase() !== "content-length") {
        res.set(name, value);
      }
    });
  }

  _ensureStreamHeaders(headerMessage) {
    if (!headerMessage.headers) {
      headerMessage.headers = {};
    }
    if (!headerMessage.headers["content-type"]) {
      this.logger.info("为流式请求补充 Content-Type: text/event-stream");
      headerMessage.headers["content-type"] = "text/event-stream";
    }
  }

  _sendErrorChunk(res, message) {
    if (!res || res.writableEnded) return;
    const payload = {
      error: {
        message: `[代理系统提示] ${message}`,
        type: "proxy_error",
        code: "proxy_error",
      },
    };
    res.write(`data: ${JSON.stringify(payload)}\n\n`);
    this.logger.info(`已向客户端发送标准错误信号: ${message}`);
  }

  _handleProcessingError(error, res) {
    if (res.headersSent) {
      this.logger.error(`请求处理错误 (头已发送): ${error.message}`);
      if (this.serverSystem.streamingMode === "fake") {
        this._sendErrorChunk(res, `处理失败: ${error.message}`);
      }
      if (!res.writableEnded) {
        res.end();
      }
      return;
    }

    this.logger.error(`请求处理错误: ${error.message}`);
    const status = error.message.includes("超时") ? 504 : 500;
    this._sendErrorResponse(res, status, `代理错误: ${error.message}`);
  }

  _sendErrorResponse(res, status, message) {
    if (res.headersSent) return;
    res.status(status || 500).type("text/plain").send(message);
  }
}

// ────────────────────────── ProxyServerSystem ────────────────────────── //
class ProxyServerSystem extends EventEmitter {
  constructor(config = {}) {
    super();
    this.config = {
      port: PORT,
      host: HOST,
      ...config,
    };

    this.streamingMode = DEFAULT_STREAMING_MODE;
    this.logger = new LoggingService("ProxyServer");
    this.connectionRegistry = new ConnectionRegistry(this.logger);
    this.requestHandler = new RequestHandler(
      this,
      this.connectionRegistry,
      this.logger
    );

    this.httpServer = null;
    this.wsServer = null;
  }

  async start() {
    try {
      await this._bootstrapServers();
      this.logger.info(
        `代理服务器系统启动完成，当前模式: ${this.streamingMode}`
      );
      this.emit("started");
    } catch (error) {
      this.logger.error(`启动失败: ${error.message}`);
      this.emit("error", error);
      throw error;
    }
  }

  async _bootstrapServers() {
    const app = this._createExpressApp();
    this.httpServer = http.createServer(app);
    this.wsServer = new WebSocket.Server({ server: this.httpServer });
    this._bindWebSocketEvents();

    await new Promise((resolve) => {
      const { port, host } = this.config;
      this.httpServer.listen(port, host, () => {
        this.logger.info(`HTTP服务器启动: http://${host}:${port}`);
        this.logger.info(`WS服务器正在监听: ws://${host}:${port}`);
        resolve();
      });
    });
  }

  _createExpressApp() {
    const app = express();

    app.use(express.json({ limit: "100mb" }));
    app.use(express.urlencoded({ extended: true, limit: "100mb" }));
    app.use(express.raw({ type: "*/*", limit: "100mb" }));

    this._registerAdminRoutes(app);
    this._registerProxyRoutes(app);

    return app;
  }

  _registerAdminRoutes(app) {
    app.get("/admin/set-mode", (req, res) => {
      const newMode = req.query.mode;
      if (newMode === "fake" || newMode === "real") {
        this.streamingMode = newMode;
        const message = `流式响应模式已切换为: ${this.streamingMode}`;
        this.logger.info(message);
        res.status(200).send(message);
        return;
      }

      const message = '无效的模式。请使用 "fake" 或 "real"。';
      this.logger.warn(message);
      res.status(400).send(message);
    });

    app.get("/admin/get-mode", (_req, res) => {
      res.status(200).send(`当前流式响应模式为: ${this.streamingMode}`);
    });
  }

  _registerProxyRoutes(app) {
    app.all("*", (req, res, next) => {
      if (req.path === "/") {
        this.logger.info("根目录'/'被访问，发送状态页面。");
        if (this.connectionRegistry.hasActiveConnections()) {
          res
            .status(200)
            .send("✅ A browser client is connected. The proxy is ready.");
        } else {
          res
            .status(404)
            .send("❌ No browser client connected. Please run the browser script.");
        }
        return;
      }

      if (req.path.startsWith("/admin/")) {
        next();
        return;
      }

      if (req.path === "/favicon.ico") {
        res.status(204).end();
        return;
      }

      this.requestHandler.processRequest(req, res);
    });
  }

  _bindWebSocketEvents() {
    this.wsServer.on("connection", (ws, req) => {
      this.connectionRegistry.addConnection(ws, {
        address: req.socket.remoteAddress,
      });
    });
  }
}

// ────────────────────────── 启动入口 ────────────────────────── //
async function initializeServer() {
  const proxySystem = new ProxyServerSystem();

  try {
    await proxySystem.start();
  } catch (error) {
    console.error("服务器启动失败:", error.message);
    process.exit(1);
  }
}

if (require.main === module) {
  initializeServer();
}

module.exports = { ProxyServerSystem, initializeServer };

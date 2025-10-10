const express = require("express");
const WebSocket = require("ws");
const http = require("http");
const { EventEmitter } = require("events");

const MY_SECRET_KEY = process.env.MY_SECRET_KEY || "123456";
const WS_HEARTBEAT_INTERVAL_MS = 30000;
const MESSAGE_QUEUE_TIMEOUT_MS = 600000;

// ────────────────────────── 工具函数 & 常量 ────────────────────────── //

function generateRequestId() {
  return `${Date.now()}_${Math.random().toString(36).substring(2, 11)}`;
}

function isStreamPath(path) {
  return path.includes("streamGenerateContent");
}

// ────────────────────────── LoggingService ────────────────────────── //

class LoggingService {
  constructor(serviceName = "ProxyServer") {
    this.serviceName = serviceName;
  }

  _formatMessage(level, message) {
    const timestamp = new Date().toISOString();
    return `[${level}] ${timestamp} [${this.serviceName}] - ${message}`;
  }

  info(message) {
    console.log(this._formatMessage("INFO", message));
  }

  error(message) {
    console.error(this._formatMessage("ERROR", message));
  }

  warn(message) {
    console.warn(this._formatMessage("WARN", message));
  }

  debug(message) {
    console.debug(this._formatMessage("DEBUG", message));
  }
}

// ────────────────────────── MessageQueue ────────────────────────── //

class MessageQueue extends EventEmitter {
  constructor(timeoutMs = MESSAGE_QUEUE_TIMEOUT_MS) {
    super();
    this.messages = [];
    this.waitingResolvers = [];
    this.defaultTimeout = timeoutMs;
    this.closed = false;
  }

  enqueue(message) {
    if (this.closed) return;
    if (this.waitingResolvers.length > 0) {
      const resolver = this.waitingResolvers.shift();
      if (resolver.timeoutId) clearTimeout(resolver.timeoutId);
      resolver.resolve(message);
    } else {
      this.messages.push(message);
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
        const index = this.waitingResolvers.indexOf(resolver);
        if (index !== -1) {
          this.waitingResolvers.splice(index, 1);
          reject(new Error("Queue timeout"));
        }
      }, timeoutMs);
    });
  }

  close() {
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

    websocket.on("message", (data) => {
      this._handleIncomingMessage(data.toString());
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

  _removeConnection(websocket) {
    this.connections.delete(websocket);
    this.logger.info("客户端连接断开");

    this.messageQueues.forEach((queue) => queue.close());
    this.messageQueues.clear();

    if (this.connections.size === 0) {
      this._stopHeartbeat();
    }

    this.emit("connectionRemoved", websocket);
  }

  _stopHeartbeat() {
    if (!this.heartbeatInterval) return;
    clearInterval(this.heartbeatInterval);
    this.heartbeatInterval = null;
    this.logger.info("连接全部断开，心跳检测机制已停止。");
  }

  _handleIncomingMessage(messageData) {
    try {
      const parsedMessage = JSON.parse(messageData);
      const { request_id, event_type } = parsedMessage;

      if (!request_id) {
        this.logger.warn("收到无效消息：缺少request_id");
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
          queue.enqueue(parsedMessage);
          break;
        case "stream_close":
          queue.enqueue({ type: "STREAM_END" });
          break;
        default:
          this.logger.warn(`未知的事件类型: ${event_type}`);
      }
    } catch (error) {
      this.logger.error(`解析WebSocket消息失败: ${error.message}`);
    }
  }

  hasActiveConnections() {
    return this.connections.size > 0;
  }

  getFirstConnection() {
    const aliveConnections = Array.from(this.connections).filter(
      (ws) => ws.isAlive
    );
    if (aliveConnections.length === 0 && this.connections.size > 0) {
      this.logger.warn("有连接记录，但没有一个通过心跳检测！");
    }
    return aliveConnections[0];
  }

  createMessageQueue(requestId) {
    const queue = new MessageQueue();
    this.messageQueues.set(requestId, queue);
    return queue;
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

    this.maxRetries = 3;
    this.retryDelay = 2000;
  }

  async processRequest(req, res) {
    if (!this._authorizeRequest(req, res)) return;
    if (!this._ensureActiveConnection(res)) return;

    const requestId = generateRequestId();
    const proxyRequest = this._buildProxyRequest(req, requestId);
    const queue = this.connectionRegistry.createMessageQueue(requestId);

    try {
      await this._dispatchByStreamingMode({
        req,
        res,
        proxyRequest,
        queue,
      });
    } catch (error) {
      this._handleRequestError(error, res);
    } finally {
      this.connectionRegistry.removeMessageQueue(requestId);
    }
  }

  // ── 授权 & 连接检查 ── //
  _authorizeRequest(req, res) {
    const clientKey = req.query.key;
    if (clientKey && clientKey === MY_SECRET_KEY) {
      delete req.query.key;
      this.logger.info(`代理密码验证通过。处理请求: ${req.method} ${req.path}`);
      return true;
    }

    this.logger.warn(
      `收到一个无效的或缺失的代理密码，已拒绝。请求路径: ${req.url}`
    );
    this._sendErrorResponse(
      res,
      401,
      "Unauthorized - Invalid API Key in URL parameter"
    );
    return false;
  }

  _ensureActiveConnection(res) {
    if (this.connectionRegistry.hasActiveConnections()) return true;
    this._sendErrorResponse(res, 503, "没有可用的浏览器连接");
    return false;
  }

  // ── 请求构造 ── //
  _buildProxyRequest(req, requestId) {
    return {
      path: req.path,
      method: req.method,
      headers: req.headers,
      query_params: req.query,
      body: this._extractRequestBody(req),
      request_id: requestId,
      streaming_mode: this.serverSystem.streamingMode,
    };
  }

  _extractRequestBody(req) {
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
  async _dispatchByStreamingMode({ req, res, proxyRequest, queue }) {
    const mode = this.serverSystem.streamingMode;
    return mode === "fake"
      ? this._processFakeModeRequest({ req, res, proxyRequest, queue })
      : this._processRealModeRequest({ res, proxyRequest, queue });
  }

  async _processFakeModeRequest({ req, res, proxyRequest, queue }) {
    const streamRequested = isStreamPath(req.path);
    return streamRequested
      ? this._handleFakeStream({ req, res, proxyRequest, queue })
      : this._handleFakeNonStream({ res, proxyRequest, queue });
  }

  async _processRealModeRequest({ res, proxyRequest, queue }) {
    const streamRequested = isStreamPath(proxyRequest.path);
    return this._handleRealStreamResponse({
      res,
      proxyRequest,
      queue,
      isStreamRequest: streamRequested,
    });
  }

  // ── 假流模式 ── //
  async _handleFakeNonStream({ res, proxyRequest, queue }) {
    this.logger.info("检测到非流式请求，将返回原始JSON格式");

    try {
      this._forwardRequest(proxyRequest);

      const headerMessage = await queue.dequeue();
      if (headerMessage.event_type === "error") {
        const errorText = `浏览器端报告错误: ${headerMessage.status || ""} ${
          headerMessage.message || "未知错误"
        }`;
        this._sendErrorResponse(res, headerMessage.status || 500, errorText);
        return;
      }

      this._setResponseHeaders(res, headerMessage);

      const dataMessage = await queue.dequeue();
      const endMessage = await queue.dequeue();

      if (dataMessage.data) {
        res.send(dataMessage.data);
        this.logger.info("已将响应作为原始JSON发送");
      }

      if (endMessage.type !== "STREAM_END") {
        this.logger.warn("未收到预期的流结束信号。");
      }
    } catch (error) {
      this.logger.error(`处理非流式请求失败: ${error.message}`);
      this._sendErrorResponse(res, 500, `代理错误: ${error.message}`);
    }
  }

  async _handleFakeStream({ req, res, proxyRequest, queue }) {
    this._prepareSseResponse(res);
    const keepAliveChunk = this._getKeepAliveChunk(req.path);
    let keepAliveTimer = null;

    try {
      keepAliveTimer = this._startKeepAlive(res, keepAliveChunk);
      this.logger.info("已向客户端发送初始响应头，假流式计时器已启动。");
      this.logger.info("请求已派发给浏览器端处理...");
      this._forwardRequest(proxyRequest);

      const headerMessage = await queue.dequeue();
      if (headerMessage.event_type === "error") {
        const errorText = `浏览器端报告错误: ${headerMessage.status || ""} ${
          headerMessage.message || "未知错误"
        }`;
        this._sendErrorChunkToClient(res, errorText);
        throw new Error(errorText);
      }

      const dataMessage = await queue.dequeue();
      const endMessage = await queue.dequeue();

      if (dataMessage.data) {
        res.write(`data: ${dataMessage.data}\n\n`);
        this.logger.info("已将完整响应体作为SSE事件发送。");
      }

      if (endMessage.type !== "STREAM_END") {
        this.logger.warn("未收到预期的流结束信号。");
      }
    } finally {
      this._stopKeepAlive(keepAliveTimer);
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

  _startKeepAlive(res, chunk) {
    return setInterval(() => {
      if (!res.writableEnded) res.write(chunk);
    }, 1000);
  }

  _stopKeepAlive(timerId) {
    if (timerId) clearInterval(timerId);
  }

  // ── 真流模式 ── //
  async _handleRealStreamResponse({
    res,
    proxyRequest,
    queue,
    isStreamRequest,
  }) {
    const headerMessage = await this._fetchResponseHeadersWithRetry({
      proxyRequest,
      queue,
    });

    if (headerMessage.event_type === "error") {
      this._sendErrorResponse(
        res,
        headerMessage.status,
        headerMessage.message
      );
      return;
    }

    if (isStreamRequest) {
      this._ensureStreamContentType(headerMessage);
    }

    this._setResponseHeaders(res, headerMessage);
    this.logger.info("已向客户端发送真实响应头，开始流式传输...");

    try {
      while (true) {
        const dataMessage = await queue.dequeue(30000);
        if (dataMessage.type === "STREAM_END") {
          this.logger.info("收到流结束信号。");
          break;
        }
        if (dataMessage.data) {
          res.write(dataMessage.data);
        }
      }
    } catch (error) {
      if (error.message !== "Queue timeout") throw error;
      this.logger.warn("真流式响应超时，可能流已正常结束。");
    } finally {
      if (!res.writableEnded) res.end();
      this.logger.info("真流式响应连接已关闭。");
    }
  }

  async _fetchResponseHeadersWithRetry({ proxyRequest, queue }) {
    let headerMessage;

    for (let attempt = 1; attempt <= this.maxRetries; attempt++) {
      this.logger.info(`请求尝试 #${attempt}/${this.maxRetries}...`);
      this._forwardRequest(proxyRequest);
      headerMessage = await queue.dequeue();

      const isError =
        headerMessage.event_type === "error" &&
        headerMessage.status >= 400 &&
        headerMessage.status <= 599;

      if (!isError || attempt === this.maxRetries) {
        break;
      }

      this.logger.warn(
        `收到 ${headerMessage.status} 错误，将在 ${
          this.retryDelay / 1000
        }秒后重试...`
      );
      await new Promise((resolve) => setTimeout(resolve, this.retryDelay));
    }

    return headerMessage;
  }

  _ensureStreamContentType(headerMessage) {
    if (!headerMessage.headers) {
      headerMessage.headers = {};
    }
    if (!headerMessage.headers["content-type"]) {
      this.logger.info("为流式请求添加 Content-Type 头");
      headerMessage.headers["content-type"] = "text/event-stream";
    }
  }

  // ── 共享辅助方法 ── //
  _forwardRequest(proxyRequest) {
    const connection = this.connectionRegistry.getFirstConnection();
    if (!connection) {
      throw new Error("无法转发请求：没有可用的WebSocket连接。");
    }

    try {
      connection.send(JSON.stringify(proxyRequest));
    } catch (error) {
      throw new Error(`请求转发失败: ${error.message}`);
    }
  }

  _setResponseHeaders(res, headerMessage) {
    res.status(headerMessage.status || 200);
    const headers = headerMessage.headers || {};
    Object.entries(headers).forEach(([name, value]) => {
      if (name.toLowerCase() !== "content-length") {
        res.set(name, value);
      }
    });
  }

  _getKeepAliveChunk(path) {
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

    return "data: {}\n\n";
  }

  _sendErrorChunkToClient(res, errorMessage) {
    if (!res || res.writableEnded) return;

    const errorPayload = {
      error: {
        message: `[代理系统提示] ${errorMessage}`,
        type: "proxy_error",
        code: "proxy_error",
      },
    };

    const chunk = `data: ${JSON.stringify(errorPayload)}\n\n`;
    res.write(chunk);
    this.logger.info(`已向客户端发送标准错误信号: ${errorMessage}`);
  }

  _handleRequestError(error, res) {
    if (res.headersSent) {
      this.logger.error(`请求处理错误 (头已发送): ${error.message}`);
      if (this.serverSystem.streamingMode === "fake") {
        this._sendErrorChunkToClient(res, `处理失败: ${error.message}`);
      }
      if (!res.writableEnded) res.end();
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

    const port = process.env.PORT || 7860;
    this.config = {
      port,
      host: "0.0.0.0",
      ...config,
    };

    this.streamingMode = "fake";
    this.logger = new LoggingService("ProxyServer");
    this.connectionRegistry = new ConnectionRegistry(this.logger);
    this.requestHandler = new RequestHandler(
      this,
      this.connectionRegistry,
      this.logger
    );

    this.server = null;
    this.wsServer = null;
  }

  async start() {
    try {
      await this._startServer();
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

  async _startServer() {
    const app = this._createExpressApp();
    const httpServer = http.createServer(app);

    this.server = httpServer;
    this.wsServer = new WebSocket.Server({ server: httpServer });
    this._bindWebSocketEvents();

    return new Promise((resolve) => {
      const { port, host } = this.config;
      this.server.listen(port, host, () => {
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

    app.get("/admin/get-mode", (_, res) => {
      res.status(200).send(`当前流式响应模式为: ${this.streamingMode}`);
    });
  }

  _registerProxyRoutes(app) {
    app.all(/(.*)/, (req, res, next) => {
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
        res.status(204).send();
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
  const serverSystem = new ProxyServerSystem();

  try {
    await serverSystem.start();
  } catch (error) {
    console.error("服务器启动失败:", error.message);
    process.exit(1);
  }
}

if (require.main === module) {
  initializeServer();
}

module.exports = { ProxyServerSystem, initializeServer };

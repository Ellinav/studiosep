const express = require("express");
const WebSocket = require("ws");
const http = require("http");
const { EventEmitter } = require("events");
const MY_SECRET_KEY = process.env.MY_SECRET_KEY || "123456";

// --- LoggingService (无变动) ---
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

// --- MessageQueue (无变动) ---
class MessageQueue extends EventEmitter {
  constructor(timeoutMs = 600000) {
    super();
    this.messages = [];
    this.waitingResolvers = [];
    this.defaultTimeout = timeoutMs;
    this.closed = false;
  }
  enqueue(message) {
    if (this.closed) return;
    if (this.waitingResolvers.length > 0) {
      this.waitingResolvers.shift().resolve(message);
    } else {
      this.messages.push(message);
    }
  }
  async dequeue(timeoutMs = this.defaultTimeout) {
    if (this.closed) throw new Error("Queue is closed");
    return new Promise((resolve, reject) => {
      if (this.messages.length > 0) {
        resolve(this.messages.shift());
        return;
      }
      const resolver = { resolve, reject };
      this.waitingResolvers.push(resolver);
      const timeoutId = setTimeout(() => {
        const index = this.waitingResolvers.indexOf(resolver);
        if (index !== -1) {
          this.waitingResolvers.splice(index, 1);
          reject(new Error("Queue timeout"));
        }
      }, timeoutMs);
      resolver.timeoutId = timeoutId;
    });
  }
  close() {
    this.closed = true;
    this.waitingResolvers.forEach((r) => {
      clearTimeout(r.timeoutId);
      r.reject(new Error("Queue closed"));
    });
    this.waitingResolvers = [];
    this.messages = [];
  }
}

// --- ConnectionRegistry (无变动) ---
class ConnectionRegistry extends EventEmitter {
  constructor(logger) {
    super();
    this.logger = logger;
    this.connections = new Set();
    this.messageQueues = new Map();
    this.heartbeatInterval = null; // 用于存放计时器ID
  }

  // 新增：心跳检测函数
  _startHeartbeat() {
    this.logger.info("心跳检测机制已启动。");
    // 清除旧的计时器以防万一
    if (this.heartbeatInterval) clearInterval(this.heartbeatInterval);

    this.heartbeatInterval = setInterval(() => {
      this.connections.forEach((ws) => {
        if (ws.isAlive === false) {
          this.logger.warn(`检测到僵尸连接，正在终止...`);
          return ws.terminate(); // 强制终止不响应的连接
        }
        ws.isAlive = false; // 假设它死了，等待pong来证明它活着
        ws.ping(() => {}); // 发送ping
      });
    }, 30000); // 每30秒检测一次
  }

  // 新增：停止心跳检测
  _stopHeartbeat() {
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
      this.heartbeatInterval = null;
      this.logger.info("连接全部断开，心跳检测机制已停止。");
    }
  }

  addConnection(websocket, clientInfo) {
    websocket.isAlive = true; // 初始化时，我们认为它是活的
    this.connections.add(websocket);
    this.logger.info(`新客户端连接: ${clientInfo.address}`);

    // 当收到pong时，标记为存活
    websocket.on("pong", () => {
      websocket.isAlive = true;
    });

    websocket.on("message", (data) =>
      this._handleIncomingMessage(data.toString())
    );
    websocket.on("close", () => this._removeConnection(websocket));
    websocket.on("error", (error) =>
      this.logger.error(`WebSocket连接错误: ${error.message}`)
    );

    if (this.connections.size === 1) {
      this._startHeartbeat(); // 当第一个连接建立时，启动心跳
    }
    this.emit("connectionAdded", websocket);
  }

  _removeConnection(websocket) {
    this.connections.delete(websocket);
    this.logger.info("客户端连接断开");
    this.messageQueues.forEach((queue) => queue.close());
    this.messageQueues.clear();

    if (this.connections.size === 0) {
      this._stopHeartbeat(); // 当所有连接都断开时，停止心跳
    }
    this.emit("connectionRemoved", websocket);
  }

  // _handleIncomingMessage, hasActiveConnections, getFirstConnection,
  // createMessageQueue, removeMessageQueue 这些方法保持不变
  _handleIncomingMessage(messageData) {
    try {
      const parsedMessage = JSON.parse(messageData);
      const { request_id, event_type } = parsedMessage;
      if (!request_id) {
        this.logger.warn("收到无效消息：缺少request_id");
        return;
      }
      const queue = this.messageQueues.get(request_id);
      if (queue) {
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
      } else {
        this.logger.warn(`收到未知请求ID的消息: ${request_id}`);
      }
    } catch (error) {
      this.logger.error("解析WebSocket消息失败");
    }
  }

  hasActiveConnections() {
    return this.connections.size > 0;
  }

  getFirstConnection() {
    // 筛选出真正存活的连接
    const aliveConnections = Array.from(this.connections).filter(
      (ws) => ws.isAlive
    );
    if (aliveConnections.length === 0 && this.connections.size > 0) {
      this.logger.warn("有连接记录，但没有一个通过心跳检测！");
    }
    return aliveConnections[0]; // 总是返回第一个健康的连接
  }

  createMessageQueue(requestId) {
    const queue = new MessageQueue();
    this.messageQueues.set(requestId, queue);
    return queue;
  }

  removeMessageQueue(requestId) {
    const queue = this.messageQueues.get(requestId);
    if (queue) {
      queue.close();
      this.messageQueues.delete(requestId);
    }
  }
}

// --- RequestHandler (无变动) ---
class RequestHandler {
  constructor(serverSystem, connectionRegistry, logger) {
    this.serverSystem = serverSystem;
    this.connectionRegistry = connectionRegistry;
    this.logger = logger;
    this.maxRetries = 3;
    this.retryDelay = 2000;
  }

  async processRequest(req, res) {
    // 1. 优先检查URL查询参数中的'key'字段 (兼容SillyTavern)
    let clientKey = req.query.key;

    // 2. 如果URL中没有，则检查Authorization header (标准Bearer Token)
    if (!clientKey) {
      const authHeader = req.headers.authorization;
      if (authHeader && authHeader.startsWith("Bearer ")) {
        clientKey = authHeader.substring(7); // 提取 "Bearer " 后面的部分
      }
    }

    // 3. [新增] 如果还是没有，检查 'x-goog-api-key' header (兼容Cherry Studio)
    if (!clientKey) {
      // 注意：header 的 key 都是小写的
      clientKey = req.headers["x-goog-api-key"];
    }

    // 4. 执行统一的验证
    if (!clientKey || clientKey !== MY_SECRET_KEY) {
      // (可选) 既然找到问题了，你可以删除下面这 6 行调试日志
      this.logger.warn("--- 调试信息：认证失败 ---");
      this.logger.warn(`请求路径 (Path): ${req.url}`);
      this.logger.warn(`查询参数 (Query): ${JSON.stringify(req.query)}`);
      this.logger.warn(
        `请求头 (Headers): ${JSON.stringify(req.headers, null, 2)}`
      );
      this.logger.warn("---------------------------");

      this.logger.warn(
        `收到一个无效的或缺失的代理密码 (query, auth header, or x-goog-api-key)，已拒绝。请求路径: ${req.url}`
      );
      return this._sendErrorResponse(
        res,
        401,
        "Unauthorized - Invalid or missing API Key. Check 'key' param, 'Authorization: Bearer' header, or 'x-goog-api-key' header."
      );
    }

    // 5. 验证通过，如果key在query中，则移除它，避免它被发送到Google
    if (req.query.key) {
      delete req.query.key;
    }

    this.logger.info(`代理密码验证通过。处理请求: ${req.method} ${req.path}`);

    if (!this.connectionRegistry.hasActiveConnections()) {
      return this._sendErrorResponse(res, 503, "没有可用的浏览器连接");
    }

    const requestId = this._generateRequestId();
    const proxyRequest = this._buildProxyRequest(req, requestId);
    const messageQueue = this.connectionRegistry.createMessageQueue(requestId);

    try {
      if (this.serverSystem.streamingMode === "fake") {
        await this._handlePseudoStreamResponse(
          proxyRequest,
          messageQueue,
          req,
          res
        );
      } else {
        await this._handleRealStreamResponse(proxyRequest, messageQueue, res);
      }
    } catch (error) {
      this._handleRequestError(error, res);
    } finally {
      this.connectionRegistry.removeMessageQueue(requestId);
    }
  }
  _generateRequestId() {
    return `${Date.now()}_${Math.random().toString(36).substring(2, 11)}`;
  }
  _buildProxyRequest(req, requestId) {
    let requestBody = "";
    if (Buffer.isBuffer(req.body)) {
      requestBody = req.body.toString("utf-8");
    } else if (typeof req.body === "string") {
      requestBody = req.body;
    } else if (req.body) {
      requestBody = JSON.stringify(req.body);
    }
    return {
      path: req.path,
      method: req.method,
      headers: req.headers,
      query_params: req.query,
      body: requestBody,
      request_id: requestId,
      streaming_mode: this.serverSystem.streamingMode,
    };
  }
  _forwardRequest(proxyRequest) {
    const connection = this.connectionRegistry.getFirstConnection();
    if (connection) {
      connection.send(JSON.stringify(proxyRequest));
    } else {
      throw new Error("无法转发请求：没有可用的WebSocket连接。");
    }
  }
  _sendErrorChunkToClient(res, errorMessage) {
    const errorPayload = {
      error: {
        message: `[代理系统提示] ${errorMessage}`,
        type: "proxy_error",
        code: "proxy_error",
      },
    };
    const chunk = `data: ${JSON.stringify(errorPayload)}\n\n`;
    if (res && !res.writableEnded) {
      res.write(chunk);
      this.logger.info(`已向客户端发送标准错误信号: ${errorMessage}`);
    }
  }

  async _handlePseudoStreamResponse(proxyRequest, messageQueue, req, res) {
    res.status(200).set({
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
    });
    this.logger.info("已向客户端发送初始响应头，假流式计时器已启动。");
    let connectionMaintainer = null;
    try {
      const keepAliveChunk = this._getKeepAliveChunk(req);
      connectionMaintainer = setInterval(() => {
        if (!res.writableEnded) {
          res.write(keepAliveChunk);
        }
      }, 1000);

      // --- 移除了服务器端的 for 循环重试 ---
      this.logger.info(`请求已派发给浏览器端处理...`);
      this._forwardRequest(proxyRequest);

      // 等待浏览器端的最终处理结果（成功或失败）
      const lastMessage = await messageQueue.dequeue();

      // 如果浏览器端报告了错误，直接抛出，不再重试
      if (lastMessage.event_type === "error") {
        const errorText = `浏览器端报告错误: ${lastMessage.status || ""} ${
          lastMessage.message || "未知错误"
        }`;
        this._sendErrorChunkToClient(res, errorText);
        throw new Error(errorText);
      }

      const dataMessage = await messageQueue.dequeue();
      const endMessage = await messageQueue.dequeue();

      if (dataMessage.data) {
        res.write(`data: ${dataMessage.data}\n\n`);
        this.logger.info("已将完整响应体作为SSE事件发送。");
      }
      if (endMessage.type !== "STREAM_END") {
        this.logger.warn("未收到预期的流结束信号。");
      }
    } finally {
      if (connectionMaintainer) clearInterval(connectionMaintainer);
      if (!res.writableEnded) res.end();
      this.logger.info("假流式响应处理结束。");
    }
  }

  async _handleRealStreamResponse(proxyRequest, messageQueue, res) {
    let headerMessage;
    for (let attempt = 1; attempt <= this.maxRetries; attempt++) {
      this.logger.info(`请求尝试 #${attempt}/${this.maxRetries}...`);
      this._forwardRequest(proxyRequest);
      headerMessage = await messageQueue.dequeue();
      if (
        headerMessage.event_type === "error" &&
        headerMessage.status >= 400 &&
        headerMessage.status <= 599
      ) {
        this.logger.warn(
          `收到 ${headerMessage.status} 错误，将在 ${
            this.retryDelay / 1000
          }秒后重试...`
        );
        if (attempt < this.maxRetries) {
          await new Promise((resolve) => setTimeout(resolve, this.retryDelay));
          continue;
        }
      }
      break;
    }
    if (headerMessage.event_type === "error") {
      return this._sendErrorResponse(
        res,
        headerMessage.status,
        headerMessage.message
      );
    }
    this._setResponseHeaders(res, headerMessage);
    this.logger.info("已向客户端发送真实响应头，开始流式传输...");
    try {
      while (true) {
        const dataMessage = await messageQueue.dequeue(30000);
        if (dataMessage.type === "STREAM_END") {
          this.logger.info("收到流结束信号。");
          break;
        }
        if (dataMessage.data) res.write(dataMessage.data);
      }
    } catch (error) {
      if (error.message !== "Queue timeout") throw error;
      this.logger.warn("真流式响应超时，可能流已正常结束。");
    } finally {
      if (!res.writableEnded) res.end();
      this.logger.info("真流式响应连接已关闭。");
    }
  }
  _getKeepAliveChunk(req) {
    if (req.path.includes("chat/completions")) {
      const payload = {
        id: `chatcmpl-${this._generateRequestId()}`,
        object: "chat.completion.chunk",
        created: Math.floor(Date.now() / 1000),
        model: "gpt-4",
        choices: [{ index: 0, delta: {}, finish_reason: null }],
      };
      return `data: ${JSON.stringify(payload)}\n\n`;
    }
    if (
      req.path.includes("generateContent") ||
      req.path.includes("streamGenerateContent")
    ) {
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
  _setResponseHeaders(res, headerMessage) {
    res.status(headerMessage.status || 200);
    const headers = headerMessage.headers || {};
    Object.entries(headers).forEach(([name, value]) => {
      if (name.toLowerCase() !== "content-length") res.set(name, value);
    });
  }
  _handleRequestError(error, res) {
    if (res.headersSent) {
      this.logger.error(`请求处理错误 (头已发送): ${error.message}`);
      if (this.serverSystem.streamingMode === "fake") {
        this._sendErrorChunkToClient(res, `处理失败: ${error.message}`);
      }
      if (!res.writableEnded) res.end();
    } else {
      this.logger.error(`请求处理错误: ${error.message}`);
      this._sendErrorResponse(
        res,
        error.message.includes("超时") ? 504 : 500,
        `代理错误: ${error.message}`
      );
    }
  }
  _sendErrorResponse(res, status, message) {
    if (!res.headersSent)
      res
        .status(status || 500)
        .type("text/plain")
        .send(message);
  }
}

class ProxyServerSystem extends EventEmitter {
  constructor(config = {}) {
    super();
    // 端口从环境变量读取，7860是Hugging Face常用的一个默认值
    const PORT = process.env.PORT || 7860;
    this.config = {
      port: PORT, // 使用动态端口
      host: "0.0.0.0",
      // SSL证书不再需要
      // sslKeyPath: "./key.pem",
      // sslCertPath: "./cert.pem",
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

  _createExpressApp() {
    const app = express();
    app.use(express.json({ limit: "100mb" }));
    app.use(express.urlencoded({ extended: true, limit: "100mb" }));
    app.use(express.raw({ type: "*/*", limit: "100mb" }));

    app.get("/admin/set-mode", (req, res) => {
      const newMode = req.query.mode;
      if (newMode === "fake" || newMode === "real") {
        this.streamingMode = newMode;
        const message = `流式响应模式已切换为: ${this.streamingMode}`;
        this.logger.info(message);
        res.status(200).send(message);
      } else {
        const message = '无效的模式。请使用 "fake" 或 "real"。';
        this.logger.warn(message);
        res.status(400).send(message);
      }
    });

    app.get("/admin/get-mode", (req, res) => {
      res.status(200).send(`当前流式响应模式为: ${this.streamingMode}`);
    });

    app.all(/(.*)/, (req, res, next) => {
      if (req.path === "/") {
        this.logger.info(`根目录'/'被访问，发送状态页面。`);
        if (this.connectionRegistry.hasActiveConnections()) {
          return res
            .status(200)
            .send("✅ A browser client is connected. The proxy is ready.");
        } else {
          return res
            .status(404)
            .send(
              "❌ No browser client connected. Please run the browser script."
            );
        }
      }

      if (req.path.startsWith("/admin/")) return next();
      if (req.path === "/favicon.ico") return res.status(204).send();
      this.requestHandler.processRequest(req, res);
    });

    return app;
  }

  async _startServer() {
    const app = this._createExpressApp();
    const httpServer = http.createServer(app);
    this.server = httpServer;
    this.wsServer = new WebSocket.Server({ server: httpServer });

    this.wsServer.on("connection", (ws, req) => {
      this.connectionRegistry.addConnection(ws, {
        address: req.socket.remoteAddress,
      });
    });

    return new Promise((resolve) => {
      this.server.listen(this.config.port, this.config.host, () => {
        this.logger.info(
          `HTTP服务器启动: http://${this.config.host}:${this.config.port}`
        );
        this.logger.info(
          `WS服务器正在监听: ws://${this.config.host}:${this.config.port}`
        );
        resolve();
      });
    });
  }
}

// --- Main execution block (无变动) ---
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

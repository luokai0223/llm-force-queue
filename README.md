# llm-force-queue

这是一个 LLM 代理服务，用于把请求转发到上游 OpenAI 兼容接口或 Anthropic 接口，转发的时候强制进行串行排队与请求延时，缓解在高峰期，大模型服务商rpm限制导致的报错。agent相关工具接入后整体响应会变慢，但是会减少429等错误的触发。

## 启动服务

```bash
python proxy_server.py
```

## 运行测试

```bash
python test.py
```

## English

This is an LLM proxy service. It forwards requests to upstream OpenAI-compatible or Anthropic APIs, and forces serial queueing plus request delays during forwarding. This helps reduce errors caused by LLM provider RPM limits during peak traffic. After agent-related tools connect through it, overall responses may become slower, but 429 and similar errors should be triggered less often.

### Start the service

```bash
python proxy_server.py
```

### Run tests

```bash
python test.py
```

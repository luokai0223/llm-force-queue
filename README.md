# llm-force-queue

这是一个 LLM 代理服务，用于把请求转发到上游 OpenAI 兼容接口或 Anthropic 接口，转发的时候强制进行串行排队与请求延时，缓解在高峰期，大模型服务商rpm限制导致的报错。agent相关工具接入后整体响应会变慢，但是会减少429等错误的触发。

## 配置

修改 `config.py` 中的配置：

- `MODELS`：配置对外暴露的模型。key 是请求时使用的模型名。
- `provider`：上游接口类型，OpenAI 兼容接口填 `openai_chat`，Anthropic 接口填 `anthropic`。
- `upstream_model`：实际转发给上游的模型名。
- `api_base`：上游服务地址。
- `api_key`：上游服务的 API Key。
- `serial`：是否强制串行排队，默认建议保持 `True`。
- `delay`：每次请求转发前等待的秒数，用来降低触发 RPM 限制的概率。
- `API_KEYS`：访问本代理服务时使用的 API Key。设置为空集合 `set()` 可关闭鉴权。
- `SERVER_HOST` / `SERVER_PORT`：代理服务监听地址和端口。

示例：

```python
MODELS = {
    "my-model": {
        "provider": "openai_chat",
        "upstream_model": "gpt-4.1-mini",
        "serial": True,
        "api_base": "https://api.example.com/v1",
        "api_key": "sk-xxx",
        "delay": 0.5,
    }
}

API_KEYS = {"sk-proxy-token"}
SERVER_HOST = "0.0.0.0"
SERVER_PORT = 4002
```

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

### Configuration

Edit the settings in `config.py`:

- `MODELS`: models exposed by this proxy. The key is the model name used in requests.
- `provider`: upstream API type. Use `openai_chat` for OpenAI-compatible APIs, or `anthropic` for Anthropic APIs.
- `upstream_model`: the actual model name sent to the upstream API.
- `api_base`: upstream API base URL.
- `api_key`: upstream API key.
- `serial`: whether to force serial queueing. Keeping it as `True` is recommended.
- `delay`: seconds to wait before forwarding each request, used to reduce the chance of hitting RPM limits.
- `API_KEYS`: API keys accepted by this proxy. Set it to `set()` to disable authentication.
- `SERVER_HOST` / `SERVER_PORT`: host and port used by the proxy service.

Example:

```python
MODELS = {
    "my-model": {
        "provider": "openai_chat",
        "upstream_model": "gpt-4.1-mini",
        "serial": True,
        "api_base": "https://api.example.com/v1",
        "api_key": "sk-xxx",
        "delay": 0.5,
    }
}

API_KEYS = {"sk-proxy-token"}
SERVER_HOST = "0.0.0.0"
SERVER_PORT = 4002
```

### Start the service

```bash
python proxy_server.py
```

### Run tests

```bash
python test.py
```

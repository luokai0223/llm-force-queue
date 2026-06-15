# ==================== 配置 ====================

# 模型配置:
# - provider: "openai_chat" 使用 /v1/chat/completions
# - provider: "anthropic" 使用 /v1/messages
# - upstream_model: 转发到上游时使用的 model 名，不配置则使用下游 body.model
# - serial: 是否按模型串行排队；默认 True
# - delay: 每次转发前等待秒数；serial=False 时仍会对每个请求生效
# MODELS 的 key 是下游可请求的本地 model 名。
MODELS = {
    "deepseek-v4-flash": {
        "provider": "openai_chat",
        "upstream_model": "deepseek-v4-flash",
        "serial": True,
        "api_base": "http://192.168.106.100:4000/v1",
        "api_key": "sk-5mbs5LyhImQmn2RJTc1D2IH50RqpJP5iyC7WVNEo7XBE5zex",
        "delay": 0.5,
    },
    "deepseek-v4-flash_coding": {
        "provider": "anthropic",
        "upstream_model": "deepseek-v4-flash_coding",
        "serial": True,
        "api_base": "http://192.168.106.100:4000",
        "api_key": "sk-5mbs5LyhImQmn2RJTc1D2IH50RqpJP5iyC7WVNEo7XBE5zex",
        "delay": 0.5,
    }
}

# 对外 API Key 验证（留空则不验证）
API_KEYS = {"sk-xx"}

SERVER_HOST = "0.0.0.0"
SERVER_PORT = 4002

# 配置存储与动态刷新示例

先按项目 README 构建并启动三节点。示例只依赖 Python 标准库与 `yewukv-client`，默认连接本机 9101–9103。所有配置放在 `config/<name>` 的单个 KV 中。

```bash
python3 examples/config_store.py publish --file examples/config.json
python3 examples/config_store.py show
python3 examples/config_store.py serve --port 8080
```

在另一个终端访问：

```bash
curl http://127.0.0.1:8080/hello
curl http://127.0.0.1:8080/health
curl http://127.0.0.1:8080/config
```

修改 JSON 的 `message` 或 `enabled` 后，再执行 publish。服务默认每 0.5 秒读取一次配置，自动更新响应内容及开关，无需重启。

自定义集群、命名空间和时限：

```bash
python3 examples/config_store.py --nodes 127.0.0.1:9101,127.0.0.1:9102,127.0.0.1:9103 \
  --name checkout --timeout-ms 1500 publish --file examples/config.json
python3 examples/config_store.py --name checkout serve --poll-seconds 0.5 --stale-after 5
```

配置必须包含且仅包含 `message`（1–200 字符）、`enabled`（布尔值）、`request_timeout_ms`（1–60000 的整数）。发布前验证结构及 8192 字节的 KV value 限制，保存 schema version 与内容 SHA-256 revision，消费者重新校验后才替换整份配置。`enabled=false` 使 `/hello` 返回 503；timeout 字段作为使用方配置返回，示例不模拟下游业务请求。

默认后端的成功 GET 遵循 ReadIndex 契约；应用层通过轮询缓存，更新可见性是最终的，不能把缓存 HTTP 接口当作直接的线性一致读。超过 `--stale-after` 没有成功刷新后，`/health`、`/hello` 返回 503，`/config` 标记 stale，并保留最后一份有效配置用于诊断。启动时配置不存在或不可读会失败，避免带空配置启动。

发布成功表示 KV 已多数派提交并应用。超时后的发布结果可能未知，示例不会自动重放；可以 show 核对内容 revision 后由操作者决定下一步。多发布者采用最后一次提交覆盖，没有 CAS、版本递增、审计历史、Watch、租约或分布式锁语义。该例用于受控本机环境中的配置发布闭环。

`config_application` 集成测试实际运行发布 CLI 和 HTTP 消费者，验证动态刷新、Leader 强杀、失去多数派后配置过期、恢复、全节点重启和非法配置拒绝。

# MiniVectorDB — OceanBase 2026 加赛

本分支基于官方 `2026-competition-round2`，实现 JSONL 协议、基础 SQL、float32 向量检索、磁盘持久化、事务与进程崩溃恢复。

[运行、设计与测试说明](MINIVECTORDB.md) · [官方协议](STDIO_PROTOCOL.md) · [公开测试](mysqltest/README.md) · [真实开发与验证记录](agent/conversation.json)

```bash
docker build -t minivectordb:local .
docker run --rm -i --network=none --read-only \
  --mount type=volume,src=minivectordb-data,dst=/data minivectordb:local
```

启动后主动输出 `{"type":"ready"}`。每一行输入/输出都是 JSON，例如：

```json
{"id":1,"op":"execute","sql":"CREATE TABLE docs (id BIGINT PRIMARY KEY, embedding VECTOR(2));"}
{"id":2,"op":"execute","sql":"INSERT INTO docs VALUES (1,'[3,4]');"}
{"id":3,"op":"execute","sql":"SELECT id,L2_DISTANCE(embedding,'[0,0]') AS distance FROM docs ORDER BY distance LIMIT 10;","search_mode":"exact"}
{"id":4,"op":"shutdown"}
```

正常重启使用同一数据卷。事务示例：依次执行 `BEGIN;`、数据修改语句、查询，最后执行 `COMMIT;` 或 `ROLLBACK;`。未提交事务在退出后恢复为提交前状态。

本地与 Linux 容器已通过公开20项用例及16项额外回归。正式隐藏用例准入、性能排名与 Agent 评审尚未进行，不能把这些本地结果计作比赛得分。ANN模式当前采用精确平面索引回退，不承诺近似索引性能。

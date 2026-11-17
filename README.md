# MiniVectorDB

自行实现的单机、单进程、单客户端轻量向量数据库。通过 stdin/stdout JSONL 交互；stdout 仅包含协议结果。源码使用 C++17，通用 nlohmann/json 3.12.0 仅用于 JSON 编解码，不依赖现成数据库、存储引擎或向量索引库。

## 构建和运行

```bash
docker build -t minivectordb:local .
mkdir -p data
docker run --rm -i --network=none --cpus=2 --memory=2g --memory-swap=2g --pids-limit=128 --read-only --cap-drop=ALL --security-opt=no-new-privileges -v "$PWD/data:/data" minivectordb:local
```

默认入口直接运行数据库，持久化文件全在 /data，无需开放端口。基础镜像固定 SHA256，构建阶段安装固定 g++ 元包版本，运行镜像不联网。源代码通过 Dockerfile 编译，未提交预编译程序。Linux 本地也可运行 make，然后 build/minivectordb --data-dir ./data。

## 协议示例

程序先输出 `{"type":"ready"}`。随后每行发送一个请求并等待同 id 的响应：

```json
{"id":1,"op":"execute","sql":"CREATE TABLE docs(id BIGINT PRIMARY KEY,content VARCHAR(16),embedding VECTOR(2));","search_mode":"exact"}
{"id":2,"op":"execute","sql":"INSERT INTO docs VALUES(1,'蓝雨伞','[3,4]');","search_mode":"exact"}
{"id":3,"op":"execute","sql":"SELECT id,L2_DISTANCE(embedding,'[0,0]') AS distance FROM docs ORDER BY distance LIMIT 1;","search_mode":"exact"}
{"id":4,"op":"shutdown"}
```

第三个请求返回列 id、distance，行 [1,5.0]。所有请求响应后立即刷新 stdout。错误返回 ok=false 和非空 error.code，之后继续接受请求。shutdown 回滚未提交事务，响应后以0退出。

## 实现范围

支持 BIGINT、VARCHAR(n)、VECTOR(n)、一列 BIGINT 主键、每表最多一列向量、建表、可指定列顺序的多行 INSERT、SELECT、常量 UPDATE、DELETE、普通比较与 AND（含括号）、ORDER BY、LIMIT/OFFSET、L2_DISTANCE、CREATE VECTOR INDEX、BEGIN/COMMIT/ROLLBACK。

VARCHAR 按有效 Unicode 码点计长，UTF-8 字节序与有效 Unicode 码点顺序一致。向量写入时转换为有限 float32，距离按存储后的数值使用双精度累加。普通排序和向量距离并列均按主键升序。

向量索引为自行实现的磁盘映射扁平索引；精确扫描配合有界 TopK 堆和运行时 AVX2 分派，过滤在候选选择前应用。exact 与 ann 均返回精确结果，未实现 HNSW 等近似算法。事务内索引读取当前可见记录；修改与回滚使派生索引失效，提交后修正日志地址。持久化索引定义在恢复就绪前完成必要重建。

不支持 NULL、JOIN、聚合、子查询、ALTER TABLE、外键、嵌套事务、主键更新及一般算术表达式；相应语句返回错误。单次结果受协议16 MiB限制。主键到磁盘位置的元数据按记录数占用内存，记录内容和向量无需全部驻留内存。

## 持久化与恢复

每条修改先完整解析并校验，语句失败不会保留部分变更；显式事务保留前面成功语句。未提交操作写在独立 pending.txn，内存只保存位置和回滚信息。提交将操作装入带长度、校验和与完整结束标记的 WAL 帧，fdatasync 成功后才返回成功。启动仅重放完整已提交帧，清理未完成尾部，未提交事务不进入可见状态。恢复不改写已提交帧，反复中断可重复执行。进程强杀模型下已确认提交的事务保留。

## 验证

官方 protocol_check/ 与 mysqltest/ 保留原始内容，按固定官方提交 f4d4e26653af823a7601668802f80e374ad6643c 下载并逐个 Git blob 校验。

```bash
python3 protocol_check/run.py --image minivectordb:local --output protocol-results/public
```

已在实际源码镜像和官方限制下通过20个公开用例、95条SQL。独立状态与距离真值验证还包含800次TopK比较、提交与未提交强杀、残缺日志尾部及恢复中再次强杀。validation/ 下的脚本为额外开发验证，不修改官方用例。evidence/ 保存原始结果。

资源验证：2 GiB限制内写入2,241,876,675字节已提交日志，强杀未提交更新后重启，64,000条记录正确恢复，恢复约4.86秒，未发生OOM。

开发基准：10,000条128维、每批100条提交、1,000次Top10（半数等值过滤），无过滤与有过滤召回均为1.0、距离正确。耗时和进程RSS详见证据；这些结果不是正式cgroup性能指标或排名，也不能替代29例正式准入。

## 学习材料

agent/conversation.json 与根目录 conversation.json 内容相同，记录真实用户要求、Agent负责的实现和实际工具结果。用户未提供技术预测或独立实验解释，材料明确说明，不将Agent分析冒充学生发言。源码、Dockerfile和这份材料来自同一次提交。

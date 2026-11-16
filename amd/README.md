# hipVS 加速 seekdb 原生 HNSW 构建

本实现将密集 L2 HNSW 快照构建中的图计算交给 AMD Radeon GPU 和 hipVS，生成完整 HNSW 层级后导入 seekdb 自带的 VSAG。SQL 查询、过滤、增删更新、事务和索引持久化继续使用 VSAG 原生实现。

## 环境和运行条件

验证环境为 AMD Radeon PRO W7900 / gfx1100、ROCm 7.2.4、比赛 seekdb-hipvs 模板，Linux x86_64，Python 3.12。hipVS 需要提供 `cuvsHnswFromCagra` 和 GPU hierarchy API。

原生 GPU 构建范围为 `TYPE=HNSW, LIB=CUVS, DISTANCE=L2`，M 在 32–64、无 extra_info、行数至少 4096。其他原生构建参数走 CPU；GPU 不可用或构建失败也回退 CPU，不发布不完整图。验证使用 M=32、ef_construction=200。

## 从源码编译

先按 seekdb 项目说明安装 `deps/3rd`、Clang 17 和固定 Rust 1.98.1。公开依赖需要可访问的 Cargo 包源；网络受限时可使用锁定版本的公开下载缓存。无需使用作者本地文件或私有服务。

```bash
bash tools/hipvs_bridge/build.sh "$PWD/libseekdb_cuvs_bridge.so"
bash build.sh release \
  -DOB_BUILD_CUVS=ON \
  -DCUVS_BRIDGE_LIB="$PWD/libseekdb_cuvs_bridge.so" \
  -DOB_MAX_UNITY_BATCH_SIZE=4 --make -j6
```

GPU 功能默认关闭。普通 CPU 编译不需要 hipVS。启动 GPU 版本时确保动态库可见，例如：

```bash
export LD_LIBRARY_PATH="$PWD:/opt/hipvs/lib:/opt/rocm/lib:${LD_LIBRARY_PATH:-}"
mkdir -p "$PWD/amd/runtime"
build_release/src/observer/seekdb \
  --base-dir="$PWD/amd/runtime" --port=12882 --nodaemon \
  --parameter memory_limit=4G --parameter log_disk_size=2G
```

动态库不能在 seekdb 正在使用时覆盖；更新后重启进程。该目录只是验证实例，业务部署应选择自己的目录和资源配置。

## SQL 使用

```sql
CREATE TABLE documents(id BIGINT PRIMARY KEY, embedding VECTOR(128));
-- 先插入数据，再创建索引，才能测量离线快照构建。
CREATE VECTOR INDEX idx ON documents(embedding)
WITH (distance=l2, type=hnsw, lib=cuvs,
      m=32, ef_construction=200, ef_search=512);

-- 实际查询搜索预算由此会话变量控制。
SET SESSION ob_hnsw_ef_search=512;
SELECT id FROM documents
ORDER BY l2_distance(embedding, '[填写128维查询向量]') APPROXIMATE LIMIT 10;
```

内核日志出现 `HIPVS_NATIVE_HNSW_BUILD_COMMITTED`，表示 GPU 生成的图已经成功导入原生索引。启用 GPU 库、声明 lib=cuvs 或 Notebook 中的批量查询加速本身，都不能替代该构建链路证据。

## 真实验证结果

两万行、128维、同一持久化 SQL 数据、双方 M=32、ef_construction=200、查询预算512，分别通过真实 `CREATE VECTOR INDEX` 计时，包含 SQL DDL、构建和发布。三轮结果：

| 轮次 | CPU VSAG / 秒 | GPU hipVS / 秒 |
| --- | ---: | ---: |
| 1 | 7.0615 | 3.2852 |
| 2 | 7.0307 | 2.6506 |
| 3 | 7.0304 | 3.0155 |

中位构建加速约 2.33 倍。20个固定随机查询中，双方 Recall@10 均为1.0；非连续SQL主键映射、更新回滚、建成后插入/删除、进程重启后的图恢复、VECTOR INDEX执行计划与SQL过滤均通过。另在对该进程禁用GPU可见性的空白实例中，5000行 `lib=cuvs` 建索引及ANN查询成功，验证CPU回退。

这些是本机实测，不能替代平台正式评分，也不保证其他数据、参数或设备获得同样收益。十万行组件级公平测试为3.39倍，但它不包含SQL内核链路；早期不同度数和未达召回门槛的报告也保存在证据中，不作为最终SQL加速结论。

## 工程实现

`src/observer/vector_index/ob_plugin_vector_index_adaptor.cpp` 在已有快照事务构建管道中收集向量，再批量执行GPU构建；业务增量写入保持原路径。`src/oblib/lib/vector/ob_vsag_adaptor.cpp` 在内存中校验并导入GPU图，成功后不重复构建CPU图；失败保留空索引供CPU构建。`tools/hipvs_bridge/seekdb_cuvs_hnsw_bridge.cpp` 负责hipVS资源、层级转换、原生序列化、SQL向量ID映射和清理。

GPU工作统一运行在标准栈线程中，避免ROCm即时编译的深调用占满DDL线程的小栈。临时文件通过mkstemp生成并在所有返回路径清理，不使用用户输入作为临时文件名。

## 材料来源

基于官方比赛分支 `5bce9e06d1281347ad61344e4b01fab9158c58a2`。复用 AMD 公开 PR https://github.com/oceanbase/seekdb/pull/1314 的 opt-in DDL、构建开关和原有GPU检索桥接，来源修订 `aadf910a6dae3b51ebc9f7cc61c3e3fe6bdd1a53`。本提交将相关改动移植到冻结版接口，并新增原生HNSW批量构建、ID映射、标准栈执行与CPU回退。上游许可证继续保留，完整来源及验证证据见本目录。

## 自动复现

在已编译并启动的数据库旁执行：

```bash
python3 -m pip install -r amd/requirements.txt
python3 amd/verify_sql.py --port 12882 --rows 20000 --trials 3 --output amd_sql_report.json
```

脚本会在独立验证数据库中创建带随机后缀的表，双方使用相同向量、主键、M、ef_construction和会话搜索预算。原始数据在索引创建前通过SQL写入；计时只包围真实CREATE VECTOR INDEX。还会核对ANN召回、ID映射和原生执行计划。脚本不修改业务表；验证表保留供检查。

5000行小数据验证中，GPU构建约3.34秒，CPU约1.54秒，GPU没有加速；该结果也保存在证据中。应按实际数据规模衡量收益，不能把每次lib=cuvs构建都称为更快。运行测试不会自动声称使用了GPU，需同时核对数据库内核提交标记。

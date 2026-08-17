# 入湖流水线控制台（真实运行版）

本地前端和 Python 后端直接接入正式入湖项目：

- 正式项目目录：D:codepythonpythonProjget_ddl
- 正式流水线：run_pipeline.py
- 正式任务文件：resourceFile
esource.text
- 正式系统映射：resourceFile用户填写系统标准映射.txt
- 正式脚本映射：resourceFilejob_py_script_mapping.txt
- 正式 CLOB 白名单：resourceFileclob_tables.txt

## 启动

双击 start-console.cmd，然后访问：

    http://127.0.0.1:8080

也可以在当前目录手动运行：

    python server.py

## 安全机制

- 服务默认监听 `0.0.0.0`，可供局域网访问；当前为 HTTP 且无登录鉴权，只应部署在可信内网。
- 同一时间只允许运行一个真实流水线。
- 启动真实流水线前，页面会列出任务并要求二次确认。
- 支持数据源管理：现有正式数据源只读展示，新数据源统一注册、删除并验证 MySQL/Oracle 连接及数据库或 Schema。
- 新数据源密码使用当前 Windows 用户的 DPAPI 加密，保存在正式 `resourceFile/managed_data_sources.json` 中，读取接口不返回密码或密文。
- 正式 `run_pipeline.py` 在启动时动态加载托管数据源的前置校验配置、系统别名和初始化脚本映射，不再要求为新源重复修改 `DB_CHECK_CONFIGS` 和两份映射文件。
- 测试/生产初始化脚本仍承载 StarRocks Resource、ODS 命名、主键及 SeaTunnel 拆分等业务规则；管理页只在脚本真实存在且连接验证通过时标记为“已就绪”。
- 支持按业务系统选择多个数据源、多个库和多张表，表名支持模糊搜索及中英文逗号或换行分隔。
- 单库数据源生成任务时省略库名前缀，多库数据源保留 `库名.表名`，避免正式初始化脚本重复拼接默认库名。
- 可视化选表只追加到页面任务列表，不会自动保存或启动流水线。
- CLOB 白名单使用 `系统.Schema.表名` 格式，避免不同系统或 Schema 的同名表发生冲突。
- 流水线运行及停止期间禁止修改任务与映射文件。
- 支持在页面手动停止当前流水线；停止时会终止 `run_pipeline.py` 及其派生进程。
- 某张表任一阶段失败或中断后，可在运行监控中仅选择该失败任务重跑；为保证依赖顺序，重跑仍执行该表的完整流水线。
- 手动停止不会回滚已经完成的建表、配置上传或 SeaTunnel 操作，任务文件会保留以便检查和重试。
- 后端以原子写入方式更新配置文件。
- 运行日志来自正式 run_pipeline.py；完整日志仍由正式脚本写入其日志目录。
- 最近 100 次运行记录持久化到 `.runtime/pipeline-history`，单次历史日志最多保留末尾 1 MiB。
- 若控制台在流水线运行期间异常退出，对应历史记录会标记为“终态未知”，不会误判流水线已经停止。

## 接口

- GET /api/health：检查正式脚本和 Python 环境
- GET /api/catalog：读取业务系统、数据源和可选库目录
- GET /api/source-management：读取脱敏后的业务系统和数据源管理列表
- POST /api/source-management/systems：新增业务系统
- POST /api/source-management/systems/delete：删除空的托管业务系统
- POST /api/source-management/sources：新增托管数据源
- POST /api/source-management/sources/update：更新托管数据源
- POST /api/source-management/sources/validate：验证托管数据源连接与数据库或 Schema
- POST /api/source-management/sources/delete：删除未被任务引用的托管数据源
- POST /api/catalog/tables：按多个数据源和库查询、模糊匹配源表
- GET/POST /api/tasks：读写正式 resource.text
- GET/POST /api/mapping：读写三份正式映射配置
- POST /api/pipeline/run：启动真实 run_pipeline.py
- POST /api/pipeline/stop：按运行 ID 手动停止当前流水线进程树
- GET /api/pipeline/status：读取真实运行日志、退出码和逐表结果
- GET /api/pipeline/history：读取最近 100 次运行摘要
- GET /api/pipeline/history/{runId}：读取指定运行的任务、逐表结果和历史日志

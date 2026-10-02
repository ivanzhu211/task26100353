# skillrecalc — 学员技能画像后台重算模块

纯 Python + 单文件 SQLite，无外部服务依赖。录入进程与多个重算工作进程是各自独立的操作系统进程，通过同一个 SQLite 文件（WAL 模式）协作。

## 库用法（培训平台上层代码调用）

```python
import skillrecalc as sr
from skillrecalc.db import connect

sr.init_db("/var/lib/platform/skillrecalc.db")   # 首次初始化
db = connect("/var/lib/platform/skillrecalc.db")

# 录入完成记录（<100ms，同事务把学员标记为待重算）
sr.record_completion(db, "learner-001", "article-42", score=91)   # 无测验时不传 score

# 创建/修改文章（难度 + 标签）；修改后学过它的学员自动进入重算队列
sr.upsert_article(db, "article-42", sr.Difficulty.EXPERT, ["高压", "安全"])

# 查询画像（单学员要么全旧要么全新，不会半新半旧）
profile = sr.get_profile(db, "learner-001")

# 死信处理
for d in sr.list_dead_letters(db):
    print(d.learner_id, d.reason)
sr.requeue_dead_letter(db, "learner-001")   # 管理员放回，从头计数
```

## 启动重算工作进程

```bash
pip install .
skillrecalc-worker --db /var/lib/platform/skillrecalc.db
# 或不安装直接运行：
PYTHONPATH=src python3 -m skillrecalc --db /var/lib/platform/skillrecalc.db
```

可同时启动任意多个工作进程。常用参数：`--lease-seconds`（默认 30，超时由别的进程接管）、`--max-attempts`（默认 5，连续失败进入死信）、`--backoff-base`（重试间隔基数，按 2 的幂递增）、`--batch-size`、`--once`（队列清空后退出）。

## 可靠性设计

- 完成记录与"待重算"标记在同一事务写入，录入成功的记录不会丢。
- 等待中的学员重复标记不增加次数；重算进行中来的新记录只触发一次补算。
- 工作进程凭租约认领学员；进程被杀或卡住超过租约时间由其他进程接管，并计一次失败。
- 画像写回前校验租约令牌，卡住的进程醒来后无法覆盖别人用更新数据算出的结果；单个学员的画像在单事务内整体替换。
- 连续失败 5 次进入死信（含最后一次失败原因），不再自动重试；死信中学员的新记录仍被保存，管理员放回后重算会覆盖全部记录。

## 测试

```bash
python3 -m pytest tests/
```

覆盖：多进程混沌（持续录入/改文章 + 随机杀进程/失败，最终与从头重算完全一致）、卡住进程写回被丢弃、恰好第 5 次失败进死信及放回恢复、性能基线（10k 学员入队 <1s、录入 <100ms、4 进程一分钟内清空 1 万积压）。

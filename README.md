# skillprofile — 学员技能画像后台重算模块

纯 Python + SQLite 的库，供培训平台上层代码直接调用；另附一个启动后台
重算工作进程的命令行入口。无接口层、无外部服务，所有数据（完成记录、
文章、画像、重算队列）都在同一个 SQLite 文件里，录入进程与多个工作
进程可以是各自独立的操作系统进程。

## 库用法（上层平台调用）

```python
import skillprofile as sp

conn = sp.connect("/data/platform.db")   # 各进程各自打开自己的连接

# 录入一条完成记录（score=None 表示该模块没有测验），<100ms 返回
sp.record_completion(conn, "learner-001", "article-42", score=92)

# 管理员修改文章难度/标签；学过它的学员自动进入重算队列，<1s 返回
sp.upsert_article(conn, "article-42", sp.Difficulty.EXPERT, ["safety", "relay"])

# 读取画像：按标签给出已掌握文章数与最高难度（整体旧或整体新）
profile = sp.get_profile(conn, "learner-001")   # list[TagStat]

# 死信管理
dead = sp.list_dead(conn)          # list[DeadLetter]，含最后一次失败原因
sp.revive(conn, "learner-001")     # 管理员放回：从头计数，补算全部记录
```

掌握判定：某篇文章至少一条不带分记录，或任意一次得分 ≥ 80。

## 启动工作进程

```bash
skillprofile-worker --db /data/platform.db
# 或
python -m skillprofile --db /data/platform.db \
    --lease-seconds 30 --poll-interval 0.2 \
    --base-backoff 1 --max-backoff 60
```

可同时启动任意多个工作进程。`--once` 表示清完当前可认领的积压后退出。

## 可靠性设计

- **不丢记录**：录入与入队在同一事务；画像重算以 completions 表为准，
  队列只负责调度。
- **租约接管**：工作进程被认领的学员带租约（默认 30s）。进程被杀或卡住
  超过租约，其他进程接管并记一次失败。
- **栅栏令牌**：每次认领递增 fence；卡住的进程醒来后写回时 fence 已变，
  整个写事务作废，不会覆盖别人用更新数据算出的画像。
- **退避与死信**：失败按 1s/2s/4s/…（上限 60s）退避重试；连续 5 次失败
  （含进程被杀、卡住）进入死信，不再自动重试；成功一次计数清零。
- **原子画像**：单个学员的画像在一个事务里整体替换，读到的要么全旧、
  要么全新。
- **去重**：等待中的学员重复入队不增加次数；重算进行中来的新记录只
  置 dirty 标记，本次结束后再补算一次。

## 测试

```bash
python -m pytest tests/ -q
```

覆盖：业务规则单元测试；多进程混沌测试（持续录入/改文章 + 随机杀进程
+ 注入失败与卡住，最终与从头算一致）；卡住进程写回防护；恰好第 5 次
失败进死信及放回恢复；4 进程一分钟清 1 万积压的吞吐验收。

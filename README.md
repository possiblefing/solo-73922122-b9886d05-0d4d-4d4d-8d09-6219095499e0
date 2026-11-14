# 公共采购公告采集系统

面向招标数据员的公告采集交付件：FastAPI 采集器 + 带限流/延迟开关的本地模拟采购站，
Docker Compose 一键启动，浏览器即可启动采集、查看实时进度、停止任务并下载 JSON。

## 目录结构

```
.
├── docker-compose.yml          # 两服务编排：procurement-site + collector
├── collector/                  # 采集器（FastAPI + httpx + BeautifulSoup）
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app/
│       ├── main.py             # API、JSON 下载、采购站同源代理、静态页挂载
│       ├── crawler.py          # 采集引擎：并发闸门、429 全站冷却、停止回收
│       ├── parser.py           # BeautifulSoup 列表/详情解析（失败抛 ParseError）
│       └── static/index.html   # 浏览器控制台（单文件，原生 JS）
├── site/                       # 本地模拟公共采购站
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app/
│       ├── main.py             # 栏目/详情页、滑动窗口限流、延迟开关、管理页
│       └── data.py             # 确定性内存数据集（含跨栏目重复编号与 1 个坏详情页）
└── tests/                      # 契约测试与解析器单测（标准库 + venv 依赖）
```

## 启动

```bash
docker compose up --build
```

| 入口 | 地址 | 说明 |
| --- | --- | --- |
| 采集控制台 | http://localhost:8000/ | 启动/停止/进度/事件/下载 JSON |
| 采购站（同源代理入口） | http://localhost:8000/site/ | 浏览器入口，经采集器反代到站点 |
| 采购站限流管理 | http://localhost:8000/site/admin | 限流阈值、窗口、延迟毫秒开关 |
| 采购站直连 | http://localhost:8080/ | 仅排障时使用 |

## 采集规则与实现对照

| 需求 | 实现 |
| --- | --- |
| 固定站点两个栏目，遍历分页列表及详情 | `Crawler` 配置 `/tenders`、`/awards`；首页解析 `data-total-pages` 后**只调度下一页**，逐页推进，不预排全部分页 |
| 首页标出几十万页时不一次占满内存 | 列表页惰性调度：每页最多排入"本页详情 + 下一页列表"；内存/进度只随实际工作量增长，绝不随站点标称页数增长 |
| 已请求未成功的页保留页码与尝试次数 | `failed_pages[]`：每项含栏目、类型、页码/公告编号、来源列表页、URL、原因、`attempts` |
| 未请求页按可定位连续页段与数量报告 | `unrequested_pages.ranges[]`：`start_page`–`end_page` + `count`（站点未给出总页数时 `end_page/count` 为 `null`，仍给出起始页）；详情按来源列表页聚合为一段并带编号 |
| 按公告编号去重 | 集合 `_seen_codes` 跨栏目去重；同编号只采一次详情，跳过计数在分栏目进度中可见 |
| 提取标题、采购单位、截止日期 | `parser.parse_detail_page`，缺任一字段即 `ParseError` |
| 可下载 JSON | `GET /api/records/download`，附件文件名带轮次 ID |
| 单进程同时只采一轮 | 单 uvicorn worker + `round_lock`；重复启动返回 `409` |
| 最多两个请求在途 | 全站共享 `asyncio.Semaphore(2)`，两个栏目共用 |
| 429 按 Retry-After 整数秒全站暂停 | `_extend_cooldown` 维护单一冷却钟，`_pass_gate` 拦截所有后续请求（含另一栏目与重试）；后续 429 只延长不缩短；已在途请求允许收尾 |
| 每页最多尝试三次 | 列表/详情统一 `_get_parsed_page`，传输错误、4xx/5xx、解析失败均消耗尝试次数 |
| 耗尽/解析失败须列出未完成页，不能报整轮成功 | 已请求失败页进入 `failed_pages`，未请求页进入 `unrequested_pages`；任一存在则状态为 `partial`，`success=false` |
| 停止受理后不再发送新请求 | `stop_round` 先同步置停止位；发请求前的停止检查与任务登记之间无 `await`，不会再有请求越过停止位 |
| 主动中止并回收在途请求后才显示已停止 | 每个请求是独立可取消任务；`stop_round` 置位后 `cancel()` 在途任务、`aclose()` 连接池并 `await` 轮次收尾，之后才变 `stopped` |
| 停采不能显示成功 | 状态 `stopped` 时 `success` 恒为 `false`，JSON 下载同样如此 |
| 旧响应不能写入新轮 | 每轮生成 `round_id`，响应解析后、回写前校验；新轮重建客户端、状态与记录容器 |
| 网页与下载 JSON 都区分失败页/未请求页 | `/api/state` 与 `/api/records/download` 均返回 `failed_pages[]` 与 `unrequested_pages{count,ranges[]}` |

轮次状态机：`idle → running → completed | partial | stopped`（停止过程中短暂为 `stopping`）。
`unrequested_pages.count` 是各页段数量之和；若存在站点未披露总页数的页段，则该值为下限，
并以 `unrequested_count_incomplete=true` 标出（网页同步提示），不会为这些页虚构页码。
只有 `completed` 且 `success=true` 才算成功；`partial` 表示有失败页或未请求页；`stopped` 永不代表成功。

未完成内容的两种口径（网页与下载 JSON 相同）：

- `failed_pages[]`：**已请求但未成功**，保留页码/编号与尝试次数（`attempts`）；
- `unrequested_pages.ranges[]`：**从未请求**，按连续页段与数量报告，例如
  `{"page_type":"list","start_page":2,"end_page":200000,"count":199999}`，不会逐页生成记录。

列表页按栏目顺序推进：某页失败后不再调度其后续页，因此"失败页之后的剩余分页"始终是一段可定位、
可计数的连续区间。已成功的页不会重复请求；跨栏目编号去重仍在入队详情前完成。

## 模拟采购站说明

- 数据集：36 条招标公告（6 个列表页）+ 24 条中标公告（4 个列表页），
  中标公告复用前 24 个招标编号，用于验证跨栏目去重；
- `ZB-2026-010` 详情页故意缺少“采购单位/截止日期”，用于验证三次解析失败后
  进入未完成页面列表、整轮标记为 `partial`；
- 限流为滑动窗口，默认 10 秒窗口 12 个请求，超限返回 `429` 与整数 `Retry-After: 5`；
- `/admin` 可运行时切换限流开关、窗口容量、窗口秒数与每请求延迟毫秒，以及
  **列表总页数覆盖**（0 = 按数据集；填 `200000` 即让列表宣称 20 万页），保存即重置计数，
  也可单独重置计数；环境变量 `SITE_TOTAL_PAGES` 设置启动初值；
- 页码超出数据集时仍返回一页确定性数据（编号会重复），用于验证超大页数下的去重与按需调度；
  页导航只渲染首尾与当前页附近少量链接，不会渲染几十万个锚点。

## 推荐演示流程

1. `docker compose up --build`，打开 http://localhost:8000/ ；
2. 直接点“启动一轮采集”：观察到 `partial`——35 条记录、1 个坏详情页在未完成列表中；
3. 在 `/site/admin` 把窗口容量调到 3：再启动一轮，观察控制台“全站冷却剩余”倒计时，
   冷却期间两个栏目均无新请求完成；
4. 冷却进行中点“停止”：状态在 1 秒内变为 `stopped`，页面按“失败页 / 未请求页段”分别列出；
5. 在 `/admin` 把“列表总页数覆盖”设为 `200000`，再启动一轮：进度中的“已调度页面”保持很小，
   查询与停止依旧即时；停止后未请求分页以连续页段（如 `2 – 200000，199999 页`）报告；
6. 立即再启动一轮：状态与记录完全重置，证明旧轮响应不会写入新轮；
7. 点“下载 JSON”获取结构化结果（`failed_pages` 与 `unrequested_pages` 分开，`success=false` 表示停采）。

## 本地测试（无需 Docker）

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r collector/requirements.txt
.venv/bin/python -I tests/test_parser.py
.venv/bin/python -I tests/test_crawler_contract.py
.venv/bin/python -I tests/test_pagination_lazy.py
.venv/bin/python -I tests/test_site_e2e.py
```

- `test_crawler_contract.py`：在途请求峰值恰为 2、429 后整站等待不少于 Retry-After 且重试可恢复、
  冷却中停止后零请求泄漏、停止后新轮干净无旧数据；
- `test_pagination_lazy.py`：20 万页标称下调度量保持很小、失败页与未请求页段严格分离、
  停止时未请求详情按段聚合、首轮即停止时未请求区间从可定位页开始且数量未知；
- `test_site_e2e.py`：同一事件循环内启动真实采购站（`SITE_TOTAL_PAGES=200000`）与采集器，
  验证网页入口/同源代理、进度查询与停止的响应时间、下载 JSON 的 `success` 与两类未完成清单。

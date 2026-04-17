# BOFA_2026_Code_Academy

## bonds
https://docs.google.com/spreadsheets/d/14V14XBP0ffwWG6vGDt-fdmB9ozHbzys-fkXu8QHxOwE/edit?usp=sharing

## events
https://docs.google.com/spreadsheets/d/1Mmdxrpq4zE89HDPFgBLHOrnxPBMa6tq0tq2Ijke7KPE/edit?usp=drivesdk


## claude code
代码结构说明

main.py              ← 入口：读CSV、组装流程、打印结果
interest_handler.py  ← 计算 Accrued Interest（纯函数）
price_handler.py     ← 计算 Dirty Price（纯函数）
position_handler.py  ← PositionTracker 类，管理 BUY/SELL 状态
pv_handler.py        ← PVTracker 类，计算 PV、记录 PV 变化、P&L 查询
三人分工方案（下一步）
Person A — 计算层（最简单，可以最先完成）

interest_handler.py — calc_accrued_interest()
price_handler.py — calc_dirty_price()
Person B — 状态管理（最复杂）

position_handler.py — PositionTracker 类
负责 BUY/SELL 累加、按 desk/trader 分组的 position
Person C — PV 逻辑 + 查询

pv_handler.py — PVTracker 类，P&L since EventID
可同时协助写 main.py 中的 query 函数
关键接口约定（三人开始前对齐）：

process_event(event: dict) — event 的 key 是 bond_id, buy_sell, quantity, desk, trader
calc_accrued_interest(coupon, months_since_coupon) → float（coupon 是小数，如 0.05）
pv_tracker.record(event_id, bond_id, pv) → float（返回 pv_change）
接口锁定后，三人可以完全并行开发，最后 main.py 胶水代码把四个 handler import 进来即可。

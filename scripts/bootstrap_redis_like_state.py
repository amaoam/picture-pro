#!/usr/bin/env python3
"""将 MySQL 导出的点赞关系 CSV 初始化到 V3 Redis 命名空间。

必须在停止点赞写流量后运行。每行 CSV 至少需要 target_id,user_id 两列。
此脚本只处理导入，不会读取 MySQL，也不会碰 picture:like:v3 以外的 Redis key。
"""
from __future__ import annotations

import argparse
import csv
import sys
import zlib
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, Tuple

try:
    import redis
except ImportError:
    print("缺少 redis-py，请先运行: py -m pip install redis", file=sys.stderr)
    raise SystemExit(2)


def shard_for(target_type: str, target_id: str, shard_count: int) -> int:
    """与 Java LikeRedisKeyFactory 使用同一 CRC32 算法，保证迁移后 key 对齐。"""
    payload = f"{target_type}:{target_id}".encode("utf-8")
    return zlib.crc32(payload) % shard_count


def target_base(prefix: str, target_type: str, target_id: str, shard_count: int) -> str:
    shard = shard_for(target_type, target_id, shard_count)
    return f"{prefix}:{{like-{shard:02d}}}:target:{target_type}:{target_id}"


def scan_and_delete_namespace(client: redis.Redis, prefix: str) -> int:
    """仅删除本 V3 的 prefix；需要显式 --reset 才会被调用。"""
    count = 0
    batch = []
    for key in client.scan_iter(match=f"{prefix}:*", count=1000):
        batch.append(key)
        if len(batch) >= 1000:
            count += client.delete(*batch)
            batch.clear()
    if batch:
        count += client.delete(*batch)
    return count


def rows_from_csv(path: Path) -> Iterable[Tuple[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        required = {"target_id", "user_id"}
        if not reader.fieldnames or not required.issubset(set(reader.fieldnames)):
            raise ValueError(f"{path} 必须包含表头 target_id,user_id；当前表头={reader.fieldnames}")
        previous_identity = None
        for line_number, row in enumerate(reader, start=2):
            target_id = (row.get("target_id") or "").strip()
            user_id = (row.get("user_id") or "").strip()
            if not target_id.isdigit() or not user_id.isdigit():
                raise ValueError(f"{path}:{line_number} 的 target_id/user_id 必须是正整数")
            # 0 不是合法的业务雪花 ID；及时拒绝，避免把无效关系导入 Redis。
            identity = (int(target_id), int(user_id))
            if identity[0] <= 0 or identity[1] <= 0:
                raise ValueError(f"{path}:{line_number} 的 target_id/user_id 必须大于 0")
            if previous_identity is not None and identity == previous_identity:
                raise ValueError(f"{path}:{line_number} 出现重复关系 {identity}，请检查导出文件")
            if previous_identity is not None and identity < previous_identity:
                raise ValueError(f"{path}:{line_number} CSV 没有按 target_id,user_id 升序排列，请重新导出")
            previous_identity = identity
            yield target_id, user_id


def import_file(client: redis.Redis, path: Path, target_type: str, prefix: str,
                shard_count: int, batch_size: int) -> int:
    """将关系写入 Set，并重建每个目标的计数。前提是命名空间已清空。"""
    pipe = client.pipeline(transaction=False)
    pending = 0
    imported = 0
    for target_id, user_id in rows_from_csv(path):
        base = target_base(prefix, target_type, target_id, shard_count)
        # 关系表应有唯一约束。因导入前要求清空 V3 namespace，每一行会使计数 +1。
        pipe.sadd(f"{base}:users", user_id)
        pipe.incr(f"{base}:count")
        pending += 2
        imported += 1
        if pending >= batch_size * 2:
            pipe.execute()
            pending = 0
    if pending:
        pipe.execute()
    return imported


def main() -> int:
    parser = argparse.ArgumentParser(description="Bootstrap the V3 Redis-like state from exported CSV files")
    parser.add_argument("--redis-url", default="redis://127.0.0.1:6380/0")
    parser.add_argument("--post-csv", required=True, type=Path)
    parser.add_argument("--picture-csv", required=True, type=Path)
    parser.add_argument("--shards", default=16, type=int)
    parser.add_argument("--prefix", default="picture:like:v3")
    parser.add_argument("--batch-size", default=1000, type=int)
    parser.add_argument("--reset", action="store_true", help="删除 prefix:* 并重建；会清除该 V3 命名空间")
    args = parser.parse_args()

    if args.shards < 1 or args.shards > 256:
        parser.error("--shards 应在 1 到 256 之间")
    if args.batch_size < 1:
        parser.error("--batch-size 必须大于 0")
    if not args.prefix.strip() or any(char in args.prefix for char in "*?[]\\"):
        parser.error("--prefix 不能为空，也不能包含 Redis SCAN 通配符或反斜杠")
    if not args.post_csv.is_file() or not args.picture_csv.is_file():
        parser.error("post-csv 和 picture-csv 必须是存在的 CSV 文件")
    if not args.reset:
        parser.error("为避免重复计数，必须显式提供 --reset；请先确认停写并备份")

    client = redis.Redis.from_url(args.redis_url, decode_responses=True, socket_timeout=5)
    client.ping()
    print(f"Redis 连接成功。即将删除命名空间 {args.prefix}:* 并导入数据。")
    print("警告：请确认点赞写流量已暂停，且目标 V3 命名空间不被其他业务使用。")
    deleted = scan_and_delete_namespace(client, args.prefix)
    print(f"已删除 {deleted} 个旧 key。开始导入关系快照...")

    post_count = import_file(client, args.post_csv, "POST", args.prefix, args.shards, args.batch_size)
    picture_count = import_file(client, args.picture_csv, "PICTURE", args.prefix, args.shards, args.batch_size)
    # ready 标记最后写入；应用在启动 redis-stream 模式时会 fail-fast 检查该标记。
    client.set(f"{args.prefix}:meta:ready", "1")
    print(f"导入完成：帖子点赞关系 {post_count} 条，公共图片点赞关系 {picture_count} 条。")
    print("已写入 meta:ready。注意：仍必须先核对 Redis SCARD/GET 与 MySQL COUNT(*)，确认一致后才切换应用模式。")
    return 0


import traceback  # 确保导入了 traceback

if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001 - 启动脚本需要把 CSV/Redis 错误打印给操作者
        print(f"导入失败，捕获到异常：{exc}", file=sys.stderr)
        traceback.print_exc()  # 👈 打印完整的错误堆栈和行号
        raise SystemExit(1) 
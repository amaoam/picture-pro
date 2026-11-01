#!/usr/bin/env python3
"""从 MySQL 导出的点赞关系 CSV 初始化 RabbitMQ V1 专用 Redis namespace。

输入 CSV 表头必须是 target_id,user_id；按 target_id,user_id 升序，不允许重复关系。
只有在停止点赞写流量并确认目标 namespace 可清空时才能使用 --reset。
"""
from __future__ import annotations

import argparse
import csv
import sys
import zlib
from pathlib import Path
from typing import Iterable, Tuple

try:
    import redis
except ImportError:
    print("缺少 redis-py，请运行：py -m pip install redis", file=sys.stderr)
    raise SystemExit(2)


def shard_for(target_type: str, target_id: str, shards: int) -> int:
    return zlib.crc32(f"{target_type}:{target_id}".encode("utf-8")) % shards


def base_key(prefix: str, target_type: str, target_id: str, shards: int) -> str:
    shard = shard_for(target_type, target_id, shards)
    return f"{prefix}:{{like-{shard:02d}}}:target:{target_type}:{target_id}"


def registry_key(prefix: str, shard: int) -> str:
    return f"{prefix}:{{like-{shard:02d}}}:targets"


def scan_delete_namespace(client: redis.Redis, prefix: str) -> int:
    deleted = 0
    batch = []
    for key in client.scan_iter(match=f"{prefix}:*", count=1000):
        batch.append(key)
        if len(batch) >= 1000:
            deleted += client.delete(*batch)
            batch.clear()
    if batch:
        deleted += client.delete(*batch)
    return deleted


def csv_rows(path: Path) -> Iterable[Tuple[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        required = {"target_id", "user_id"}
        if not reader.fieldnames or not required.issubset(set(reader.fieldnames)):
            raise ValueError(f"{path} 必须包含表头 target_id,user_id；当前表头={reader.fieldnames}")
        previous = None
        for line, row in enumerate(reader, start=2):
            target_id = (row.get("target_id") or "").strip()
            user_id = (row.get("user_id") or "").strip()
            if not target_id.isdigit() or not user_id.isdigit() or int(target_id) <= 0 or int(user_id) <= 0:
                raise ValueError(f"{path}:{line} 的 target_id/user_id 必须是正整数")
            identity = (int(target_id), int(user_id))
            if previous is not None and identity <= previous:
                raise ValueError(f"{path}:{line} CSV 必须严格按 target_id,user_id 升序且无重复关系")
            previous = identity
            yield target_id, user_id


def import_relations(client: redis.Redis, path: Path, target_type: str,
                     prefix: str, shards: int, batch_size: int) -> int:
    pipe = client.pipeline(transaction=False)
    operations = 0
    imported = 0
    for target_id, user_id in csv_rows(path):
        base = base_key(prefix, target_type, target_id, shards)
        shard = shard_for(target_type, target_id, shards)
        pipe.sadd(f"{base}:users", user_id)
        pipe.incr(f"{base}:count")
        pipe.sadd(registry_key(prefix, shard), f"{target_type}:{target_id}")
        operations += 3
        imported += 1
        if operations >= batch_size * 3:
            pipe.execute()
            operations = 0
    if operations:
        pipe.execute()
    return imported


def main() -> int:
    parser = argparse.ArgumentParser(description="Bootstrap RabbitMQ like V1 Redis state from exported MySQL relations")
    parser.add_argument("--redis-url", default="redis://127.0.0.1:6380/0")
    parser.add_argument("--post-csv", required=True, type=Path)
    parser.add_argument("--picture-csv", required=True, type=Path)
    parser.add_argument("--shards", type=int, default=16)
    parser.add_argument("--prefix", default="picture:like:rabbitmq:v1")
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--reset", action="store_true", help="显式清空 prefix:* 再回填；不可省略")
    args = parser.parse_args()

    if not 1 <= args.shards <= 256: parser.error("--shards 必须在 1-256 之间")
    if args.batch_size < 1: parser.error("--batch-size 必须大于 0")
    if not args.prefix.strip() or any(ch in args.prefix for ch in "*?[]\\"):
        parser.error("--prefix 不能为空且不能包含 Redis SCAN 通配符或反斜杠")
    if not args.post_csv.is_file() or not args.picture_csv.is_file():
        parser.error("post-csv/picture-csv 必须是存在的 CSV 文件")
    if not args.reset: parser.error("必须显式提供 --reset；先备份并确认停止点赞写流量")

    client = redis.Redis.from_url(args.redis_url, decode_responses=True, socket_timeout=5)
    client.ping()
    print(f"将清理 {args.prefix}:* 并从 CSV 回填。请确认这是 RabbitMQ V1 独立 namespace。")
    deleted = scan_delete_namespace(client, args.prefix)
    print(f"已删除 {deleted} 个旧 key，开始导入...")
    post_count = import_relations(client, args.post_csv, "POST", args.prefix, args.shards, args.batch_size)
    picture_count = import_relations(client, args.picture_csv, "PICTURE", args.prefix, args.shards, args.batch_size)

    # 导入结束后逐个核对 Redis Set 基数和计数器；不一致则拒绝写 ready 标记。
    for key in client.scan_iter(match=f"{args.prefix}:*:target:*:users", count=1000):
        count_key = key[:-len(":users")] + ":count"
        cardinality = client.scard(key)
        count_value = int(client.get(count_key) or "0")
        if cardinality != count_value:
            raise RuntimeError(f"回填校验失败：{key} SCARD={cardinality}, count={count_value}")

    client.set(f"{args.prefix}:meta:ready", "1")
    print(f"完成：帖子关系 {post_count} 条，公共图片关系 {picture_count} 条。")
    print(f"ready 标记已设置：{args.prefix}:meta:ready")
    print("下一步请确认 MySQL 关系表计数与 CSV 完全匹配，再设置 community.like.mode=rabbitmq 启动应用。")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"导入失败：{exc}", file=sys.stderr)
        raise SystemExit(1)

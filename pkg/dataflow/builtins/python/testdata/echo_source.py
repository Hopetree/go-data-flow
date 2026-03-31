#!/usr/bin/env python3
"""测试用 Source 脚本：输出固定数量的 JSON 记录。"""
import json
import sys

records = [
    {"id": 1, "name": "alice", "age": 30},
    {"id": 2, "name": "bob", "age": 25},
    {"id": 3, "name": "charlie", "age": 35},
]

for record in records:
    json.dump(record, sys.stdout)
    sys.stdout.write("\n")
    sys.stdout.flush()

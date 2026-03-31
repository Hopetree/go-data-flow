#!/usr/bin/env python3
"""测试用 Processor 脚本：处理几条后模拟崩溃（sys.exit(1)）。"""
import json
import sys

count = 0
for line in sys.stdin:
    count += 1
    if count > 2:
        print("模拟崩溃: 处理了 2 条后退出", file=sys.stderr)
        sys.stderr.flush()
        sys.exit(1)

    record = json.loads(line)
    record["processed"] = True
    json.dump(record, sys.stdout)
    sys.stdout.write("\n")
    sys.stdout.flush()

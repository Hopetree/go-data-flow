#!/usr/bin/env python3
"""测试用 Processor 脚本：读取 stdin JSON，添加 processed 字段后输出到 stdout。"""
import json
import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    record = json.loads(line)
    record["processed"] = True
    json.dump(record, sys.stdout)
    sys.stdout.write("\n")
    sys.stdout.flush()

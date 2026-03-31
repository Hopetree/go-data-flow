#!/usr/bin/env python3
"""示例 Python Processor: 将 name 字段转为大写，score 加 5 分，并添加 processed 标记。"""

import json
import sys

for line in sys.stdin:
    record = json.loads(line.strip())

    record["name"] = record["name"].upper()
    record["score"] = record["score"] + 5
    record["processed"] = True

    json.dump(record, sys.stdout)
    sys.stdout.write("\n")
    sys.stdout.flush()

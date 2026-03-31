#!/usr/bin/env python3
"""示例 Python Source: 生成内存数据并输出为 JSON lines。"""

import json
import sys

records = [
    {"id": 1, "name": "alice", "score": 85},
    {"id": 2, "name": "bob", "score": 92},
    {"id": 3, "name": "charlie", "score": 78},
]

for record in records:
    json.dump(record, sys.stdout)
    sys.stdout.write("\n")
    sys.stdout.flush()

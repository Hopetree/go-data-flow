#!/usr/bin/env python3
"""示例 Python Sink: 将数据格式化输出到控制台，统计处理记录数。"""

import json
import sys

count = 0
for line in sys.stdin:
    record = json.loads(line.strip())
    print(f"[{record['id']}] {record['name']} - score: {record['score']} - processed: {record.get('processed', False)}")
    count += 1

print(f"\n共处理 {count} 条记录", file=sys.stderr)
sys.stderr.flush()

#!/usr/bin/env python3
"""示例: 生成批量测试数据，用于演示多进程 Processor。"""

import json
import sys

# 生成 20 条数据，让多进程处理的效果更明显
for i in range(20):
    record = {"id": i + 1, "name": f"user_{i+1}", "score": i * 10 + 50}
    json.dump(record, sys.stdout)
    sys.stdout.write("\n")
    sys.stdout.flush()

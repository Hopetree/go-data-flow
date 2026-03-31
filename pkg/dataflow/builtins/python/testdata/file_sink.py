#!/usr/bin/env python3
"""测试用 Sink 脚本：读取 stdin JSON，写入到指定文件。"""
import json
import os
import sys

output_file = os.environ.get("SINK_OUTPUT_FILE", "/tmp/python_sink_test_output.jsonl")

count = 0
with open(output_file, "w") as f:
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        record = json.loads(line)
        f.write(json.dumps(record) + "\n")
        count += 1

    f.flush()

# 向 stderr 输出处理统计
print(f"已写入 {count} 条记录到 {output_file}", file=sys.stderr)

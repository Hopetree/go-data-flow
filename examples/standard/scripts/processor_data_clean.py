#!/usr/bin/env python3
"""示例: 数据清洗 Processor —— 使用 Python 处理复杂数据清洗逻辑。

清洗规则：
1. name 转为首字母大写
2. status 为 inactive 的记录过滤掉
3. tags 为空时填充默认值
4. score 低于 60 时标记为需要跟进
"""

import json
import sys


def process_record(record):
    # 过滤：跳过 inactive 记录
    if record.get("status") != "active":
        return None

    # 清洗 name
    name = record.get("name", "")
    record["name"] = name.strip().title()

    # 填充默认 tags
    if not record.get("tags"):
        record["tags"] = ["default"]

    # 标记低分
    record["need_followup"] = record.get("score", 0) < 60

    # 移除不需要的字段
    record.pop("raw_data", None)

    return record


for line in sys.stdin:
    record = json.loads(line.strip())
    result = process_record(record)
    if result is not None:
        json.dump(result, sys.stdout)
        sys.stdout.write("\n")
        sys.stdout.flush()

#!/usr/bin/env python3
"""示例: 使用 Python 标准 logging 模块输出日志。

Go 框架会解析 stderr 上的日志级别前缀，自动匹配 Go 的日志级别：
  INFO:message  → Go logger.Info
  WARNING:message → Go logger.Warn
  ERROR:message → Go logger.Error
  DEBUG:message → Go logger.Debug
  其他格式 → Go logger.Warn（向后兼容）
"""

import json
import logging
import sys

# 配置 logging 输出到 stderr（Go 框架会捕获）
logging.basicConfig(
    level=logging.DEBUG,
    format="%(levelname)s:%(message)s",
    stream=sys.stderr,
)
log = logging.getLogger(__name__)


def process_record(record):
    log.info("开始处理记录 id=%s", record.get("id"))
    log.debug("记录详情: %s", record)

    # 模拟业务逻辑
    name = record.get("name", "").strip().title()
    record["name"] = name

    log.info("处理完成 id=%s, name=%s", record.get("id"), name)
    return record


for line in sys.stdin:
    record = json.loads(line.strip())
    result = process_record(record)
    json.dump(result, sys.stdout)
    sys.stdout.write("\n")
    sys.stdout.flush()

log.info("全部处理完成")

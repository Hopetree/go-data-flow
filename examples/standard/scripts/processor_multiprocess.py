#!/usr/bin/env python3
"""示例: 多进程 Processor —— 使用 multiprocessing 并行处理数据。

适用于 CPU 密集型场景（如模型推理、复杂计算）。
Go 单线程发送数据到 Python，Python 内部用多进程并行处理。
"""

import json
import sys
from multiprocessing import Pool, cpu_count
import time


def process_record(record):
    """单个记录的处理函数（在子进程中执行）。

    模拟一个 CPU 密集型操作：耗时计算。
    实际使用时替换为你的业务逻辑（如模型推理、数据转换等）。
    """
    # 模拟耗时操作（实际使用时替换为真实逻辑）
    score = record.get("score", 0)
    result = score ** 2 + score * 0.5

    return {
        **record,
        "computed": round(result, 2),
        "processed": True,
    }


def main():
    # 收集所有输入记录
    records = []
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        records.append(json.loads(line))

    if not records:
        return

    # 使用多进程并行处理
    # 默认使用 CPU 核心数，可通过环境变量 WORKERS 自定义
    workers = int(sys.argv[1]) if len(sys.argv) > 1 else cpu_count()
    workers = min(workers, len(records))  # 不超过记录数

    start = time.time()

    with Pool(processes=workers) as pool:
        results = pool.map(process_record, records)

    elapsed = time.time() - start

    # 输出结果
    for result in results:
        json.dump(result, sys.stdout)
        sys.stdout.write("\n")
    sys.stdout.flush()

    print(f"处理 {len(records)} 条记录, 耗时 {elapsed:.3f}s, {workers} 个进程",
          file=sys.stderr)
    sys.stderr.flush()


if __name__ == "__main__":
    main()

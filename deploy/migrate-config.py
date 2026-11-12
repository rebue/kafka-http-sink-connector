#!/usr/bin/env python3
"""把 Kafka Connect 上 3.5.x 的 HTTP Sink 连接器配置迁移到本插件的键名。

这是一次不兼容重写，三个变化必须一起改，否则连接器起不来或者配置静默失效：

  1. connector.class   rebue.wheel.kafka.httpsink.HttpSinkConnector -> rebue.kafka.httpsink.HttpSinkConnector
  2. http.api.url      -> http.url（新实现不再读旧键）
  3. max.retries 等    -> http.retry.* 命名空间

用法：
    # 只看会改成什么，不改动集群
    ./migrate-config.py --connect http://localhost:8083 --connector my-sink

    # 真正写入
    ./migrate-config.py --connect http://localhost:8083 --connector my-sink --apply

前置条件：新 jar 已经替换进 worker 的 plugin.path 并**重启过 worker**（Connect 只在启动时扫描
插件目录），否则 PUT 会因为找不到 connector.class 而返回 400。
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request

NEW_CLASS = "rebue.kafka.httpsink.HttpSinkConnector"
OLD_CLASSES = {"rebue.wheel.kafka.httpsink.HttpSinkConnector"}

# 旧键 -> 新键。同时标注取值映射，避免 auto/string/bytes 这类语义差异被一并平替掉
RENAMES = {
    "http.api.url": "http.url",
    "http.version": "http.protocol",
    "http.header.content-type": "http.content.type",
    "value.format": "http.payload.format",
    "value.bytes.encoding": "http.payload.bytes.encoding",
    "max.retries": "http.retry.max",
    "retry.backoff.ms": "http.retry.backoff.ms",
    "retry.backoff.max.ms": "http.retry.backoff.max.ms",
}

VALUE_MAP = {
    "http.payload.format": {"string": "text", "bytes": "auto", "auto": "auto", "json": "json"},
}


def http(method: str, url: str, body: dict | None = None) -> dict | list | str:
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(url, data=data, method=method)
    if data is not None:
        request.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read().decode()
    except urllib.error.HTTPError as error:
        detail = error.read().decode(errors="replace")
        raise SystemExit(f"{method} {url} 失败: HTTP {error.code} {detail}") from error
    except urllib.error.URLError as error:
        raise SystemExit(f"连不上 {url}: {error.reason}") from error
    return json.loads(raw) if raw.strip() else ""


def migrate(config: dict) -> dict:
    result = {}
    for key, value in config.items():
        target = RENAMES.get(key, key)
        if target in config and target != key:
            # 新键已经存在，旧键直接丢掉，不覆盖
            continue
        if target in VALUE_MAP:
            value = VALUE_MAP[target].get(str(value), value)
        result[target] = value
    result["connector.class"] = NEW_CLASS
    # 旧键残留会以 unused 告警的形式留在页面上，清掉
    for stale in ("http.api.url", "value.format", "value.bytes.encoding"):
        result.pop(stale, None)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="迁移 HTTP Sink 连接器配置到本插件")
    parser.add_argument("--connect", required=True, help="Connect REST 地址，例如 http://localhost:8083")
    parser.add_argument("--connector", action="append", default=[],
                        help="连接器名，可重复；省略则处理所有使用旧类的连接器")
    parser.add_argument("--apply", action="store_true", help="真正写回（默认只打印差异）")
    args = parser.parse_args()

    base = args.connect.rstrip("/")
    names = args.connector or discover(base)
    if not names:
        print("没有发现使用旧 connector.class 的连接器，无需迁移")
        return 0

    for name in names:
        print(f"\n=== {name}")
        before = http("GET", f"{base}/connectors/{name}/config")
        after = migrate(before)

        changed = {key: (before.get(key), value) for key, value in after.items() if before.get(key) != value}
        removed = [key for key in before if key not in after]
        if not changed and not removed:
            print("  配置已是新形态，跳过")
            continue
        for key, (old, new) in changed.items():
            print(f"  {key}: {old!r} -> {new!r}")
        for key in removed:
            print(f"  移除 {key} (值为 {before[key]!r})")

        if not args.apply:
            print("  （未写入，加 --apply 生效）")
            continue

        http("PUT", f"{base}/connectors/{name}/config", after)
        status = http("GET", f"{base}/connectors/{name}/status")
        print(f"  已写入，状态: {status['connector']['state']} / task: "
              f"{[task['state'] for task in status.get('tasks', [])]}")
    return 0


def discover(base: str) -> list[str]:
    """找出仍在使用旧 connector.class 的连接器"""
    found = []
    for name in http("GET", f"{base}/connectors"):
        config = http("GET", f"{base}/connectors/{name}/config")
        if config.get("connector.class") in OLD_CLASSES:
            found.append(name)
    return found


if __name__ == "__main__":
    sys.exit(main())

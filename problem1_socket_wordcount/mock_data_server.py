"""
模拟 Socket 数据源：向指定端口发送英文句子，替代 Linux 下的 `nc -lk 9999`。
Windows 下没有 nc 命令，用本脚本即可在本机造数据。

用法：
  python mock_data_server.py            # 默认 9999 端口，每 1 秒发一句
  python mock_data_server.py --port 9999 --interval 0.5
"""
import argparse
import random
import socket
import time

# 模拟的英文语句流（词长覆盖 1~8 个字母，方便验证 3-5 字母过滤）
SENTENCES = [
    "hello world spark streaming",
    "big data is changing the world",
    "structured streaming makes stream processing easy",
    "spark sql engine powers the streaming job",
    "real time data pipeline with kafka and spark",
    "the cat sat on the mat",
    "we are learning big data technology",
    "apache spark is fast and general engine",
    "stream processing in the cloud era",
    "data engineers love clean pipelines",
]


def main(host: str = "localhost", port: int = 9999, interval: float = 1.0):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # 避免 TIME_WAIT 导致重启绑定失败
    srv.bind((host, port))
    srv.listen(1)
    print(f"[mock server] 监听 {host}:{port}，等待 Spark 连接 ...")

    conn, addr = srv.accept()
    print(f"[mock server] 客户端 {addr} 已连接，开始发送数据（Ctrl+C 停止）")
    try:
        while True:
            line = random.choice(SENTENCES)
            conn.sendall((line + "\n").encode("utf-8"))
            print(f"[mock server] 已发送: {line}")
            time.sleep(interval)
    except (BrokenPipeError, ConnectionResetError):
        print("[mock server] 客户端断开，退出")
    except KeyboardInterrupt:
        print("[mock server] 手动停止")
    finally:
        conn.close()
        srv.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="模拟 Socket 数据源")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=9999)
    parser.add_argument("--interval", type=float, default=1.0, help="发送间隔秒数")
    args = parser.parse_args()
    main(args.host, args.port, args.interval)

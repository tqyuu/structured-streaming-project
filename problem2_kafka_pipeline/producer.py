"""
题目二-问题1：Kafka 生产者
每秒生成一个随机商品 ID（格式：product_xxx）并发送到 input-topic，
模拟用户对商品的点击事件流。

用法：
  python producer.py                       # 默认 localhost:9092 -> input-topic，每秒 1 条
  python producer.py --interval 0.2 --max-products 20
"""
import argparse
import random
import time

from kafka import KafkaProducer


def kafka_producer(bootstrap_servers: str = "localhost:9092",
                   topic: str = "input-topic",
                   interval: float = 1.0,
                   max_products: int = 100) -> None:
    """持续生产点击事件：每 interval 秒发送一个 product_xxx 到 topic。"""
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers.split(","),
        value_serializer=lambda v: v.encode("utf-8"),  # 消息体直接存商品ID字符串
        key_serializer=lambda v: v.encode("utf-8"),
        acks="all",      # 等待所有 ISR 副本确认，防止 broker 宕机丢消息
        retries=3,       # 发送失败自动重试
    )
    print(f"[producer] 已连接 {bootstrap_servers}，每 {interval}s 发送一条 -> {topic}（Ctrl+C 停止）")
    try:
        while True:
            # 生成随机商品ID，如 product_042
            product_id = f"product_{random.randint(1, max_products):03d}"
            # key = 商品ID：Kafka 按 key 哈希选分区 -> 同一商品固定进同一分区（分区内有序）
            producer.send(topic, key=product_id, value=product_id)
            print(f"[producer] 已发送: {product_id}")
            time.sleep(interval)
    except KeyboardInterrupt:
        print("[producer] 手动停止")
    finally:
        producer.close()  # 刷出缓冲区中未发送的消息并释放连接


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka 点击事件生产者")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka 地址，逗号分隔")
    parser.add_argument("--topic", default="input-topic")
    parser.add_argument("--interval", type=float, default=1.0, help="发送间隔秒数")
    parser.add_argument("--max-products", type=int, default=100, help="商品ID范围 1~N，越大越分散")
    args = parser.parse_args()
    kafka_producer(args.bootstrap, args.topic, args.interval, args.max_products)

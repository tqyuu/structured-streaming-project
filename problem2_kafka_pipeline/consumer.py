"""
题目二-问题2/3：Kafka 实时数据处理流水线（Structured Streaming 消费者）
  1. 从 input-topic 读取点击事件（value = 商品ID 字符串）
  2. 按 30 秒事件时间窗口统计每个商品的点击次数
  3. 以 "商品ID:次数" 格式写入 output-topic（Kafka Sink）

查看结果（另开终端）：
  kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic output-topic

用法：
  python consumer.py [--bootstrap localhost:9092] [--debug]
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, window


def kafka_consumer(bootstrap_servers: str = "localhost:9092",
                   input_topic: str = "input-topic",
                   output_topic: str = "output-topic",
                   checkpoint_path: str = "/tmp/kafka_click_checkpoint",
                   window_seconds: int = 30,
                   debug: bool = False,
                   runtime: float = 0) -> None:
    """Kafka -> 聚合 -> Kafka 的完整流式流水线。"""
    spark = (
        SparkSession.builder
        .appName("KafkaClickCount")
        .master("local[*]")  # 本地调试用；集群上用 spark-submit 时删除
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ---------- 1. 读取 Kafka 源 ----------
    # Kafka 源 schema 固定：key/value(bytes) + topic/partition/offset/timestamp 等元数据列
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", input_topic)
        .option("startingOffsets", "latest")  # 首次启动从最新位点开始；重启后由 checkpoint 接管
        .load()
    )

    # ---------- 2. 解析：二进制 value -> 字符串商品ID ----------
    # 事件时间直接用 Kafka 消息自带 timestamp（数据到达 broker 的时刻），无需额外解析
    clicks = raw.select(
        col("value").cast("string").alias("product_id"),
        col("timestamp").alias("event_time"),
    ).filter(col("product_id").isNotNull())

    # ---------- 3. 统计：30 秒事件时间窗口内每个商品的点击次数 ----------
    counts = (
        clicks
        .withWatermark("event_time", "10 seconds")   # 允许消息迟到 10 秒，之后清理过期状态
        .groupBy(window(col("event_time"), f"{window_seconds} seconds"), col("product_id"))
        .count()
    )

    # ---------- 4. 组装输出：格式 "商品ID:次数" ----------
    # Kafka Sink 要求结果 DataFrame 必须包含 value 列（string/binary），可选 key 列
    result = counts.select(
        col("product_id").alias("key"),  # 以商品ID为消息key，输出分区内按商品有序
        concat(col("product_id"), lit(":"), col("count").cast("string")).alias("value"),
    )

    # ---------- 5. 写回 Kafka ----------
    query = (
        result.writeStream
        .outputMode("append")                 # 有 watermark 时窗口结果可安全追加
        .trigger(processingTime=f"{window_seconds} seconds")  # 每 30 秒提交一个微批
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("topic", output_topic)
        .option("checkpointLocation", checkpoint_path)  # 位点与状态持久化，重启自动续跑
        .start()
    )

    # debug 模式：同时输出到控制台，方便课堂演示/截图
    if debug:
        console_query = (
            counts.writeStream
            .outputMode("update")   # update：只输出本批有变化的 (窗口, 商品, 次数)
            .trigger(processingTime=f"{window_seconds} seconds")  # 与窗口对齐，避免高频刷屏
            .format("console")
            .option("truncate", "false")
            .start()
        )
        # 两个查询并行运行
        if runtime and runtime > 0:
            spark.streams.awaitAnyTermination(runtime)   # 限时运行，到时返回
            for q in spark.streams.active:               # 优雅停止所有查询
                q.stop()
        else:
            spark.streams.awaitAnyTermination()          # 任一查询终止即退出
    else:
        if runtime and runtime > 0:
            query.awaitTermination(runtime)
            query.stop()
        else:
            query.awaitTermination()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka 实时点击统计流水线")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka 地址，逗号分隔")
    parser.add_argument("--input-topic", default="input-topic")
    parser.add_argument("--output-topic", default="output-topic")
    parser.add_argument("--checkpoint", default="/tmp/kafka_click_checkpoint",
                        help="检查点目录（Windows 本地调试请改成 Windows 路径）")
    parser.add_argument("--window", type=int, default=30, help="统计窗口秒数")
    parser.add_argument("--debug", action="store_true", help="额外输出到控制台")
    parser.add_argument("--runtime", type=float, default=0,
                        help="运行秒数，>0 时到时自动优雅停止（演示/测试用），默认一直运行")
    args = parser.parse_args()
    kafka_consumer(args.bootstrap, args.input_topic, args.output_topic,
                   args.checkpoint, args.window, args.debug, args.runtime)

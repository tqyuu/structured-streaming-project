"""
班级_学号_姓名.py

云平台计算课程作业 · Spark Structured Streaming
================================================================
一个文件包含两道题目（题目一：Socket 实时词频统计 / 题目二：Kafka 实时点击统计）

运行方式（Windows 终端示例）：
    python 班级_学号_姓名.py p1-mock          # 辅助：模拟 nc -lk 9999（Windows 无 nc 时用）
    python 班级_学号_姓名.py p1-1             # 题目一 问题1：Complete 模式输出到控制台
    python 班级_学号_姓名.py p1-2             # 题目一 问题2：只统计 3-5 字母的单词
    python 班级_学号_姓名.py p1-3             # 题目一 问题3：Parquet 落盘 + checkpoint

    python 班级_学号_姓名.py p2-1             # 题目二 问题1：Kafka 生产者（每秒 1 条）
    python 班级_学号_姓名.py p2-2             # 题目二 问题2：30s 窗口聚合，控制台输出
    python 班级_学号_姓名.py p2-3             # 题目二 问题3：30s 窗口聚合，写回 output-topic

常用参数：
    --host localhost --port 9999              # 题目一 socket 源地址
    --output  /tmp/wordcount_output           # 题目一 问题3 的 Parquet 输出目录
    --checkpoint /tmp/wordcount_checkpoint    # 题目一 问题3 的检查点目录
    --bootstrap localhost:9092                # 题目二 Kafka 地址
    --input-topic input-topic --output-topic output-topic
    --window 30                               # 题目二 窗口秒数
    --runtime 120                             # 限时运行秒数，>0 时到时优雅停止（演示用）
    --master "local[*]"                       # 集群上可改成 spark://... 或 yarn

依赖：pyspark；题目二另需 kafka-python（pip install pyspark kafka-python）
"""

'''
#第一题：实时词频统计系统
场景：基于 Socket 源实现实时英文语句词频统计。
问题1：编写 Structured Streaming 程序，从 localhost:9999 端口读取文本数据流，
       统计每个单词的出现频率，要求每 10 秒触发一次计算，使用 Complete 输出模式。
问题2：修改程序，只统计长度为 3-5 个字母的单词，并将结果输出到控制台。
问题3：使用 File 接收器将处理结果以 Parquet 格式保存到本地目录 /tmp/wordcount_output，
       并设置检查点目录。
'''

import argparse
import random
import socket
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat,
    current_timestamp,
    explode,
    length,
    lit,
    split,
    window,
)


# ============================================================
# 公共工具
# ============================================================
def build_spark(app_name: str = "StructuredStreamingHomework",
                master: str = "local[*]") -> SparkSession:
    """创建 SparkSession（日志级别设为 WARN，避免控制台被 INFO 淹没）。"""
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(master)          # 本地调试用 local[*]；集群用 spark-submit 指定 master
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def run_query(query, runtime: float = 0) -> None:
    """运行流式查询：runtime>0 时到时停止（演示/测试用），否则一直运行。

    说明：停止时若正好有微批在写文件，该批会被中断（日志里的 "Aborting job" 属正常现象）；
    已提交的批次不受影响，checkpoint 会保证重启后从正确位点继续。
    """
    if runtime and runtime > 0:
        query.awaitTermination(runtime)   # 阻塞 runtime 秒后返回
        query.stop()                      # 停止查询并释放资源
    else:
        query.awaitTermination()


def read_socket(spark: SparkSession, host: str, port: int):
    """读取 Socket 源：schema 固定为 value(string，一行文本) + 元数据列。"""
    return (
        spark.readStream
        .format("socket")
        .option("host", host)
        .option("port", port)
        .load()
    )


def split_to_words(lines):
    """把一行文本拆成多行 (word, event_time)，并过滤掉切分产生的空串。

    事件时间列的处理：Spark 3.x 的 socket 源自带 timestamp 列（数据接收时刻）；
    Spark 4.x 移除了该列，此时退化为 current_timestamp()（处理时间）。
    """
    if "timestamp" in lines.columns:
        words = lines.select(
            explode(split(col("value"), " ")).alias("word"),
            col("timestamp").alias("event_time"),
        )
    else:
        words = lines.select(
            explode(split(col("value"), " ")).alias("word"),
            current_timestamp().alias("event_time"),
        )
    return words.filter(col("word") != "")


# ============================================================
# 第一题 问题1
# ============================================================
'''
#第一题 问题1：
从 localhost:9999 端口读取文本数据流，统计每个单词的出现频率，
要求每 10 秒触发一次计算，使用 Complete 输出模式。
'''
def problem1_1(host: str, port: int, runtime: float, master: str) -> None:
    spark = build_spark("P1-Q1-SocketWordCount", master)

    lines = read_socket(spark, host, port)
    words = split_to_words(lines)                 # 一行 -> 多个单词

    word_counts = words.groupBy("word").count()   # 按单词聚合计数

    query = (
        word_counts.writeStream
        .outputMode("complete")                   # Complete：每次触发输出全量最新词频表
        .trigger(processingTime="10 seconds")      # 每 10 秒触发一次微批
        .format("console")
        .option("truncate", "false")
        .start()
    )
    run_query(query, runtime)


# ============================================================
# 第一题 问题2
# ============================================================
'''
#第一题 问题2：
在问题1的基础上修改程序，只统计长度为 3-5 个字母的单词，并将结果输出到控制台。
'''
def problem1_2(host: str, port: int, runtime: float, master: str) -> None:
    spark = build_spark("P1-Q2-FilterWordCount", master)

    lines = read_socket(spark, host, port)
    words = split_to_words(lines)

    # 只保留长度在 3~5 之间的单词（between 为闭区间，包含 3 和 5）
    words = words.filter(length(col("word")).between(3, 5))

    word_counts = words.groupBy("word").count()

    query = (
        word_counts.writeStream
        .outputMode("complete")                   # 与问题1一致：全量刷新
        .trigger(processingTime="10 seconds")
        .format("console")
        .option("truncate", "false")
        .start()
    )
    run_query(query, runtime)


# ============================================================
# 第一题 问题3
# ============================================================
'''
#第一题 问题3：
使用 File 接收器将处理结果以 Parquet 格式保存到本地目录 /tmp/wordcount_output，并设置检查点目录。
'''
def problem1_3(host: str, port: int, runtime: float, master: str,
               output_path: str, checkpoint_path: str) -> None:
    spark = build_spark("P1-Q3-ParquetSink", master)

    lines = read_socket(spark, host, port)
    words = split_to_words(lines).filter(length(col("word")).between(3, 5))

    # 关键点：File Sink 只支持 Append 输出模式；
    #        而 Append 模式下的流式聚合必须定义 watermark，否则 Spark 直接报错。
    #        原因是 Spark 必须确认"水位线之后不会再有更早的数据"，
    #        才能判定某个窗口的结果已经终结、可以一次性追加写出。
    word_counts = (
        words
        .withWatermark("event_time", "20 seconds")             # 允许迟到 20 秒，之后清理该窗口状态
        .groupBy(window(col("event_time"), "10 seconds"), col("word"))
        .count()
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("word"),
            col("count"),
        )
    )

    query = (
        word_counts.writeStream
        .outputMode("append")                                  # 文件接收器唯一支持的模式
        .trigger(processingTime="10 seconds")
        .format("parquet")
        .option("path", output_path)                           # 结果输出目录
        .option("checkpointLocation", checkpoint_path)          # 检查点：断点续跑
        .start()
    )
    run_query(query, runtime)


# ============================================================
# 第一题 辅助：模拟 socket 数据源
# ============================================================
# 模拟的英文语句流（词长覆盖 1~8 个字母，方便验证 3-5 字母过滤）
MOCK_SENTENCES = [
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


'''
#第一题 辅助：Windows 下没有 nc 命令，用本函数模拟 "nc -lk 9999"。
#        每隔 interval 秒向客户端发送一行英文句子。
'''
def mock_socket_server(host: str, port: int, interval: float) -> None:
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)   # 避免 TIME_WAIT 导致重启绑定失败
    srv.bind((host, port))
    srv.listen(1)
    print(f"[mock server] 监听 {host}:{port}，等待 Spark 连接 ...")

    conn, addr = srv.accept()
    print(f"[mock server] 客户端 {addr} 已连接，开始发送数据（Ctrl+C 停止）")
    try:
        while True:
            line = random.choice(MOCK_SENTENCES)
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


'''
#第二题：Kafka 实时数据处理流水线
场景：基于 Kafka 源构建完整的数据处理流水线。
问题1：编写生产者程序，每秒生成一个随机商品 ID（格式：product_xxx）并发送到 Kafka 的 input-topic 主题。
问题2：编写 Structured Streaming 消费者程序，从 input-topic 读取数据，统计每 30 秒内每个商品的点击次数。
问题3：将统计结果写入 Kafka 的 output-topic 主题，格式为 "商品ID:次数"。
'''


# ============================================================
# 第二题 问题1
# ============================================================
'''
#第二题 问题1：
编写 Kafka 生产者程序，每秒生成一个随机商品 ID（格式 product_xxx）并发送到 input-topic 主题。
'''
def problem2_1(bootstrap: str, topic: str, interval: float, max_products: int) -> None:
    from kafka import KafkaProducer   # 局部导入：只跑题目一时不需要 kafka-python

    producer = KafkaProducer(
        bootstrap_servers=bootstrap.split(","),
        value_serializer=lambda v: v.encode("utf-8"),   # 消息体为商品 ID 字符串
        key_serializer=lambda v: v.encode("utf-8"),
        acks="all",     # 等待所有 ISR 副本确认，防止 broker 宕机丢消息
        retries=3,      # 发送失败自动重试
    )
    print(f"[producer] 已连接 {bootstrap}，每 {interval}s 发送一条 -> {topic}（Ctrl+C 停止）")
    try:
        while True:
            product_id = f"product_{random.randint(1, max_products):03d}"
            # key = 商品 ID：Kafka 按 key 哈希选分区 -> 同一商品固定进同一分区（分区内有序）
            producer.send(topic, key=product_id, value=product_id)
            print(f"[producer] 已发送: {product_id}")
            time.sleep(interval)
    except KeyboardInterrupt:
        print("[producer] 手动停止")
    finally:
        producer.close()   # 刷出缓冲区并释放连接


# ============================================================
# 第二题 公共：读取 Kafka -> 30 秒窗口聚合
# ============================================================
def build_window_counts(spark: SparkSession, bootstrap: str, input_topic: str,
                        window_seconds: int):
    """从 input-topic 读流，做 30 秒事件时间窗口的按商品点击计数。"""
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", input_topic)
        .option("startingOffsets", "latest")   # 首次从最新位点开始；重启后由 checkpoint 接管
        .load()
    )
    # Kafka 源字段固定：value 为 bytes，需转 string；timestamp 为消息到达 broker 的时刻
    clicks = raw.select(
        col("value").cast("string").alias("product_id"),
        col("timestamp").alias("event_time"),
    ).filter(col("product_id").isNotNull())

    return (
        clicks
        .withWatermark("event_time", "10 seconds")     # 允许迟到 10 秒
        .groupBy(window(col("event_time"), f"{window_seconds} seconds"), col("product_id"))
        .count()
    )


# ============================================================
# 第二题 问题2
# ============================================================
'''
#第二题 问题2：
编写 Structured Streaming 消费者程序，从 input-topic 读取数据，统计每 30 秒内每个商品的点击次数
（结果输出到控制台）。
'''
def problem2_2(bootstrap: str, input_topic: str, window_seconds: int,
               runtime: float, master: str) -> None:
    spark = build_spark("P2-Q2-KafkaWindowCount", master)

    counts = build_window_counts(spark, bootstrap, input_topic, window_seconds)

    query = (
        counts.writeStream
        .outputMode("update")                      # update：只输出本批有变化的行，实时性好
        .trigger(processingTime=f"{window_seconds} seconds")
        .format("console")
        .option("truncate", "false")
        .start()
    )
    run_query(query, runtime)


# ============================================================
# 第二题 问题3
# ============================================================
'''
#第二题 问题3：
将统计结果写入 Kafka 的 output-topic 主题，格式为 "商品ID:次数"，并设置检查点目录。
'''
def problem2_3(bootstrap: str, input_topic: str, output_topic: str,
               window_seconds: int, checkpoint_path: str, runtime: float,
               master: str, debug: bool) -> None:
    spark = build_spark("P2-Q3-KafkaSink", master)

    counts = build_window_counts(spark, bootstrap, input_topic, window_seconds)

    # Kafka Sink 要求结果 DataFrame 必须包含 value 列（string/binary），key 列可选
    result = counts.select(
        col("product_id").alias("key"),   # key = 商品 ID，保证同一商品落在同一分区
        concat(col("product_id"), lit(":"), col("count").cast("string")).alias("value"),
    )

    query = (
        result.writeStream
        .outputMode("append")                       # 有 watermark 时窗口结果可安全追加
        .trigger(processingTime=f"{window_seconds} seconds")
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("topic", output_topic)
        .option("checkpointLocation", checkpoint_path)   # 位点与聚合状态持久化
        .start()
    )

    if debug:
        # 演示用：同时把同一份聚合结果打印到控制台
        console_query = (
            counts.writeStream
            .outputMode("update")
            .trigger(processingTime=f"{window_seconds} seconds")
            .format("console")
            .option("truncate", "false")
            .start()
        )
        if runtime and runtime > 0:
            spark.streams.awaitAnyTermination(runtime)
            for q in spark.streams.active:
                q.stop()
        else:
            spark.streams.awaitAnyTermination()
    else:
        run_query(query, runtime)


# ============================================================
# 命令行入口
# ============================================================
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Structured Streaming 作业：题目一（Socket 词频统计）+ 题目二（Kafka 点击统计）",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "task",
        choices=["p1-1", "p1-2", "p1-3", "p1-mock", "p2-1", "p2-2", "p2-3"],
        help="p1-1/p1-2/p1-3：第一题三个小问；p1-mock：模拟 nc -lk 9999；\n"
             "p2-1/p2-2/p2-3：第二题三个小问",
    )

    # 题目一参数
    parser.add_argument("--host", default="localhost", help="socket 源主机")
    parser.add_argument("--port", type=int, default=9999, help="socket 源端口")
    parser.add_argument("--output", default="/tmp/wordcount_output",
                        help="第一题 问题3 的 Parquet 输出目录（Windows 请改成 D:/tmp/... 之类）")
    parser.add_argument("--checkpoint", default="/tmp/wordcount_checkpoint",
                        help="第一题 问题3 的检查点目录（Windows 请改成 Windows 路径）")
    parser.add_argument("--interval", type=float, default=1.0,
                        help="数据发送间隔秒数（p1-mock 与 p2-1 共用）")

    # 题目二参数
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka 地址，逗号分隔")
    parser.add_argument("--input-topic", default="input-topic")
    parser.add_argument("--output-topic", default="output-topic")
    parser.add_argument("--kafka-checkpoint", default="/tmp/kafka_click_checkpoint",
                        help="第二题 问题3 的检查点目录")
    parser.add_argument("--window", type=int, default=30, help="第二题统计窗口秒数")
    parser.add_argument("--max-products", type=int, default=100,
                        help="商品 ID 范围 1~N，越大越分散")
    parser.add_argument("--debug", action="store_true",
                        help="第二题 问题3：同时把结果打印到控制台")

    # 公共参数
    parser.add_argument("--runtime", type=float, default=0,
                        help="运行秒数，>0 时到时自动优雅停止（演示/测试用），默认一直运行")
    parser.add_argument("--master", default="local[*]",
                        help="Spark master，默认本地全部核心；集群可改为 spark://host:7077")
    args = parser.parse_args()

    if args.task == "p1-1":
        problem1_1(args.host, args.port, args.runtime, args.master)
    elif args.task == "p1-2":
        problem1_2(args.host, args.port, args.runtime, args.master)
    elif args.task == "p1-3":
        problem1_3(args.host, args.port, args.runtime, args.master,
                   args.output, args.checkpoint)
    elif args.task == "p1-mock":
        mock_socket_server(args.host, args.port, args.interval)
    elif args.task == "p2-1":
        problem2_1(args.bootstrap, args.input_topic, args.interval, args.max_products)
    elif args.task == "p2-2":
        problem2_2(args.bootstrap, args.input_topic, args.window, args.runtime, args.master)
    elif args.task == "p2-3":
        problem2_3(args.bootstrap, args.input_topic, args.output_topic, args.window,
                   args.kafka_checkpoint, args.runtime, args.master, args.debug)


if __name__ == "__main__":
    main()

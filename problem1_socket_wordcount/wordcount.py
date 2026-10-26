"""
题目一：实时词频统计系统
基于 Socket 源实现实时英文语句词频统计（Structured Streaming）

包含三个递进的实现（对应作业的 3 个问题）：
  问题1：从 localhost:9999 读取文本流，统计词频，每 10 秒触发一次，Complete 模式输出到控制台
  问题2：只统计长度为 3-5 个字母的单词
  问题3：结果以 Parquet 格式保存到 /tmp/wordcount_output，并设置检查点目录

启动方式：
  python wordcount.py --mode basic     # 问题1
  python wordcount.py --mode filter    # 问题2
  python wordcount.py --mode parquet   # 问题3

另开一个终端造数据（二选一）：
  Linux/Mac : nc -lk 9999   （手工敲句子回车）
  Windows   : python mock_data_server.py （自动每秒发一句英文）
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, length, col, window, current_timestamp


def build_spark(app_name: str = "SocketWordCount") -> SparkSession:
    """创建 SparkSession。

    注：master("local[*]") 仅为本地/教学环境调试用；
    集群环境通过 spark-submit --master 指定时应删除该行。
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .getOrCreate()
    )


def word_count_socket(mode: str = "basic",
                      host: str = "localhost",
                      port: int = 9999,
                      output_path: str = "/tmp/wordcount_output",
                      checkpoint_path: str = "/tmp/wordcount_checkpoint",
                      runtime: float = 0) -> None:
    """Socket 源实时词频统计主流程。

    mode:
      basic   -> 问题1：全量词频，Complete 模式输出控制台
      filter  -> 问题2：在问题1基础上过滤 3-5 字母单词
      parquet -> 问题3：Parquet 文件接收器 + checkpoint
    runtime:
      运行秒数，>0 时到时自动优雅停止（用于限时演示/测试），默认 0 表示一直运行
    """
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # ---------- 1. 创建 Socket 数据源 ----------
    # socket 源 schema 固定为两列：value(string, 一行文本) + timestamp(timestamp, 接收时刻)
    lines = (
        spark.readStream
        .format("socket")
        .option("host", host)
        .option("port", port)
        .load()
    )

    # ---------- 2. 算子级处理：一行 -> 多个单词 ----------
    # explode 把数组“拍平”成多行；过滤掉按空格切分产生的空串
    # 事件时间列：Spark 3.x 的 socket 源自带 timestamp 列（数据接收时刻）；
    #            Spark 4.x 移除了该列，退化为 current_timestamp()（处理时间）
    if "timestamp" in lines.columns:
        words = (
            lines.select(
                explode(split(col("value"), " ")).alias("word"),
                col("timestamp").alias("event_time"),
            )
            .filter(col("word") != "")
        )
    else:
        words = (
            lines.select(
                explode(split(col("value"), " ")).alias("word"),
                current_timestamp().alias("event_time"),
            )
            .filter(col("word") != "")
        )

    if mode == "parquet":
        # ---------- 问题3：Parquet 文件接收器 ----------
        # 关键点：File Sink 只支持 Append 输出模式；
        # 而 Append 模式下的流式聚合必须定义 watermark（否则 Spark 直接报错），
        # 因为 Spark 必须等到“水位线之后不再有更早数据”才能确定某窗口结果已终结，才能追加输出。
        word_counts = (
            words
            .filter(length(col("word")).between(3, 5))       # 保留问题2的过滤逻辑
            .withWatermark("event_time", "20 seconds")       # 迟到容忍 20 秒，超过后状态被清理
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
            .outputMode("append")                    # 文件接收器唯一支持的模式
            .trigger(processingTime="10 seconds")    # 保持每 10 秒一个微批
            .format("parquet")
            .option("path", output_path)             # 结果输出目录
            .option("checkpointLocation", checkpoint_path)  # 检查点：断点续跑 & exactly-once 基础
            .start()
        )
    else:
        # ---------- 问题1 / 问题2：控制台输出 ----------
        if mode == "filter":
            words = words.filter(length(col("word")).between(3, 5))

        word_counts = words.groupBy("word").count()

        query = (
            word_counts.writeStream
            .outputMode("complete")                  # Complete：每次触发输出全量最新结果
            .trigger(processingTime="10 seconds")    # 每 10 秒触发一次微批计算
            .format("console")
            .option("truncate", "false")
            .start()
        )

    if runtime and runtime > 0:
        query.awaitTermination(runtime)  # 限时运行：到时返回（不抛异常）
        query.stop()                     # 优雅停止：flush 输出并清理运行资源
    else:
        query.awaitTermination()  # 阻塞等待流式查询持续运行


def main():
    parser = argparse.ArgumentParser(description="Socket 实时词频统计")
    parser.add_argument("--mode", choices=["basic", "filter", "parquet"],
                        default="basic", help="basic=问题1 filter=问题2 parquet=问题3")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=9999)
    parser.add_argument("--output", default="/tmp/wordcount_output",
                        help="问题3 的 Parquet 输出目录（Windows 本地调试请改成 D:/tmp/... 之类的路径）")
    parser.add_argument("--checkpoint", default="/tmp/wordcount_checkpoint",
                        help="检查点目录（Windows 本地调试请改成 Windows 路径）")
    parser.add_argument("--runtime", type=float, default=0,
                        help="运行秒数，>0 时到时自动停止（演示/测试用），默认一直运行")
    args = parser.parse_args()

    word_count_socket(mode=args.mode, host=args.host, port=args.port,
                      output_path=args.output, checkpoint_path=args.checkpoint,
                      runtime=args.runtime)


if __name__ == "__main__":
    main()

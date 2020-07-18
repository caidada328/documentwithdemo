package day2

import org.apache.flink.streaming.api.scala._

object SourceFromFile {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env
      .readTextFile("/C:\\Scalaa\\FlinkSZ1128\\src\\main\\resources\\sensor.txt")
      .map(r => {
        // 使用逗号切割字符串
        val arr = r.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })

    stream.print()
    env.execute()
  }
}
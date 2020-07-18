package com.atguiug.flink.day01_03

import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object environmentCreate {
  def main(args: Array[String]): Unit = {
    //创建执行环境 这个环境上下文取决于环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(6000)
    env.setParallelism(1)
    val checkpointconfig = env.getCheckpointConfig
    checkpointconfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    checkpointconfig.setMinPauseBetweenCheckpoints(300001)//时间间隔
    checkpointconfig.setCheckpointTimeout(600001)//超时时间

    //返回本地环境,开发时候
    val env2 = StreamExecutionEnvironment.createLocalEnvironment()
    //返回集群环境，测试完了需要修改成这个
    val env3 = StreamExecutionEnvironment.createRemoteEnvironment("hadoop104",6132,"/opt/module/...jar")
  }

}

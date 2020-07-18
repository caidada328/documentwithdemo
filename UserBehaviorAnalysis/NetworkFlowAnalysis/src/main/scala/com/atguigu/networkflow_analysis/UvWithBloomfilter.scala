package com.caicai.networkflow_analysis
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessAllWindowFunction, ProcessWindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

object UvWithBloomfilter {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val inputStream: DataStream[String] = env.readTextFile("D:\\Projects\\BigData\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")

    val dataStream: DataStream[UserBehavior] = inputStream
      .map( (data: String) => {
        val dataArray: Array[String] = data.split(",")
        UserBehavior( dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong )
      } )
      .assignAscendingTimestamps((_: UserBehavior).timestamp * 1000L)


    val UvStream: DataStream[UvCount] = dataStream
      .filter((_: UserBehavior).behavior == "pv")
      .map(data => ("uv",data.userId))
      .keyBy(_._1)
      .timeWindowAll(Time.hours(1))
      //使用Trigger是因为默认全窗口函数是在窗口完了才对数据进行去重，
      // 这样会消耗很多内存，所以自定义trigger,实现来一个数据处理一次(但间接保存了数据的状态)，并清空之前的状态
    //有时候如果设置了allowedtimedelay这个用处明显，就是当达到windowEnd时候，一般是计算并且关闭窗口
    //这样设置的话（onELement = Fire）就是允许在延时的时候叠加计算
      .trigger(new MyTrigger())
      .process(new UvCountResultBloomFilter())

    println(UvStream)

    env.execute(" bloomfilter job")
  }

}

//首先自定义一个触发器，每次一个数据触发计算一次
/*TriggerResult是枚举类四种，其中表示是否触发一次计算，是否清空窗口状态
* false,false -> CONTINUE
* ture,ture -> FIRE_AND_PURGE  -- default
* false,true ->PURGE
* ture,false ->FIRE
* */
class MyTrigger() extends Trigger[(String,Long), TimeWindow] {
  //数据来了触发计算，清空状态,来一个元素就计算并清空状态,
    override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx:TriggerContext): TriggerResult =TriggerResult.FIRE

    override def onProcessingTime(time: Long, window: TimeWindow, ctx:TriggerContext): TriggerResult =
      TriggerResult.CONTINUE
    override def onEventTime(time: Long, window: TimeWindow, ctx:TriggerContext):TriggerResult =
      TriggerResult.CONTINUE
    override def clear(window:TimeWindow, ctx:TriggerContext): Unit = {}
  }

//自定义ProcessWIndowFunction,把当前数据进行处理，位图保存在Redis中(设置布隆过滤器)

class UvCountResultBloomFilter() extends ProcessAllWindowFunction[(String,Long),UvCount,TimeWindow] {
  var jedis: Jedis = _
  var bloom:Bloom = _
    override def open(parameters: Configuration): Unit = {
      jedis = new Jedis("hadoop104",6379)
      //假如有一亿个key,那么使用10亿个位(近似是128M)来增加容错率，就是需要1000*1000*1000肯定是小于2^10^3(1024^3)
      bloom = new Bloom(1 << 30)
    }
  override def process(context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
    //每来一个数据，主要用布隆过滤器来判断redis中是否对应的位置是1，就是判断是否存在，只要判断是0能判断不存在
    //用当前窗口的windowEnd作为ke'y,以Hash结构来保存到redis,(windowEnd,BitMap)
    val storedKey = context.window.getEnd.toString
    //把窗口中的UvCount值，作为状态也存在Redis中，存在一张Hash表中
    val countMap = "countMap"
    //先获取当前count的值
    var count= 0L


    if(jedis.hget(countMap,storedKey) != null)
      count = jedis.hget(countMap,storedKey).toLong

    var userId = elements.last._2.toString
    val offset = bloom.hash(userId,15)

    //利用这类API，判断某一个窗口内某一个用户是否存在过，这个就是主要操作，将userId转化为是否存在，其中，如何保证是否存在的容错率高，就是设计的hash
    //是否足够散列，offset比userId小，countMap只有一个，storedKey也很有限，所以主要就是userId的优化了，
    // 将userId转化为最小的占用内存单位Boolean(1个字节),通过高可靠性来对boolean判断并进行累加
    val isExist = jedis.getbit(storedKey,offset)

    //不存在，做如下操作
    if(!isExist){
      jedis.setbit(storedKey,offset,true)
      jedis.hset(countMap,storedKey,(count + 1).toString)
    }
  }
}

//自定义布隆过滤器,将数据转换为一个byte存储在redis中
//继承Serializable是否就是说这个可以序列化到磁盘中，而不需要一直呆在缓存中
class Bloom(size : Long) extends Serializable{

  //定义位图的大小，应该是2的整次幂
  private val cap = size

  //实现一个hash函数
  def hash(str:String,seed:Int):Long ={
    var result= 0
    for(i <- 0 until str.length){
      result = result*seed + str.charAt(i)
    }
    //返回一个在cap范围内的一个值
    (cap - 1)&result
  }
}
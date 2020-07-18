package day3

import java.lang.Exception
import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._


object JDBCSinkExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val sourceStream = env.addSource(new SensorSource)

    sourceStream.addSink(new Mysink)

    env.execute("user define jdbcSink")
  }

  class Mysink extends RichSinkFunction[SensorReading] {
    var conn: Connection = _
    var insertpstmt: PreparedStatement = _
    var updatepstmt: PreparedStatement = _

    override def open(parameters: Configuration): Unit = {
      conn = DriverManager.getConnection("jdbc:mysql://localhost/test", "root", "")
      insertpstmt = conn.prepareStatement("insert into temp values(?,?)")
      updatepstmt = conn.prepareStatement(("update temp set tempreture = ? where id =?"))
    }

    override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
      updatepstmt.setDouble(1, value.temperature)
      updatepstmt.setString(2, value.id)
      updatepstmt.execute()
      if (updatepstmt.getUpdateCount == 0) {
        insertpstmt.setDouble(1, value.temperature)
        insertpstmt.setString(2, value.id)
        insertpstmt.execute()
      }

    }

    override def close(): Unit = {
      insertpstmt.close()
      updatepstmt.close()
      conn.close()


    }
  }


}

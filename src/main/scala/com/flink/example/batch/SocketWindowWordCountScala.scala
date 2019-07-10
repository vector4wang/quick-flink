package com.flink.example.batch

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

/**
  *
  * @author vector
  * @date: 2019/7/10 0010 9:32
  *
  */
object SocketWindowWordCountScala {
  def main(args: Array[String]): Unit = {
    val port = try{
      ParameterTool.fromArgs(args).getInt("port")
    }catch {
      case e => {
        System.err.println("No port specified. Please run 'SocketWindowWordCount --port <port>'")
        return
      }
    }

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.socketTextStream("192.168.1.82",port,'\n')

    val windowCounts =  text.flatMap{ w=>w.split("\\s")}
      .map{w => Tuple2(w,1)}
      .keyBy(0)
      .sum(1)

    windowCounts.print().setParallelism(1)

    env.execute("Socket window WordCount For Scala")

  }
}

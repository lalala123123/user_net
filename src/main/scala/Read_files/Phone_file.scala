package Read_files

import java.text.SimpleDateFormat

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession

object Phone_file {

  def read_phone_file(path:String): Unit ={
    val sparkSession = SparkSession.builder().master("local").appName("SecondSpark").getOrCreate()
    val text = sparkSession.sparkContext.textFile(path)
    val call_edges = text.filter(line => line.contains("A1CALL")).map{line =>
      val fields = line.split('|')
      print(fields(5).toLong,fields(6).toLong,fields(12).toLong,fields(11).toLong)
      Edge(fields(5).toLong,fields(6).toLong,cal_time(fields(12).toString,fields(11).toString))
    }
    val cal_graph = Graph.fromEdges(call_edges,1L)
    print(cal_graph.numEdges)
  }

  def cal_time(start_time:String,end_time:String): Long ={
    val sdf = new SimpleDateFormat("yyMMddHHmmss")
    val cal = sdf.parse(end_time).getTime-sdf.parse(start_time).getTime

    return cal/1000L
  }
}

import Read_files.Phone_file
import org.apache.spark.sql.SparkSession

object Main {

  def main(args:Array[String]): Unit ={
    val sparkSession = SparkSession.builder().master("local").appName("FirstSpark").getOrCreate()
    print(sparkSession.sparkContext.textFile("./2017060109").first())
    Phone_file.read_phone_file("./2017060109")
  }
}

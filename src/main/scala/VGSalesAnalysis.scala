import java.io.BufferedOutputStream

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

object VGSalesAnalysis {
  def parseDouble(s: String) = try { s.toDouble } catch { case _ => 0.0 }

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("VGSalesAnalysis").getOrCreate()
    val sc = SparkContext.getOrCreate()

    val fileRDD = sc.textFile("hdfs://boise:4001/datasets/vgsales/vgsales.csv")

    // RDD filtered by "PC". If line contains "PC", it is added to the RDD
    val pcOnlyEntriesRDD = fileRDD.filter(s => s.split(",")(2).contains("PC"))
    // Calls cache on RDD to create checkpoint for when the lineage of the RDD branches out.
    //pcOnlyEntriesRDD.cache()

    // RDD containing relevant fields from the pcOnlyEntriesRDD
    val pcSalesDataRDD = pcOnlyEntriesRDD.map(s => s.split(",")(1) + "\t" + s.split(",")(10))

    // Maps the global sales and then reduces by adding them together
    val pcGlobalSales = pcOnlyEntriesRDD.map(s => parseDouble(s.split(",")(10)))
    val totalGlobalSalesPC = pcGlobalSales.reduce((a, b) => a + b)
    //totalGlobalSalesPC.saveAsTextFile("hdfs://boise:4001/vgsales-results/globalSalesOnPC")

    // Maps the North America sales and then reduces by adding them together
    val pcNA_Sales = pcOnlyEntriesRDD.map(s => parseDouble(s.split(",")(6)))
    val totalNA_SalesPC = pcNA_Sales.reduce((a, b) => a + b)

    // Maps the Europe sales and then reduces by adding them together
    val pcEU_Sales = pcOnlyEntriesRDD.map(s => parseDouble(s.split(",")(7)))
    val totalEU_SalesPC = pcEU_Sales.reduce((a, b) => a + b)

    // Maps the Japan sales and then reduces by adding them together
    val pcJP_Sales = pcOnlyEntriesRDD.map(s => parseDouble(s.split(",")(8)))
    val totalJP_SalesPC = pcJP_Sales.reduce((a, b) => a + b)

    // Maps the sales in other areas and then reduces by adding them together
    val pcOther_Sales = pcOnlyEntriesRDD.map(s => parseDouble(s.split(",")(9)))
    val totalOther_SalesPC = pcOther_Sales.reduce((a, b) => a + b)

    val path = new Path("hdfs://boise:4001/myfile.txt")
    val conf = new Configuration(spark.sparkContext.hadoopConfiguration)
    conf.setInt("dfs.blocksize", 16 * 1024 * 1024) // 16 MB HDFS Block Sizes
    val fs = path.getFileSystem(conf)
    if(fs.exists(path))
      fs.delete(path, true)
    val out = new BufferedOutputStream(fs.create(path))
    val txt = "Total PC Games Global Sales: " + totalGlobalSalesPC + " million\n" +
          "Total PC Games NA Sales: " + totalNA_SalesPC + " million\n" +
          "Total PC Games EU Sales: " + totalEU_SalesPC + " million\n" +
          "Total PC Games JP Sales: " + totalJP_SalesPC + " million\n" +
          "Total PC Games Other Country Sales: " + totalOther_SalesPC + " million"
    out.write(txt.getBytes("UTF-8"))
    out.flush()
    out.close()
    fs.close()






    /*
    * Fields in vgsales.csv:
    * Name is (1)
    * Platform is (2)
    * Global Sales is (10)
    */
    //val salesData = file.map(s => (s.split(",")(2), List(s.split(",")(1) , s.split(",")(10))))

    // Lookup Tables
    //val nameGlobalSalesMap = salesData.flatMap(s => (s(0) , s(2)))

    // Filter global sales by PC platform
    //val globalSalesOnPC = salesData.filter(x => x(0).contains( "PC"))


    //val data = salesData.collect{ case x => 0 }

    //globalSalesOnPC.saveAsTextFile("hdfs://boise:4001/vgsales-results/globalSalesOnPC")
    //val totalSales = globalSales.reduce((a, b) => a + b)

  }
}

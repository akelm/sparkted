/*
 *
MIT License

Copyright (c) 2018 Anna Kelm

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
 *
 */

package tedstats

import java.io.File
import java.net.URL
import java.nio.file.{Files, Paths}

import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.sys.process._
import scala.util.matching.Regex
import org.apache.spark.sql.functions.col
/*
Basing on TED CAN .csv yearly reports from http://data.europa.eu/euodp/repository/ec/dg-grow/mapps/,
computes average CAN number per year and average CAN value in EUR for each country.
 */

object TEDStats {

  def uniqueAward[T](ts: Iterable[mutable.Map[String,String]]): Iterable[Tuple3[String, Double,Int]] = {
    /* Function that takes data for single CAN
    and returns country ISO code and total CAN value in EUR
     */
    val cols = List("ISO_COUNTRY_CODE", "ISO_COUNTRY_CODE_ALL",
      "ID_AWARD","AWARD_VALUE_EURO_FIN_1")

    val tsRed =
      ts
        // removing multiple ID_AWARDS
        .map(row => {
        if (row("AWARD_VALUE_EURO_FIN_1") == null || row("AWARD_VALUE_EURO_FIN_1").isEmpty)
          row("AWARD_VALUE_EURO_FIN_1") = "0"
        row
      })
        .groupBy(x => x("ID_AWARD"))
        .values
        .map(x => x.reduce((a, b) => {
          a("AWARD_VALUE_EURO_FIN_1") =
            math.max(
              a("AWARD_VALUE_EURO_FIN_1").toDouble,
              b("AWARD_VALUE_EURO_FIN_1").toDouble
            ).toString
          a
        })
        )
        // sum over ID_AWARDS -- total CAN value
        .reduce((x, y) => {
        x("AWARD_VALUE_EURO_FIN_1") = (
          y("AWARD_VALUE_EURO_FIN_1").toDouble +
            x("AWARD_VALUE_EURO_FIN_1").toDouble
          ).toString
        x
      })

    // produces Array countries with all ISO country codes for CAN
    val countries ={ if (tsRed("ISO_COUNTRY_CODE_ALL") != null && tsRed("ISO_COUNTRY_CODE_ALL").nonEmpty) {
      tsRed("ISO_COUNTRY_CODE_ALL")
        .split("[^a-zA-Z]")
        .map(x => x.replaceAll("[^a-zA-Z]", ""))
        .filter(x => x.nonEmpty)
    }
    else Array(tsRed("ISO_COUNTRY_CODE")) }

    countries.map(c => Tuple3(c, tsRed("AWARD_VALUE_EURO_FIN_1").toDouble,tsRed("YEAR").toInt ) )
  }

  def main(args: Array[String]) {
    /* function arguments:
       yearStart -- the first year of stats
       yearStop -- the first year of stats
       basePath -- folder for download of TED data files (~500 MB/file) and results save
       ifRemFiles -- whether to remove downloaded files after execution
     */


    // parameters matching
    val intPattern = new Regex("^\\s*(\\d{4})\\s*$")
    val boolPattern = new Regex("^(true|false)$")
    var argMap = mutable.Map[String, Any]()
    args.indices.foreach(ind => {
      args(ind) match {
        case intPattern(sth) => if (!argMap.contains("yearStart")) argMap += "yearStart" -> sth.toInt
        else {
          if (!argMap.contains("yearEnd")) {
            if (sth.toInt < argMap("yearStart").asInstanceOf[Int]) {
              argMap += "yearEnd" -> argMap("yearStart")
              argMap("yearStart") = sth.toInt
            }
            else
              argMap += "yearEnd" -> sth.toInt
          }
        }
        case boolPattern(sth) => if (!argMap.contains("ifRemFiles")) argMap += "ifRemFiles" -> sth.toBoolean
        case x => if (!argMap.contains("basePath")) argMap += "basePath" -> x
      }
    })
    if (!argMap("basePath").asInstanceOf[String].endsWith("/")) argMap("basePath") += "/"

    val argDef = mutable.Map("yearStart" -> 2006, "yearEnd" -> 2017,"ifRemFiles" -> false)
    argDef.keys.foreach(key => {
      if (!argMap.contains(key)) argMap += key -> argDef(key)
    })

    if (argMap.contains("basePath")) {
      val dirLocal = new File(argMap("basePath").asInstanceOf[String])
      if (!dirLocal.exists() || !dirLocal.isDirectory) {
        println("Invalid temporary/results path specified!")
        System.exit(1)
      }
    }

    // setting spark
    val spark = SparkSession
      .builder
      .appName("Allegro Spark praktyki2018")
      .getOrCreate()
    import spark.implicits._

    // setting main RDD for data
    val sc = spark.sparkContext
    type pairRDD = mutable.Map[String, String]
    var resultRDD = sc.emptyRDD[pairRDD]

    // other important vars
    var filesDownloaded = ArrayBuffer[String]()
    var resName = ArrayBuffer[String]()
    val cols = List("ID_NOTICE_CAN", "ISO_COUNTRY_CODE", "ISO_COUNTRY_CODE_ALL",
      "ID_AWARD", "AWARD_VALUE_EURO_FIN_1")

    for (i <- argMap("yearStart").asInstanceOf[Int] to argMap("yearEnd").asInstanceOf[Int]) {

      // downloading file with TED CAN data
      val fileName = argMap("basePath").asInstanceOf[String] + "TED_CAN_" + i + ".csv"
      val addr = "http://data.europa.eu/euodp/repository/ec/dg-grow/mapps/TED_CAN_" + i + ".csv"
      try {

        val fileLocal = new File(fileName)
        if (!fileLocal.exists || fileLocal.length == 0) {
          filesDownloaded += fileName
          new URL(addr) #> fileLocal !!
        }

        if (fileLocal.length > 0) {
          // setting fileload params
          val file = spark
            .read
            .format("com.databricks.spark.csv")
            .option("header", "true")
            .option("quote", "\"")
            .option("escape", "\"")
            .load(fileName)

          // loading files to RDD
          resultRDD ++= file
            .select(cols.map(c=>col(c)).toSeq:_*)
            .rdd
            .map(row => {
              mutable.Map(cols.map(c => {
                c -> row.getAs[String](c)
              }).toMap.toSeq: _*)
            })
            .map(row => row += "YEAR" -> i.toString)

          resName += i.toString
        }
      }
      catch {
        case e: java.io.FileNotFoundException => println(e + "\n Remote file: " + addr + " not found!")
        case e: Throwable => println("Got exception: " + e)
      }
    }

    val yearRange = resName.length
    if (yearRange > 0) {
      resultRDD
        // sum of uniqe ID awards within CANs, duplicating CAN to multiple countries
        .keyBy(_("ID_NOTICE_CAN"))
        .groupByKey()
        .values
        .flatMap( uniqueAward )

        // computing (for each country) total av. CAN per yearRange years and average CAN value for non-zero CANs
        .map(x => {
        val ifVal = if (x._2 > 0) 1 else 0
        (x._1, (1, ifVal, x._2, x._3))
      })
        .groupByKey()
        .mapValues(x => {
          val cntSet = mutable.Set[Int]()
          x.foreach { y =>
            cntSet += y._4
          }
          val cntYear = cntSet.size
          x.map(y => (y._1.toDouble / cntYear.toDouble, y._2, y._3))
            .reduce((a,b) => (a._1+b._1,a._2+b._2,a._3+b._3))
        })
        .map(x => (x._1, x._2._1, x._2._3/x._2._2.toDouble))

        // saving to csv
        .toDF("country", "av./year CAN", "av CAN in EUR")
        .coalesce(1)
        .write
        .format("csv")
        .mode(SaveMode.Overwrite)
        .option("header", "true")
        .option("quoteMode", "ALL")
        .option("quote", "\"")
        .option("encoding", "UTF-8")
        .save(argMap("basePath").asInstanceOf[String] + "results_" + resName.mkString("_"))
    }

    else {
      println(argMap("yearStart").toString + " to " + argMap("yearEnd").toString + " yields an empty range!")
    }

    if (argMap("ifRemFiles").asInstanceOf[Boolean]) {
      // removes downloaded files
      filesDownloaded.foreach(fileName => {
        if (Files.exists(Paths.get(fileName))) new File(fileName).delete
      })
    }

    spark.stop()
  }
}

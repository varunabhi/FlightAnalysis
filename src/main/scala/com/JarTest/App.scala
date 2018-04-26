package com.JarTest

import java.io.{File, FileNotFoundException}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

/**
 * @author ${VARUN.THK7}
 */
object App {

  val changeToString: UserDefinedFunction =udf((x:Int) => x  match {
    case 1 => "Sunday"
    case 2 => "Monday"
    case 3 => "Tuesday"
    case 4 => "Wednesday"
    case 5 => "Thursday"
    case 6 => "Friday"
    case 7 => "Saturday"
  })

      def makeSchema(listOfColumns:List[String]): StructType ={
        val initColumns=listOfColumns.take(4)
        val startCol=initColumns.map((x) => StructField(x,IntegerType,nullable = false))
        val carrierCol=StructField(listOfColumns(4),StringType,nullable = false)
        val flightNum=StructField(listOfColumns(5),IntegerType,nullable = false)

        val midColumns= listOfColumns.slice(6,12)
       val centerCol= midColumns.map((x) => StructField(x,StringType,nullable = false))

        val cancelled=StructField(listOfColumns(12),IntegerType,nullable = false)
        val airTime=StructField(listOfColumns(14),StringType,nullable = false)
        val distance=StructField(listOfColumns(15),IntegerType,nullable = false)

        val listOfFields=startCol:::List(carrierCol):::List(flightNum):::centerCol:::List(cancelled,airTime,distance)
        StructType(listOfFields)

      }

        def mapDelays(spark:SparkSession,df:DataFrame):DataFrame={
          val exactOnTimeDf=spark.sql("select count(*) as ExactOnTime from flights where DEP_DELAY=0 and ARR_DELAY=0")

          val DepOnTime=df.filter(df("DEP_DELAY")===0).count()
          val ArrivalOnTime=df.filter(df("ARR_DELAY")===0).count()
          val LateDep=df.filter(df("DEP_DELAY")>0 ).count()
          val lateArr=df.filter(df("ARR_DELAY")>0 ).count()
          val earlyDep=df.filter(df("DEP_DELAY")<0 ).count()
          val earlyArr=df.filter(df("ARR_DELAY")<0 ).count()
          val earlyDepAndEarlyArr=df.filter(df("DEP_DELAY")<0 && df("ARR_DELAY")<0).count()
          val earlyDepAndLateArr=df.filter(df("DEP_DELAY")<0 && df("ARR_DELAY")>0).count()
          val lateDepAndEarlyArr=df.filter(df("DEP_DELAY")>0 && df("ARR_DELAY")<0).count()
          val lateDepAndLateArr=df.filter(df("DEP_DELAY")>0 && df("ARR_DELAY")>0).count()

          val finalDataFrame=exactOnTimeDf.withColumn("departureOnTime",lit(DepOnTime))
            .withColumn("ArrivalOnTime",lit(ArrivalOnTime))
            .withColumn("LateDeparture",lit(LateDep))
            .withColumn("lateArrival",lit(lateArr))
            .withColumn("earlyDeparture",lit(earlyDep))
            .withColumn("EarlyArrival",lit(earlyArr))
            .withColumn("earlyDepAndArr",lit(earlyDepAndEarlyArr))
            .withColumn("lateDepAndlateArrival",lit(lateDepAndLateArr))

          finalDataFrame

        }

      def rowMaker(line:List[String]): Row ={
        val mapInitCol=line.take(4).map(_.toInt)
        val carrier=List(line(4).toString)
        val flightNum=List(line(5).toInt)
        val midCol=line.slice(6,12).map(_.toString)
        val cancelled=List(line(12).toInt)
        val airTime =List(line(14).toString)
        val distance=List(line(15).toInt)
        val lst=mapInitCol:::carrier:::flightNum:::midCol:::cancelled:::airTime:::distance
        Row.fromSeq(lst)
      }

  def bindDelaysWithRationAndPercent(spark:SparkSession, df:DataFrame): DataFrame ={
      val totalDays=spark.sql("SELECT DAY_OF_WEEK as DaysTotal,Count(*) as Count from flights GROUP BY DAY_OF_WEEK")
    val totalDaysToString=spark.sql("SELECT Count(*) from flights GROUP BY DAY_OF_WEEK")

    val dfDayOfWeek=spark.sql("SELECT DAY_OF_WEEK,Count(*) as Delayed FROM flights WHERE DEP_DELAY > 0 AND DEP_DELAY!='NA' GROUP BY DAY_OF_WEEK")
    val updated1=dfDayOfWeek.join(totalDays,dfDayOfWeek("DAY_OF_WEEK")===totalDays("DaysTotal"))

    val ds=updated1.persist()
    val dataFrameDup=updated1.withColumn("DAY_OF_WEEK_Name",changeToString(ds("DAY_OF_WEEK")))
    val dd = dataFrameDup.select("DAY_OF_WEEK_Name","Delayed","Count")

    val percentDf=dd.withColumn("Delayed(%)" , (dd.col("Delayed")/ dd("Count")) * 100)

    val onTimeDf= spark.sqlContext.sql("select DAY_OF_WEEK as Days,Count(*) as OnTime from flights where ARR_DELAY=0 AND DEP_DELAY=0 GROUP BY DAY_OF_WEEK")
    val dfToDaysName=onTimeDf.withColumn("DAY_OF_WEEK2",changeToString(onTimeDf("Days")))
    val updated=dfToDaysName.select("DAY_OF_WEEK2","OnTime")
    val withOnTime= percentDf.join(updated,percentDf.col("DAY_OF_WEEK_Name")===updated.col("DAY_OF_WEEK2"))

    val updatedDf=withOnTime.select("DAY_OF_WEEK_Name","Delayed","Delayed(%)","OnTime","Count")
      .withColumn("OnTime(%)",(withOnTime("OnTime")/withOnTime("Count")) * 100)

    val finalDataFrame=updatedDf.withColumn("Ratio",updatedDf("Delayed")/updatedDf("OnTime"))

    finalDataFrame
  }

  def finalResults(spark:SparkSession): DataFrame ={
    val dataFrame=spark.sql("select FL_NUM,Count(*) as TotalCount from flights GROUP BY FL_NUM ORDER BY FL_NUM")
    val dataFrameWithDelay=spark.sql("select FL_NUM as FL_NUM2,Count(*) as DelayedCount from flights where DEP_DELAY > 0 GROUP BY FL_NUM ORDER BY FL_NUM")
    val updated=dataFrame.join(dataFrameWithDelay,dataFrame("FL_NUM")===dataFrameWithDelay("FL_NUM2"))
         val percentDf=  updated.withColumn("PercentageDelayed",(dataFrameWithDelay("DelayedCount")/dataFrame("TotalCount")) * 100)

    val dataFrameWithDelayOnTime=spark.sql("select FL_NUM as FL_NUM3,Count(*) as OnTime from flights where DEP_DELAY = 0 AND ARR_DELAY=0 GROUP BY FL_NUM ORDER BY FL_NUM")
    val updatedOnTime=percentDf.join(dataFrameWithDelayOnTime,percentDf("FL_NUM")===dataFrameWithDelayOnTime("FL_NUM3"))
    val percentDfOnTime=  updatedOnTime.withColumn("PercentageDelayedOnTime",(updatedOnTime("OnTime")/updatedOnTime("TotalCount")) * 100)
    val finalDF=percentDfOnTime.select("FL_NUM","TotalCount","DelayedCount","PercentageDelayed","OnTime","PercentageDelayedOnTime")
    finalDF

  }


  def addToCluster(dataFrame:DataFrame): Unit = {
    dataFrame.write.format("orc").mode(SaveMode.Overwrite).saveAsTable("flight.flightDetails")
      println("Successfully Inserted")
  }

  def main(args : Array[String]): Unit = {

    val spark= SparkSession.builder().master("local[*]").enableHiveSupport().appName("JarTest").getOrCreate()
//          val file=new File(args(0))
//            if(!file.exists()){
//              throw FileNotFoundException
//            }
      val fileRdd=spark.sparkContext.textFile("hdfs:///user/test_df.csv")
          val lstOfColumns=fileRdd.first().split(",").toList
          val schema= makeSchema(lstOfColumns)

            val newRdd=fileRdd.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }.map(_.split(",").toList).map(rowMaker)
             val df= spark.createDataFrame(newRdd,schema)

            df.createOrReplaceTempView("flights")
            val firstDataframe=mapDelays(spark,df)
            firstDataframe.show()
            val finalDataFrame=bindDelaysWithRationAndPercent(spark,df)
            finalDataFrame.show()
            finalResults(spark).show()
//    finalDataFrame.printSchema()
            addToCluster(finalDataFrame)
              spark.stop()
    }

}

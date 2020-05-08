package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{

  def ST_Contains(pointString:String, queryRectangle:String): Boolean = {

    var pstr = pointString.trim()
    var point = pstr.split(",")
    var px = point(0).toDouble
    var py = point(1).toDouble
    var tstr = queryRectangle.trim()
    var rectangle = tstr.split(",")
    var x1 = rectangle(0).toDouble
    var y1 = rectangle(1).toDouble
    var x2 = rectangle(2).toDouble
    var y2 = rectangle(3).toDouble

    if (x1 > x2){
      var temp = x1
      x1 = x2
      x2 = temp
    }

    if (y1 > y2){
      var temp = y1
      y1 = y2
      y2 = temp
    }

    if (px >= x1 && px <= x2 && py >= y1 && py <= y2)
      return true
    else
      return false
  }

  def ST_Within(pointString1:String, pointString2:String, distance:Double):Boolean={
    try{
      //You first need to parse the pointString1 to a format that you are comfortable with.

      val FirstPoint = pointString1.trim().split(",")
      val FirstPoint_x=FirstPoint(0).toDouble
      val FirstPoint_y=FirstPoint(1).toDouble

      //And parse the pointString2 to a format that you are comfortable with.

      val SecondPoint = pointString2.trim().split(",")
      val SecondPoint_x = SecondPoint(0).toDouble
      val SecondPoint_y=SecondPoint(1).toDouble

      //Assume all coordinates are on a planar space and calculate their Euclidean distance.
      var PointDis=Math.sqrt((SecondPoint_x - FirstPoint_x)*(SecondPoint_x - FirstPoint_x) + (SecondPoint_y - FirstPoint_y)*(SecondPoint_y - FirstPoint_y))

      // Then check whether the two points are within the given distance. Consider on-boundary point.
      if (PointDis>=distance)
        return false
      else
        return true

    }
      //Catch the error and exception and return false.
    catch {
      case _: Exception => return false
      case _: Error => return false
    }

  }

  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>( ST_Contains(pointString, queryRectangle) ))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(ST_Contains(pointString, queryRectangle)))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((ST_Within(pointString1, pointString2, distance))))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((ST_Within(pointString1, pointString2, distance))))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}

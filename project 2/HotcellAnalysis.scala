package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)



  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
  {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    //pickupInfo.show()

    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 0)
      )))
    spark.udf.register("CalculateY",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 1)
      )))
    spark.udf.register("CalculateZ",(pickupTime: String)=>((
      HotcellUtils.CalculateCoordinate(pickupTime, 2)
      )))
    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
    var newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName:_*)


    // Define the min and max of x, y, z
    val minX = (-74.50/HotcellUtils.coordinateStep).toInt
    val maxX = (-73.70/HotcellUtils.coordinateStep).toInt
    val minY = (40.50/HotcellUtils.coordinateStep).toInt
    val maxY = (40.90/HotcellUtils.coordinateStep).toInt
    val minZ = 1
    val maxZ = 31

    /*val minX = 0
    val maxX = 2
    val minY = 0
    val maxY = 2
    val minZ = 0
    val maxZ = 2*/

    val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

    import spark.implicits._
    val offset_x = maxX - minX + 1
    val offset_y = maxX - minX + 1
    val offset_z = maxX - minX + 1

    val cell = Seq.tabulate(offset_x, offset_y, offset_z)((a, b, c) => (a + minX, b + minY, c + minZ))
    val cells = cell.flatten.flatten.toDF("x", "y", "z")
    pickupInfo.createOrReplaceTempView("pickupInfo")

    //cells.show()
    cells.createOrReplaceTempView("cells")

    //val pickup = spark.sql("SELECT count(*) as c, cells.x, cells.y, cells.z FROM cells FULL JOIN pickupInfo ON cells.x = pickupInfo.x AND cells.y=pickupInfo.y AND cells.z=pickupInfo.z GROUP BY cells.x, cells.y, cells.z")
    var pickup = spark.sql("SELECT x,y,z,count(*) as c FROM pickupInfo GROUP BY x,y,z")
    //pickup.show()
    pickup.createOrReplaceTempView("pickup")


//    var data = spark.sql(" SELECT cells.x, cells.y, cells.z, pickup.c FROM cells FULL JOIN pickup ON cells.x = pickup.x AND cells.y=pickup.y AND cells.z=pickup.z ORDER BY pickup.c DESC")
//    data.createOrReplaceTempView("data")
    pickup = pickup.withColumn("c", coalesce('c, lit(0)))
    pickup = pickup.withColumn("c_sqr", col("c")*col("c"))
//    //data.show()
    pickup.createOrReplaceTempView("pickup")

//    var for_g = spark.sql("SELECT sum(data.c) as sc, sum(data.c_sqr) as scq FROM data")
//    for_g.createOrReplaceTempView("for_g")
//    for_g = for_g.withColumn("mean",col("sc")/numCells)
//    for_g = for_g.withColumn("S", sqrt(col("scq")/numCells - (col("mean") * col("mean"))))
//    for_g.createOrReplaceTempView("for_g")
//    for_g.show()

    var for_g = spark.sql("SELECT sum(c) as sc, sum(c_sqr) as scq FROM pickup")
    for_g.createOrReplaceTempView("for_g")
    for_g = for_g.withColumn("mean",col("sc")/numCells)
    for_g = for_g.withColumn("S", sqrt(col("scq")/numCells - (col("mean") * col("mean"))))
    for_g.createOrReplaceTempView("for_g")

    spark.udf.register("IsNeighbor", (cells_x: Int, cells_y: Int, cells_z: Int, point_x: Int, point_y : Int, point_z :Int) => (HotcellUtils.IsNeighbor(cells_x,cells_y,cells_z,point_x,point_y,point_z)))
    var g_info = spark.sql("select d1.x, d1.y, d1.z, sum(d2.c) as s from cells as d1, pickup as d2 where IsNeighbor(d1.x,d1.y,d1.z,d2.x,d2.y,d2.z) group by d1.x,d1.y,d1.z ")
    g_info.createOrReplaceTempView("g_info")
//    g_info.show()
//    var vali_g=spark.sql("select count(*), count(distinct(co)) from g_info").show()
    spark.udf.register("FindNeighbor", (x: Int, y: Int, z: Int) => (HotcellUtils.FindNeighbor(x,y,z)))

    var g_count = spark.sql("SELECT x,y,z,FindNeighbor(x,y,z) as w FROM cells")
//    g_count.show()
    g_count.createOrReplaceTempView("g_count")

    spark.udf.register("calc_gscore", (mean: Double, s: Double, gs: Double, gw:Double) => (HotcellUtils.calc_gscore(mean,s,gs,gw)))
    var gg = spark.sql("SELECT gc.x, gc.y, gc.z, gc.w, gi.s FROM g_count as gc LEFT JOIN g_info as gi ON gc.x = gi.x AND gc.y = gi.y AND gc.z = gi.z ")
    gg = gg.withColumn("s", coalesce('s, lit(0)))
    //gg.show()
    gg.createOrReplaceTempView("gg")

    var gscore = spark.sql("SELECT gg.x, gg.y, gg.z FROM gg cross join for_g as fg ORDER BY calc_gscore(fg.mean, fg.S, gg.s, gg.w) DESC")
    //gscore.show()
    gscore.createOrReplaceTempView("gscore")
    return gscore // YOU NEED TO CHANGE THIS PAR——info
  }
}


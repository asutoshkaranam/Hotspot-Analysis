package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

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
  pickupInfo.show()

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
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  // YOU NEED TO CHANGE THIS PART --- START
  
  pickupInfo.createOrReplaceTempView("pickupInfo")
  
  val PointCoords = spark.sql("select x,y,z,count(*) as countVal from pickupInfo where x>=" + minX + " and x<=" + maxX + " and y>="+minY +" and y<="+maxY+" and z>="+minZ+" and z<=" +maxZ +" group by x,y,z").persist()
  PointCoords.createOrReplaceTempView("PointCoords")    
    
  val pnt = spark.sql("select sum(countVal) as sumVal, sum(countVal*countVal) as sumSqr from PointCoords").persist()
  val sumVal = pnt.first().getLong(0).toDouble
  val sumSqr = pnt.first().getLong(1).toDouble  
  
  val mean = (sumVal/numCells)
  val std_dev = Math.sqrt((sumSqr/numCells) - (mean*mean))   
  
  val allNeighbour = spark.sql("select geoPoint1.x as x , geoPoint1.y as y, geoPoint1.z as z, count(*) as neighborCnt, sum(geoPoint2.countVal) as summation from PointCoords as geoPoint1 inner join PointCoords as geoPoint2 on ((abs(geoPoint1.x-geoPoint2.x) <= 1 and  abs(geoPoint1.y-geoPoint2.y) <= 1 and abs(geoPoint1.z-geoPoint2.z) <= 1)) group by geoPoint1.x, geoPoint1.y, geoPoint1.z").persist()
  allNeighbour.createOrReplaceTempView("allNeighbour")
  
  spark.udf.register("CalculateGScore",(mean: Double, stddev: Double, neighborCnt: Int, summation: Int, numCells: Int)=>((
    HotcellUtils.CalculateGScore(mean, stddev, neighborCnt, summation, numCells)
    )))  
  
  val inclGscore =  spark.sql("select x,y,z,CalculateGScore("+ mean + ","+ std_dev +",neighborCnt,summation," + numCells+") as gscore from allNeighbour")
  inclGscore.createOrReplaceTempView("inclGscore")
  
  val res = spark.sql("select x,y,z from inclGscore order by gscore desc")
  return res

  // return pickupInfo // YOU NEED TO CHANGE THIS PART -- END
}
}

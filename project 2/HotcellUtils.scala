package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01
  //Change this part for actual dataset
  /*val minX = 0
  val maxX = 2
  val minY = 0
  val maxY = 2
  val minZ = 0
  val maxZ = 2*/
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31


  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }


  def FindNeighbor(x: Int, y: Int, z: Int): Int =
  {
    var pointloc = 0
    if (x == maxX || x ==minX)
      pointloc += 1;
    if( y == maxY || y == minY)
      pointloc += 1;
    if(z == maxZ || z == minZ)
      pointloc += 1;

    pointloc match {
      case 0 => return 27
      case 1 => return 18
      case 2 => return 12
      case 3 => return 8
    }

  }

  def IsNeighbor(cells_x: Int, cells_y: Int, cells_z: Int, point_x: Int, point_y : Int, point_z :Int) : Boolean =
  {
    val x_diff = (cells_x - point_x).abs
    val y_diff = (cells_y - point_y).abs
    val z_diff = (cells_z - point_z).abs

    if(x_diff <= 1 && y_diff <=1 && z_diff <= 1)
      return true
    else
      return false
  }

  def calc_gscore(mean: Double, s: Double, gs: Double, gw:Double) : Double =
    {
      return (gs - mean * gw) / ( s * Math.sqrt((numCells * gw - gw * gw) / (numCells - 1)))
    }

}

package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String): Boolean = {
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

    if (x1 > x2) {
      var temp = x1
      x1 = x2
      x2 = temp
    }

    if (y1 > y2) {
      var temp = y1
      y1 = y2
      y2 = temp
    }

    if (px >= x1 && px <= x2 && py >= y1 && py <= y2)
      true
    else
      false

  }

}

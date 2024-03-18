package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    
	// START --- YOU NEED TO CHANGE THIS PART
    val rctngl_coords = queryRectangle.split(",")
	val tgt_pnt_coords = pointString.split(",")

	val x_coord: Double = tgt_pnt_coords(0).trim.toDouble
	val y_coord: Double = tgt_pnt_coords(1).trim.toDouble
	val rctngl_x_coord1: Double = math.min(rctngl_coords(0).trim.toDouble, rctngl_coords(2).trim.toDouble)
	val rctngl_y_coord1: Double = math.min(rctngl_coords(1).trim.toDouble, rctngl_coords(3).trim.toDouble)
	val rctngl_x_coord2: Double = math.max(rctngl_coords(0).trim.toDouble, rctngl_coords(2).trim.toDouble)
	val rctngl_y_coord2: Double = math.max(rctngl_coords(1).trim.toDouble, rctngl_coords(3).trim.toDouble)

	if ((x_coord >= rctngl_x_coord1) && (x_coord <= rctngl_x_coord2) && (y_coord >= rctngl_y_coord1) && (y_coord <= rctngl_y_coord2)) {
		return true
	}
	return false
    // return true // END --- YOU NEED TO CHANGE THIS PART
  }
  // YOU NEED TO CHANGE THIS PART

}

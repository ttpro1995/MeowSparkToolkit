package me.thaithien.sparktoolkit.common.utils

object MathTools {
  def calculateLonLatDistance: ((Float, Float), (Float, Float)) => Double =
    (lonlat1: (Float, Float), lonlat2: (Float, Float)) => {
      val R = 6371

      val lat1 = lonlat1._1
      val lon1 = lonlat1._2
      val lat2 = lonlat2._1
      val lon2 = lonlat2._2

      val latDistance = math.toRadians(lat2 - lat1)
      val lonDistance = math.toRadians(lon2 - lon1)
      val a = math.sin(latDistance / 2) * math.sin(latDistance / 2) +
        math.cos(math.toRadians(lat1)) * math.cos(math.toRadians(lat2)) * math.sin(lonDistance / 2) *
          math.sin(lonDistance / 2)
      val c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
      val distance = R * c * 1000

      distance
    }
}

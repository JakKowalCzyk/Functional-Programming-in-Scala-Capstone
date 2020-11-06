package observatory

import java.lang.Math._

import com.sksamuel.scrimage.{Image, Pixel, writer}

/**
  * 2nd milestone: basic visualization
  */
object Visualization extends VisualizationInterface {

  val earthRadius = 6371

  def calculateInverseDistance(distancesTemps: Iterable[(Double, Temperature)]): Temperature = {
    distancesTemps.aggregate((0.0, 0.0))({
      case ((sA, sI), (dist, temp)) =>
        val w = 1 / Math.pow(dist, 2)
        (w * temp + sA, w + sI)
    }, {
      case ((sAA, sIA), (sAB, sIB)) => (sAA + sAB, sIA + sIB)
    }) match {
      case (sA, sI) => sA / sI
    }
  }

  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location     Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature = {
    val distancesTemps = mapLocationsToDistances(temperatures, location)
    distancesTemps.find(_._1 == 0) match {
      case Some((_, temp)) => temp
      case None => calculateInverseDistance(distancesTemps)
    }
  }

  def mapLocationsToDistances(temperatures: Iterable[(Location, Temperature)], location: Location) =
    temperatures.map({ case (loc, temperature) => (distBetweenLocations(loc, location), temperature) })


  def distBetweenLocations(locationA: Location, locationB: Location): Double = {
    def longLatToRadians(value: Double): Double = {
      (Math.PI / 180) * value
    }

    val delta = acos(sin(longLatToRadians(locationA.lat)) * sin(longLatToRadians(locationB.lat))
      + cos(longLatToRadians(locationA.lat)) * cos(longLatToRadians(locationB.lat)) * cos(longLatToRadians(locationB.lon - locationA.lon)))
    delta * earthRadius
  }

  def calculateLinearValue(pAvalue: Temperature, pBvalue: Temperature, value: Temperature)(colorValueMin : Int, colorValueMax:Int ) = {
    val factor = (value - pAvalue) / (pBvalue - pAvalue)
    round(colorValueMin + (colorValueMax - colorValueMin) * factor).toInt
  }

  def linearInterpolate(pointA: Option[(Temperature, Color)], pointB: Option[(Temperature, Color)], value: Temperature): Color = (pointA, pointB) match {
    case (Some(pA), None) => pA._2
    case (None, Some(pB)) => pB._2
    case (Some((pAvalue, pAcolor)), Some((pBvalue, pBcolor))) =>
      val linearValue = calculateLinearValue(pAvalue, pBvalue, value) _
      Color(linearValue(pAcolor.red, pBcolor.red), linearValue(pAcolor.green, pBcolor.green),
        linearValue(pAcolor.blue, pBcolor.blue))
    case _ => Color(0, 0, 0)
  }

  /**
    * @param points Pairs containing a value and its associated color
    * @param value  The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(points: Iterable[(Temperature, Color)], value: Temperature): Color = {
    points.find(_._1 == value) match {
      case Some((_, color)) => color
      case None =>
        val (small, great) = points.toSeq.sortBy(_._1).partition(_._1 < value)
        linearInterpolate(small.reverse.headOption, great.headOption, value)
    }
  }

  private val imgWidth = 360
  private val imgHeight = 180

  /**
    * @param temperatures Known temperatures
    * @param colors       Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {
    val pixels = (0 until imgHeight*imgWidth).par.map({
      pos => pos -> interpolateColor(colors, predictTemperature(temperatures, posToLocation(pos))).pixel
    }).seq.sortBy(_._1).map(_._2)
    Image(imgWidth, imgHeight, pixels.toArray)
  }

  def posToLocation(pos: Int): Location = {
    val x: Int = pos % imgWidth
    val y: Int = pos / imgHeight

    Location(90 - y, x - imgHeight)
  }

}


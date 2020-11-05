package observatory

import java.lang.Math.round
import java.time.{LocalDate, Month}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.log4j.{Level, Logger}

import scala.io.Source

/**
  * 1st milestone: data extraction
  */
object Extraction extends ExtractionInterface {
  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("TemperaturesExtraction")
  @transient lazy val sc: SparkContext = new SparkContext(conf)
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  def rawStations(lines: RDD[String]): RDD[Station] =
    lines.map(line => {
      val arr = line.split(",", -1)
      Station(
        getStationID(arr),
        location = if (arr(2) == "" && arr(3) == "") None else Some(Location(arr(2).toDouble, arr(3).toDouble))
      )
    })

  def rawPoints(lines: RDD[String]): RDD[MeasurePoint] =
    lines.map(line => {
      val arr = line.split(",", -1)
      MeasurePoint(
        getStationID(arr),
        month = arr(2).toInt,
        day = arr(3).toInt,
        temperature = fahrenheitToCelsius(arr(4).toDouble)
      )
    })

  def fahrenheitToCelsius(d: Double) = (round((d - 32) * 5 / 9)).toDouble


  private def getStationID(arr: Array[String]) =
    StationID(
      STN = if (arr(0) == "") None else Some(arr(0).toInt),
      WBAN = if (arr(1) == "") None else Some(arr(1).toInt))


  def getRddFromResource(resource: String): RDD[String] = {
    val fileStream = Source.getClass.getResourceAsStream(resource)
    sc.makeRDD(Source.fromInputStream(fileStream).getLines().toList)
  }
  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {
//    val stationLines = sc.textFile(getClass.getResource(stationsFile).getPath)
    val stationLines = getRddFromResource(stationsFile)
    val stations = rawStations(stationLines)
      .filter(stat => stat.location.isDefined)
      .map(stat => (stat.stationID, stat))

//    val pointsLines = sc.textFile(getClass.getResource(temperaturesFile).getPath)
    val pointsLines = getRddFromResource(temperaturesFile)
    val points = rawPoints(pointsLines).map(point => (point.stationID, point))

    stations.join(points).mapValues({ case (station, point) =>
      (LocalDate.of(year, Month.of(point.month), point.day), station.location.get, point.temperature)
    }).values.collect()
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    sc.parallelize(records.toSeq).map({ case (date, location, temperature) => ((location, date.getYear), (temperature, 1)) })
      .reduceByKey({ case ((sumL, countL), (sumR, countR)) =>
        (sumL + sumR, countL + countR)
      }).map({ case (locYear, tempIter) => (locYear._1, tempIter._1/tempIter._2)}).collect()
  }


}

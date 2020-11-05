package observatory

import java.lang.Math.round
import java.time.{LocalDate, Month}

import observatory.Extraction.{fahrenheitToCelsius, locateTemperatures, locationYearlyAverageRecords}
import org.junit.Assert._
import org.junit.Test

trait ExtractionTest extends MilestoneSuite {
  private val milestoneTest = namedMilestoneTest("data extraction", 1) _

  private val location1: Location = Location(12, 13)

  @Test def`testFahrenheitToCelsius` : Unit = {
    assert(27 == fahrenheitToCelsius(80))
  }

  @Test def `testLocateTemperatures`: Unit = {

    val result = locateTemperatures(2000, "/stations_test.csv", "/temp_test.csv")

    assert(result.size == 10)
    assert(result.exists({ case (date, location, temperature) =>
      (date == LocalDate.of(2000, 7, 18)) && (temperature == fahrenheitToCelsius(83.6)) && location == Location(00.000,000.000)
    }))
    assert(result.exists({ case (date, location, temperature) =>
      date == LocalDate.of(2000, 7, 19) && temperature == fahrenheitToCelsius(81.4)
    }))
  }

  @Test def `testSingleLocateTemperatures`: Unit = {

    val result = locateTemperatures(2000, "/stations_test.csv", "/single_temp_test.csv")

    assert(result.size == 1)
    assert(result.head._1.getYear == 2000)
    assert(result.head._1.getDayOfMonth == 29)
    assert(result.head._1.getMonth == Month.of(7))
    assert(result.head._2.lat == -23.867)
    assert(result.head._2.lon == 029.433)
    assert(result.head._3 == fahrenheitToCelsius(81))
  }


  @Test def `singleLocationYearlyAverageRecords`: Unit = {

    val input = Iterable((LocalDate.of(2020, 12, 12), location1, 20.0))
    val result = locationYearlyAverageRecords(input)
    assert(result.size == 1)
    assert(result.head._2 == 20.0)
  }

  @Test def `twoSameKeyEntriesLocationYearlyAverageRecords`: Unit = {

    val input = Iterable((LocalDate.of(2020, 12, 12), location1, 20.0),
      (LocalDate.of(2020, 11, 10), location1, 0.0))
    val result = locationYearlyAverageRecords(input)
    assert(result.size == 1)
    assert(result.head._2 == 10.0)
    assert(result.head._1 == location1)
  }

  @Test def `twoDifferentKeysByLocationEntriesLocationYearlyAverageRecords`: Unit = {

    val input = Iterable((LocalDate.of(2020, 12, 12), location1, 20.0),
      (LocalDate.of(2010, 11, 10), Location(0, 0), 0.0))
    val result = locationYearlyAverageRecords(input)

    assert(result.size == 2)
    assert(result.exists({ case (location, temperature) => temperature == 20.0 }))
    assert(result.exists({ case (location, temperature) => temperature == 0.0 }))
  }


  @Test def `twoSameKeysAndOneDifferentEntriesLocationYearlyAverageRecords`: Unit = {

    val input = Iterable((LocalDate.of(2020, 12, 12), location1, 20.0),
      (LocalDate.of(2020, 12, 12), location1, 0.0),
      (LocalDate.of(2010, 11, 10), Location(0, 0), 0.0))
    val result = locationYearlyAverageRecords(input)
    assert(result.size == 2)
    assert(result.exists({ case (location, temperature) => temperature == 10.0 }))
    assert(result.exists({ case (location, temperature) => temperature == 0.0 }))
  }

  @Test def `fromFileLocationYearlyAverageRecords`: Unit = {

    val result = locationYearlyAverageRecords(locateTemperatures(2000, "/stations_test.csv", "/temp_test.csv"))
    assert(result.size == 3)
    assert(result.exists({ case (location, temperature) => temperature == 24 }))
  }


}

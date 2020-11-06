package observatory

import org.junit.Assert._
import org.junit.Test

trait VisualizationTest extends MilestoneSuite {
  private val milestoneTest = namedMilestoneTest("raw data display", 2) _

  // Implement tests for the methods of the `Visualization` object

  @Test def `distBetweenLocationsPoznanWarsaw` : Unit = {
    assert(Visualization.distBetweenLocations(Location(52.406376, 16.925167), Location(52.237049, 21.017532)) < 280)
    assert(Visualization.distBetweenLocations(Location(52.406376, 16.925167), Location(52.237049, 21.017532)) > 270)
  }

 @Test def `distBetweenLocationsPoznanTokyo` : Unit = {
    assert(Visualization.distBetweenLocations(Location(52.406376, 16.925167), Location(35.652832, 139.839478)) < 8900)
    assert(Visualization.distBetweenLocations(Location(52.406376, 16.925167), Location(35.652832, 139.839478)) > 8760)
  }

  @Test def `predictTemperature` : Unit = {
    assert(Visualization.predictTemperature(List((Location(45.0, -90.0), 10.0), (Location(-45.0, 0.0), 20.0)), Location(0.0, -45.0)) == 15.0)
    assert(Visualization.predictTemperature(List((Location(0.0, 0.0), 10.0)), Location(0.0, 0.0)) == 10.0)
  }

  @Test def `calculateLinearValue`:Unit = {
    assert(Visualization.calculateLinearValue(0, 10, 5)(0, 100) == 50)
    assert(Visualization.calculateLinearValue(2, 12, 7)(0, 100) == 50)
    assert(Visualization.calculateLinearValue(2, 12, 7)(10, 20) == 15)
  }


}

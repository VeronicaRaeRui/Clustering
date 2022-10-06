package edu.cse6250.phenotyping

import edu.cse6250.helper.SessionCreator
import edu.cse6250.main.Main

import java.text.SimpleDateFormat
import SessionCreator.sparkMasterURL
import edu.cse6250.util.LocalClusterSparkContext
import edu.cse6250.model.{ Diagnostic, LabResult, Medication }
import edu.cse6250.helper.{ CSVHelper, SessionCreator }
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ SQLContext, SparkSession }
import org.scalatest.{ BeforeAndAfter, FlatSpec, FunSuite, Matchers }
import org.scalatest.concurrent.TimeLimitedTests
import org.scalatest.time.{ Seconds, Span }
import org.apache.log4j.{ Level, Logger }

/**
 * @author Sungtae An <stan84@gatech.edu>.
 */
class T2dmPhenotypeTest extends FlatSpec with BeforeAndAfter with Matchers {
  var spark: SparkSession = _

  before {
    Logger.getRootLogger.setLevel(Level.WARN)
    Logger.getLogger("org").setLevel(Level.WARN)
    spark = SessionCreator.createSparkSession(appName = "Test PheKBPhenotype")
  }

  after {
    spark.stop()
  }

  "transform" should "give expected results" in {
    // test("transform get give expected results") {
    val sqlContext = spark.sqlContext
    val (med, lab, diag) = Main.loadRddRawData(spark)
    val rdd = T2dmPhenotype.transform(med, lab, diag)
    val cases = rdd.filter { case (x, t) => t == 1 }.map { case (x, t) => x }.collect.toSet
    val controls = rdd.filter { case (x, t) => t == 2 }.map { case (x, t) => x }.collect.toSet
    val others = rdd.filter { case (x, t) => t == 3 }.map { case (x, t) => x }.collect.toSet
    cases.size should be(427 + 255 + 294)
    controls.size should be(948)
    others.size should be(3688 - cases.size - controls.size)
  }
  "stat_calc" should "give expected results" in {
    // test("stat_calc get give expected results") {
    val sqlContext = spark.sqlContext
    val (med, lab, diag) = Main.loadRddRawData(spark)
    val rdd = T2dmPhenotype.transform(med, lab, diag)

    /** Ground truth data */
    val case_mean_true = 92.9746
    val control_mean_true = 86.0547
    val other_mean_true = 92.7916

    val (case_mean, control_mean, other_mean) = T2dmPhenotype.stat_calc(lab, rdd)

    val rela_err_case = ((case_mean - case_mean_true) / case_mean_true).abs
    val rela_err_other = ((other_mean - other_mean_true) / other_mean_true).abs
    val rela_err_control = ((control_mean - control_mean_true) / control_mean_true).abs

    rela_err_case should be <= 0.01
    rela_err_other should be <= 0.01
    rela_err_control should be <= 0.01

  }

}

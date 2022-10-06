package edu.cse6250.main

import edu.cse6250.clustering.Metrics
import edu.cse6250.features.FeatureConstruction
import edu.cse6250.helper.SessionCreator
import edu.cse6250.model.{ Diagnostic, LabResult, Medication }
import edu.cse6250.phenotyping.T2dmPhenotype

import java.text.{ NumberFormat, SimpleDateFormat }
import edu.cse6250.helper.{ CSVHelper, SessionCreator }
import edu.cse6250.model.{ Diagnostic, LabResult, Medication }
import org.apache.spark.mllib.clustering.{ GaussianMixture, KMeans, StreamingKMeans }
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.{ DenseMatrix, Matrices, Vector, Vectors }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.io.Source

/**
 * @author Hang Su <hangsu@gatech.edu>,
 * @author Yu Jing <yjing43@gatech.edu>,
 * @author Ming Liu <mliu302@gatech.edu>
 */
object Main {
  def main(args: Array[String]) {
    import org.apache.log4j.{ Level, Logger }

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val spark = SessionCreator.spark
    val sc = spark.sparkContext
    //  val sqlContext = spark.sqlContext

    /** initialize loading of data */
    val (medication, labResult, diagnostic) = loadRddRawData(spark)
    val (candidateMedication, candidateLab, candidateDiagnostic) = loadLocalRawData

    /** conduct phenotyping */
    val phenotypeLabel = T2dmPhenotype.transform(medication, labResult, diagnostic)

    /** feature construction with all features */
    val featureTuples = sc.union(
      FeatureConstruction.constructDiagnosticFeatureTuple(diagnostic),
      FeatureConstruction.constructLabFeatureTuple(labResult),
      FeatureConstruction.constructMedicationFeatureTuple(medication))

    // =========== USED FOR AUTO GRADING CLUSTERING GRADING =============
    // phenotypeLabel.map{ case(a,b) => s"$a\t$b" }.saveAsTextFile("data/phenotypeLabel")
    // featureTuples.map{ case((a,b),c) => s"$a\t$b\t$c" }.saveAsTextFile("data/featureTuples")
    // return
    // ==================================================================

    val rawFeatures = FeatureConstruction.construct(sc, featureTuples)

    val (kMeansPurity, gaussianMixturePurity, streamingPurity) = testClustering(phenotypeLabel, rawFeatures)
    println(f"[All feature] getPurity of kMeans is: $kMeansPurity%.5f")
    println(f"[All feature] getPurity of GMM is: $gaussianMixturePurity%.5f")
    println(f"[All feature] getPurity of StreamingKmeans is: $streamingPurity%.5f")

    /** feature construction with filtered features */
    val filteredFeatureTuples = sc.union(
      FeatureConstruction.constructDiagnosticFeatureTuple(diagnostic, candidateDiagnostic),
      FeatureConstruction.constructLabFeatureTuple(labResult, candidateLab),
      FeatureConstruction.constructMedicationFeatureTuple(medication, candidateMedication))

    val filteredRawFeatures = FeatureConstruction.construct(sc, filteredFeatureTuples)

    val (kMeansPurity2, gaussianMixturePurity2, streamingPurity2) = testClustering(phenotypeLabel, filteredRawFeatures)
    println(f"[Filtered feature] getPurity of kMeans is: $kMeansPurity2%.5f")
    println(f"[Filtered feature] getPurity of GMM is: $gaussianMixturePurity2%.5f")
    println(f"[Filtered feature] getPurity of StreamingKmeans is: $streamingPurity2%.5f")
  }

  def testClustering(phenotypeLabel: RDD[(String, Int)], rawFeatures: RDD[(String, Vector)]): (Double, Double, Double) = {
    import org.apache.spark.mllib.linalg.Matrix
    import org.apache.spark.mllib.linalg.distributed.RowMatrix

    println("phenotypeLabel: " + phenotypeLabel.count)
    /** scale features */
    val scaler = new StandardScaler(withMean = true, withStd = true).fit(rawFeatures.map(_._2))
    val features = rawFeatures.map({ case (patientID, featureVector) => (patientID, scaler.transform(Vectors.dense(featureVector.toArray))) })
    println("features: " + features.count)
    val rawFeatureVectors = features.map(_._2).cache()
    println("rawFeatureVectors: " + rawFeatureVectors.count)

    /** reduce dimension */
    val mat: RowMatrix = new RowMatrix(rawFeatureVectors)
    val pc: Matrix = mat.computePrincipalComponents(10) // Principal components are stored in a local dense matrix.
    val featureVectors = mat.multiply(pc).rows

    val densePc = Matrices.dense(pc.numRows, pc.numCols, pc.toArray).asInstanceOf[DenseMatrix]

    def transform(feature: Vector): Vector = {
      val scaled = scaler.transform(Vectors.dense(feature.toArray))
      Vectors.dense(Matrices.dense(1, scaled.size, scaled.toArray).multiply(densePc).toArray)
    }

    /**
     * TODO: K Means Clustering using spark mllib
     * Train a k means model using the variabe featureVectors as input
     * Set maxIterations =20 and seed as 6250L
     * Assign each feature vector to a cluster(predicted Class)
     * Obtain an RDD[(Int, Int)] of the form (cluster number, RealClass)
     * Find Purity using that RDD as an input to Metrics.getPurity
     * Remove the placeholder below after your implementation
     */

    val K = 3

    val kmeans = new KMeans().setK(K)
      .setMaxIterations(20)
      .setSeed(6250L)

    val Kmean_model = kmeans.run(featureVectors)
    val K_cluster_pred = Kmean_model.predict(featureVectors)

    val KMean_predict_label = features.map(_._1) //RDD[Patient_id]
      .zip(K_cluster_pred) //RDD[patient_id, predicted_class
      .join(phenotypeLabel) //rdd[patient_id, (predicted class, label)]
      .map(_._2) //rdd[(predicted class, label)]

    var case_table = KMean_predict_label.filter(x => x._2 == 1)
      .map(x => x._1)
      .countByValue()

    var control_table = KMean_predict_label.filter(x => x._2 == 2)
      .map(x => x._1)
      .countByValue()

    var other_table = KMean_predict_label.filter(x => x._2 == 3)
      .map(x => x._1)
      .countByValue()

    //    println("==================== K means ====================")
    //    println("--------------- k = " + K.toString + "--------------")
    //    println("case table")
    //    println(case_table.toString())
    //    println("control table")
    //    println(control_table.toString())
    //    println("other table")
    //    println(other_table.toString())

    val kMeansPurity = Metrics.getPurity(KMean_predict_label.map(x => (x._2, x._1)))

    /**
     * TODO: GMMM Clustering using spark mllib
     * Train a Gaussian Mixture model using the variabe featureVectors as input
     * Set maxIterations =20 and seed as 6250L
     * Assign each feature vector to a cluster(predicted Class)
     * Obtain an RDD[(Int, Int)] of the form (cluster number, RealClass)
     * Find Purity using that RDD as an input to Metrics.getPurity
     * Remove the placeholder below after your implementation
     */

    val GMM = new GaussianMixture().setK(K)
      .setMaxIterations(20)
      .setSeed(6250L)

    val GMM_model = GMM.run(featureVectors)
    val GMM_cluster_pred = GMM_model.predict(featureVectors)

    val GMM_predict_label = features.map(_._1) //RDD[Patient_id]
      .zip(GMM_cluster_pred) //RDD[patient_id, predicted_class
      .join(phenotypeLabel) //rdd[patient_id, (predicted class, label)]
      .map(_._2) //rdd[(predicted class, label)]

    case_table = GMM_predict_label.filter(x => x._2 == 1)
      .map(x => x._1)
      .countByValue()

    control_table = GMM_predict_label.filter(x => x._2 == 2)
      .map(x => x._1)
      .countByValue()

    other_table = GMM_predict_label.filter(x => x._2 == 3)
      .map(x => x._1)
      .countByValue()

    //    println("======================= GMM ======================")
//    println("--------------- k = " + K.toString + "--------------")
    //    println("case table")
    //    println(case_table.toString())
    //    println("control table")
    //    println(control_table.toString())
    //    println("other table")
    //    println(other_table.toString())

    val gaussianMixturePurity = Metrics.getPurity(GMM_predict_label.map(x => (x._2, x._1)))

    /**
     * TODO: StreamingKMeans Clustering using spark mllib
     * Train a StreamingKMeans model using the variabe featureVectors as input
     * Set the number of cluster K = 3, DecayFactor = 1.0, number of dimensions = 10, weight for each center = 0.5, seed as 6250L
     * In order to feed RDD[Vector] please use latestModel, see more info: https://spark.apache.org/docs/2.2.0/api/scala/index.html#org.apache.spark.mllib.clustering.StreamingKMeans
     * To run your model, set time unit as 'points'
     * Assign each feature vector to a cluster(predicted Class)
     * Obtain an RDD[(Int, Int)] of the form (cluster number, RealClass)
     * Find Purity using that RDD as an input to Metrics.getPurity
     * Remove the placeholder below after your implementation
     */

    val Streaming_Kmeans = new StreamingKMeans(k = K, decayFactor = 1.0, timeUnit = "points")
      .setRandomCenters(10, 0.5, 6250L)
      .latestModel()

    val Streaming_Kmeans_model = Streaming_Kmeans.update(data = featureVectors, decayFactor = 1.0, timeUnit = "points")

    val Streaming_Kmeans_cluster_pred = Streaming_Kmeans_model.predict(featureVectors)

    val Streaming_Kmeans_predict_label = features.map(_._1) //RDD[Patient_id]
      .zip(Streaming_Kmeans_cluster_pred) //RDD[patient_id, predicted_class
      .join(phenotypeLabel) //rdd[patient_id, (predicted class, label)]
      .map(_._2) //rdd[(predicted class, label)]

    case_table = Streaming_Kmeans_predict_label.filter(x => x._2 == 1)
      .map(x => x._1)
      .countByValue()

    control_table = Streaming_Kmeans_predict_label.filter(x => x._2 == 2)
      .map(x => x._1)
      .countByValue()

    other_table = Streaming_Kmeans_predict_label.filter(x => x._2 == 3)
      .map(x => x._1)
      .countByValue()

    val streamKmeansPurity = Metrics.getPurity(Streaming_Kmeans_predict_label.map(x => (x._2, x._1)))

    //    println("==================== Streaming KMeans ====================")
    //    println("--------------- k = " + K.toString + "--------------")
    //    println("case table")
    //    println(case_table.toString())
    //    println("control table")
    //    println(control_table.toString())
    //    println("other table")
    //    println(other_table.toString())
    (kMeansPurity, gaussianMixturePurity, streamKmeansPurity)
  }

  /**
   * load the sets of string for filtering of medication
   * lab result and diagnostics
   *
   * @return
   */
  def loadLocalRawData: (Set[String], Set[String], Set[String]) = {
    val candidateMedication = Source.fromFile("data/med_filter.txt").getLines().map(_.toLowerCase).toSet[String]
    val candidateLab = Source.fromFile("data/lab_filter.txt").getLines().map(_.toLowerCase).toSet[String]
    val candidateDiagnostic = Source.fromFile("data/icd9_filter.txt").getLines().map(_.toLowerCase).toSet[String]
    (candidateMedication, candidateLab, candidateDiagnostic)
  }

  def sqlDateParser(input: String, pattern: String = "yyyy-MM-dd'T'HH:mm:ssX"): java.sql.Date = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX")
    new java.sql.Date(dateFormat.parse(input).getTime)
  }

  def loadRddRawData(spark: SparkSession): (RDD[Medication], RDD[LabResult], RDD[Diagnostic]) = {
    /* the sql queries in spark required to import sparkSession.implicits._ */
    import spark.implicits._
    import edu.cse6250.helper.CSVHelper

    /* a helper function sqlDateParser may useful here */

    /**
     * load data using Spark SQL into three RDDs and return them
     * Hint:
     * You can utilize edu.cse6250.helper.CSVHelper
     * through your sparkSession.
     *
     * This guide may helps: https://bit.ly/2xnrVnA
     *
     * Notes:Refer to model/models.scala for the shape of Medication, LabResult, Diagnostic data type.
     * Be careful when you deal with String and numbers in String type.
     * Ignore lab results with missing (empty or NaN) values when these are read in.
     * For dates, use Date_Resulted for labResults and Order_Date for medication.
     *
     */

    /**
     * TODO: implement your own code here and remove
     * existing placeholder code below
     */

    val encounter_input = CSVHelper.loadCSVAsTable(
      spark,
      "data/encounter_INPUT.csv", "diag")

    val encounter_dx_input = CSVHelper.loadCSVAsTable(
      spark,
      "data/encounter_dx_INPUT.csv", "diag_dx")

    val diagnostic_df = spark.sql("SELECT diag.Member_ID, diag_dx.Code_ID,  " +
      "diag.Encounter_DateTime FROM diag JOIN diag_dx ON diag.Encounter_ID = diag_dx.Encounter_ID")

    val diagnostic: RDD[Diagnostic] = diagnostic_df.map((x: org.apache.spark.sql.Row) =>
      Diagnostic(x(0).toString, x(1).toString, sqlDateParser(x(2).toString))).rdd

    val lab_result = CSVHelper.loadCSVAsTable(
      spark,
      "data/lab_results_INPUT.csv", "lab")

    //case class LabResult(patientID: String, date: Date, testName: String, value: Double)
    val lab_imported = spark.sql("SELECT Member_ID, Date_Resulted, Result_Name, Numeric_Result FROM lab where Numeric_Result is not null")

    val lab_filtered = lab_imported.
      filter((x: org.apache.spark.sql.Row) => (x(3).toString.length != 0)
        && (!x(3).toString.isEmpty()) && (!"".equals(x(3).toString)))

    val labResult: RDD[LabResult] = lab_filtered.
      map((x: org.apache.spark.sql.Row) => LabResult(
        x(0).toString,
        sqlDateParser(x(1).toString), x(2).toString,
        (NumberFormat.getNumberInstance(java.util.Locale.US).parse(x(3).toString).doubleValue()))).rdd
    //NumberFormat.getNumberInstance(java.util.Locale.US).parse(x(3).toString).toDouble).rdd

    //case class Medication(patientID: String, date: Date, medicine: String)
    val med_table = CSVHelper.loadCSVAsTable(
      spark,
      "data/medication_orders_INPUT.csv", "med")

    val med_df = spark.sql("SELECT Member_ID, Order_Date, Drug_Name FROM med")

    val medication: RDD[Medication] = med_df.map((x: org.apache.spark.sql.Row) => Medication(
      x(0).toString,
      sqlDateParser(x(1).toString), x(2).toString.toLowerCase)).rdd

    (medication, labResult, diagnostic)
  }

}

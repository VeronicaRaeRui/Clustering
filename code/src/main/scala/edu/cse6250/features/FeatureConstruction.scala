package edu.cse6250.features

import edu.cse6250.model.{ Diagnostic, LabResult, Medication }
import edu.cse6250.model.{ Diagnostic, LabResult, Medication }
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.rdd.RDD

/**
 * @author Hang Su
 */
object FeatureConstruction {

  /**
   * ((patient-id, feature-name), feature-value)
   */
  type FeatureTuple = ((String, String), Double)

  /**
   * Aggregate feature tuples from diagnostic with COUNT aggregation,
   *
   * @param diagnostic RDD of diagnostic
   * @return RDD of feature tuples
   */
  def constructDiagnosticFeatureTuple(diagnostic: RDD[Diagnostic]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */

    val diag_count = diagnostic.map(x => ((x.patientID, x.code), 1.0))
      .reduceByKey(_ + _)

    diag_count;

  }

  /**
   * Aggregate feature tuples from medication with COUNT aggregation,
   *
   * @param medication RDD of medication
   * @return RDD of feature tuples
   */
  def constructMedicationFeatureTuple(medication: RDD[Medication]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    val med_count = medication.map(x => ((x.patientID, x.medicine), 1.0))
      .reduceByKey(_ + _)

    med_count;
  }

  /**
   * Aggregate feature tuples from lab result, using AVERAGE aggregation
   *
   * @param labResult RDD of lab result
   * @return RDD of feature tuples
   */
  def constructLabFeatureTuple(labResult: RDD[LabResult]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    val lab_count = labResult.map(x => ((x.patientID, x.testName), 1.0))
      .reduceByKey(_ + _);
    val lab_value_sum = labResult.map(x => ((x.patientID, x.testName), x.value))
      .reduceByKey(_ + _);
    val lab_average = lab_value_sum.join(lab_count)
      .map(x => (x._1, x._2._1 / x._2._2))

    lab_average;
  }

  /**
   * Aggregate feature tuple from diagnostics with COUNT aggregation, but use code that is
   * available in the given set only and drop all others.
   *
   * @param diagnostic   RDD of diagnostics
   * @param candiateCode set of candidate code, filter diagnostics based on this set
   * @return RDD of feature tuples
   */
  def constructDiagnosticFeatureTuple(diagnostic: RDD[Diagnostic], candiateCode: Set[String]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */

    val diag_candidate_count = diagnostic.filter(x => (candiateCode.contains(x.code)))
      .map(x => ((x.patientID, x.code), 1.0))
      .reduceByKey(_ + _)

    diag_candidate_count;
  }

  /**
   * Aggregate feature tuples from medication with COUNT aggregation, use medications from
   * given set only and drop all others.
   *
   * @param medication          RDD of diagnostics
   * @param candidateMedication set of candidate medication
   * @return RDD of feature tuples
   */
  def constructMedicationFeatureTuple(medication: RDD[Medication], candidateMedication: Set[String]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    val med_candidate_count = medication.filter(x => (candidateMedication.contains(x.medicine)))
      .map(x => ((x.patientID, x.medicine), 1.0))
      .reduceByKey(_ + _)

    med_candidate_count;
  }

  /**
   * Aggregate feature tuples from lab result with AVERAGE aggregation, use lab from
   * given set of lab test names only and drop all others.
   *
   * @param labResult    RDD of lab result
   * @param candidateLab set of candidate lab test name
   * @return RDD of feature tuples
   */
  def constructLabFeatureTuple(labResult: RDD[LabResult], candidateLab: Set[String]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    val lab_candidate = labResult.filter(x => (candidateLab.contains(x.testName)))
    val lab_count = lab_candidate.map(x => ((x.patientID, x.testName), 1.0))
      .reduceByKey(_ + _);
    val lab_value_sum = lab_candidate.map(x => ((x.patientID, x.testName), x.value))
      .reduceByKey(_ + _);
    val lab_average = lab_value_sum.join(lab_count)
      .map(x => (x._1, x._2._1 / x._2._2))

    lab_average;

  }

  /**
   * Given a feature tuples RDD, construct features in vector
   * format for each patient. feature name should be mapped
   * to some index and convert to sparse feature format.
   *
   * @param sc      SparkContext to run
   * @param feature RDD of input feature tuples
   *                patinet, code, value
   *                type FeatureTuple = ((String, String), Double)
   * @return
   */
  def construct(sc: SparkContext, feature: RDD[FeatureTuple]): RDD[(String, Vector)] = {

    /** save for later usage */
    feature.cache()

    /** create a feature name to id map */

    /** transform input feature */

    /**
     * Functions maybe helpful:
     * collect
     * groupByKey
     */

    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */

    val feature_index_map = feature
      .map(_._1._2)
      .distinct
      .zipWithIndex
      .collect
      .toMap

    val map_size = feature_index_map.size;

    val result = feature
      .map(x => (x._1._1, (x._1._2, x._2))) //(patient_id, (metric_name, value)
      .groupByKey() //(patient_id, iterable[(metric_name, value)]
      .map { x => //x._1: patient_id
        val mapped_features = x._2 //iterable[(metric_name, value)]
          .map(x => (feature_index_map(x._1).toInt, x._2)) //iterable[(metric_id, value)]
          .toList; //list[(metric_id, value)]
        (x._1, Vectors.sparse(map_size, mapped_features))
      }

    result
  }
}


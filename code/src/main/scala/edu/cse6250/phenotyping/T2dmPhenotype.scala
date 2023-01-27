package edu.cse6250.phenotyping

import edu.cse6250.model.{ Diagnostic, LabResult, Medication }
import org.apache.spark.rdd.RDD

/**
 * @author Hang Su <hangsu@gatech.edu>,
 * @author Sungtae An <stan84@gatech.edu>,
 */
object T2dmPhenotype {

  /** Hard code the criteria */
  val T1DM_DX = Set("250.01", "250.03", "250.11", "250.13", "250.21", "250.23", "250.31", "250.33", "250.41", "250.43",
    "250.51", "250.53", "250.61", "250.63", "250.71", "250.73", "250.81", "250.83", "250.91", "250.93")

  val T2DM_DX = Set("250.3", "250.32", "250.2", "250.22", "250.9", "250.92", "250.8", "250.82", "250.7", "250.72", "250.6",
    "250.62", "250.5", "250.52", "250.4", "250.42", "250.00", "250.02")

  val T1DM_MED = Set("lantus", "insulin glargine", "insulin aspart", "insulin detemir", "insulin lente", "insulin nph", "insulin reg", "insulin,ultralente")

  val T2DM_MED = Set("chlorpropamide", "diabinese", "diabanase", "diabinase", "glipizide", "glucotrol", "glucotrol xl",
    "glucatrol ", "glyburide", "micronase", "glynase", "diabetamide", "diabeta", "glimepiride", "amaryl",
    "repaglinide", "prandin", "nateglinide", "metformin", "rosiglitazone", "pioglitazone", "acarbose",
    "miglitol", "sitagliptin", "exenatide", "tolazamide", "acetohexamide", "troglitazone", "tolbutamide",
    "avandia", "actos", "actos", "glipizide")

  val DM_RELATED_DX = Set("790.21", "790.22", "790.2", "790.29", "648.81", "648.82", "648.83",
    "648.84", "648.0", "648.00",
    "648.01", "648.02", "648.03", "648.04", "791.5", "277.7", "V77.1", "256.4")

  val GLUCOSE_MEASURE = Set("hbA1c", "hemoglobin a1c", "fasting glucose", "fasting blood glucose", "fasting plasma glucose", "glucose", "glucose, serum")
  /**
   * Transform given data set to a RDD of patients and corresponding phenotype
   *
   * @param medication medication RDD
   * @param labResult  lab result RDD
   * @param diagnostic diagnostic code RDD
   * @return tuple in the format of (patient-ID, label). label = 1 if the patient is case, label = 2 if control, 3 otherwise
   */
  def transform(medication: RDD[Medication], labResult: RDD[LabResult], diagnostic: RDD[Diagnostic]): RDD[(String, Int)] = {
    /**
     * Remove the place holder and implement your code here.
     * Hard code the medication, lab, icd code etc. for phenotypes like example code below.
     * When testing your code, we expect your function to have no side effect,
     * i.e. do NOT read from file or write file
     *
     * You don't need to follow the example placeholder code below exactly, but do have the same return type.
     *
     * Hint: Consider case sensitivity when doing string comparisons.
     */

    val sc = medication.sparkContext

    /** Hard code the criteria */

    //gather all the patient ids from the three RDDS
    val patients = diagnostic.map(_.patientID).union(labResult.map(_.patientID)).union(medication.map(_.patientID)).distinct()

    //Case patients
    val T1_Diag_yes = diagnostic.filter(x => T1DM_DX.contains(x.code)).map(_.patientID).distinct()
    val T1_Diag_no = patients.subtract(T1_Diag_yes).distinct()

    val T2_Diag_yes = diagnostic.filter(x => T2DM_DX.contains(x.code)).map(_.patientID).distinct()
    //val T2_Diag_no = patients.subtract(T2_Diag_yes).distinct()

    val T1_Med_yes = medication.filter(x => T1DM_MED.contains(x.medicine.toLowerCase)).map(_.patientID).distinct()
    val T1_Med_no = patients.subtract(T1_Med_yes).distinct()

    val T2_Med_yes = medication.filter(x => T2DM_MED.contains(x.medicine.toLowerCase)).map(_.patientID).distinct()
    val T2_Med_no = patients.subtract(T2_Med_yes).distinct()

    val case_patient_1 = T1_Diag_no.intersection(T2_Diag_yes).intersection(T1_Med_no)
    val case_patient_2 = T1_Diag_no.intersection(T2_Diag_yes).intersection(T1_Med_yes).intersection(T2_Med_no)
    val case_patient_other = T1_Diag_no.intersection(T2_Diag_yes).intersection(T1_Med_yes).intersection(T2_Med_yes)

    val case_patient_med = medication.map(x => (x.patientID, x))
      .join(case_patient_other.map(x => (x, x)))
      .map(x => Medication(x._2._1.patientID, x._2._1.date, x._2._1.medicine))

    val case_patient_t1med_earlist = case_patient_med.filter(x => T1DM_MED.contains(x.medicine.toLowerCase)).map(x => (x.patientID, x.date.getTime())).reduceByKey(Math.min)
    val case_patient_t2med_earlist = case_patient_med.filter(x => T2DM_MED.contains(x.medicine.toLowerCase)).map(x => (x.patientID, x.date.getTime())).reduceByKey(Math.min)
    val case_patient_t2_before_t1 = case_patient_t1med_earlist.join(case_patient_t2med_earlist).filter(x => x._2._1 > x._2._2).map(_._1)

    val case_Patients = sc.union(case_patient_1, case_patient_2, case_patient_t2_before_t1).distinct()

    //Control patients
    //glucose lab result

    val glucose_lab_patient = labResult.filter(x => x.testName.toLowerCase.contains("glucose")).map(_.patientID).distinct()

    val glucose_patient_set = glucose_lab_patient.collect.toSet
    val all_lab_glucose_patient = labResult.filter(x =>
      glucose_patient_set.contains(x.patientID))

    val ab1 = all_lab_glucose_patient.filter(x => x.testName.toLowerCase.equals("hba1c") && x.value >= 6.0).map(x => x.patientID).distinct()
    val ab2 = all_lab_glucose_patient.filter(x => x.testName.toLowerCase.equals("hemoglobin a1c") && x.value >= 6.0).map(x => x.patientID).distinct()
    val ab3 = all_lab_glucose_patient.filter(x => x.testName.toLowerCase.equals("fasting glucose") && x.value >= 110).map(x => x.patientID).distinct()
    val ab4 = all_lab_glucose_patient.filter(x => x.testName.toLowerCase.equals("fasting blood glucose") && x.value >= 110).map(x => x.patientID).distinct()
    val ab5 = all_lab_glucose_patient.filter(x => x.testName.toLowerCase.equals("fasting plasma glucose") && x.value >= 110).map(x => x.patientID).distinct()
    val ab6 = all_lab_glucose_patient.filter(x => x.testName.toLowerCase.equals("glucose") && x.value > 110).map(x => x.patientID).distinct()
    val ab7 = all_lab_glucose_patient.filter(x => x.testName.toLowerCase.equals("glucose, serum") && x.value > 110).map(x => x.patientID).distinct()

    val abnormal_lab = sc.union(ab1, ab2, ab3, ab4, ab5, ab6, ab7)

    val normal_lab = glucose_lab_patient.subtract(abnormal_lab).distinct()

    val diabetes_mellitus_1 = diagnostic.filter(x => DM_RELATED_DX.contains(x.code)).map(x => x.patientID).distinct()
    val diabetes_mellitus_2 = diagnostic.filter(x => x.code.startsWith("250.")).map(x => x.patientID).distinct()

    val diabetes_mellitus_abnormal = diabetes_mellitus_1.union(diabetes_mellitus_2)

    val diabetes_mellitus_normal = patients.subtract(diabetes_mellitus_abnormal).distinct()

    val control_Patients = normal_lab.intersection(diabetes_mellitus_normal)

    //Other patients
    val other_Patients = patients.subtract(case_Patients).subtract(control_Patients).distinct()

    /** Find CASE Patients */
    val casePatient = case_Patients.map((_, 1))

    /** Find CONTROL Patients */
    val controlPatient = control_Patients.map((_, 2))

    /** Find OTHER Patients */
    val otherPatient = other_Patients.map((_, 3))

    /** Once you find patients for each group, make them as a single RDD[(String, Int)] */
    val phenotypeLabel = sc.union(casePatient, controlPatient, otherPatient)

    /** Return */
    phenotypeLabel

  }

  /**
   * calculate specific stats given phenotype labels and corresponding data set rdd
   * @param labResult  lab result RDD
   * @param phenotypeLabel phenotype label return from T2dmPhenotype.transfrom
   * @return tuple in the format of (case_mean, control_mean, other_mean).
   *         case_mean = mean Glucose lab test results of case group
   *         control_mean = mean Glucose lab test results of control group
   *         other_mean = mean Glucose lab test results of unknown group
   *         Attention: order of the three stats in the returned tuple matters!
   */
  def stat_calc(labResult: RDD[LabResult], phenotypeLabel: RDD[(String, Int)]): (Double, Double, Double) = {
    /**
     * you need to hardcode the feature name and the type of stat:
     * e.g. calculate "mean" of "Glucose" lab test result of each group: case, control, unknown
     *
     * The feature name should be "Glucose" exactly with considering case sensitivity.
     * i.e. "Glucose" and "glucose" are counted, but features like "fasting glucose" should not be counted.
     *
     * Hint: rdd dataset can directly call statistic method. Details can be found on course website.
     *
     */

    var case_mean = 1.0
    var control_mean = 1.0
    var other_mean = 1.0

    val case_id = phenotypeLabel.filter { case (x, t) => t == 1 }
      .map { case (x, t) => x }
      .collect.toSet
    val control_id = phenotypeLabel.filter { case (x, t) => t == 2 }
      .map { case (x, t) => x }
      .collect.toSet
    val other_id = phenotypeLabel.filter { case (x, t) => t == 3 }
      .map { case (x, t) => x }
      .collect.toSet

    val glucose_lab = labResult.filter(x => x.testName.toLowerCase.equals("glucose"))

    val case_glucose_lab = glucose_lab.filter(x => case_id.contains(x.patientID))
    val control_glucose_lab = glucose_lab.filter(x => control_id.contains(x.patientID))
    val other_glucose_lab = glucose_lab.filter(x => other_id.contains(x.patientID))

    case_mean = case_glucose_lab.map(x => x.value).mean()
    control_mean = control_glucose_lab.map(x => x.value).mean()
    other_mean = other_glucose_lab.map(x => x.value).mean()

    (case_mean, control_mean, other_mean)
  }
}
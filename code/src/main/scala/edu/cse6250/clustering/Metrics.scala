/**
 * @author Hang Su <hangsu@gatech.edu>.
 */

package edu.cse6250.clustering

import org.apache.spark.rdd.RDD

object Metrics {
  /**
   * Given input RDD with tuples of assigned cluster id by clustering,
   * and corresponding real class. Calculate the getPurity of clustering.
   * Purity is defined as
   * \fract{1}{N}\sum_K max_j |w_k \cap c_j|
   * where N is the number of samples, K is number of clusters and j
   * is index of class. w_k denotes the set of samples in k-th cluster
   * and c_j denotes set of samples of class j.
   *
   * @param clusterAssignmentAndLabel RDD in the tuple format
   *                                  (assigned_cluster_id, class)
   * @return
   */
  def getPurity(clusterAssignmentAndLabel: RDD[(Int, Int)]): Double = {
    /**
     * TODO: Remove the placeholder and implement your code here
     */
    val sample_size = clusterAssignmentAndLabel.collect().size.toDouble

    val sum_of_cluster = clusterAssignmentAndLabel
      .map(x => (x._1, (x._1, x._2))) //RDD(cluster_id, (cluster_id, class))
      .groupByKey() //Map(cluster_id,  iterable[(cluster_id, class)])
      .map {
        x =>
          (x._2 //iterable[(cluster_id, class)]
            .groupBy(x => x._2) // group by class, Map(class,  iterable[(cluster_id, class)])
            .map(y => y._2 //iterable[(cluster_id, class)]
              .map(a => 1.0)
              .reduce(_ + _)) //iterable[count_of_class]
            .reduce((x, y) => x max y) //max of class count
          )
      }
      .reduce(_ + _)

    sum_of_cluster / sample_size

  }
}

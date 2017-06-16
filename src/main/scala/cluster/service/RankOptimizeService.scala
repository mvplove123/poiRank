package cluster.service

import org.apache.spark.rdd.RDD

/**
  * Created by admin on 2017/6/16.
  */
trait RankOptimizeService extends Serializable {


  def rankOptimize(rankRdd: RDD[String], targetOptimizeRdd: RDD[String]): RDD[String]


}

package cluster.service.impl

import cluster.service.RankOptimizeService
import org.apache.spark.rdd.RDD

/**
  * Created by admin on 2017/6/16.
  */
class StructureRankOptimizeService extends RankOptimizeService{


  /**
    * 结构化rank优化
    * @param rankRdd
    * @param structureRankRdd
    * @return
    */
  def rankOptimize(rankRdd: RDD[String], structureRankRdd: RDD[String]): RDD[String] = {


    val rank: RDD[(String, String)] = rankRdd.map(x => (x.split("\t")(1), x))
    val structureRank: RDD[(String, String)] = structureRankRdd.map(x=>x.split("\t")).map(x=>(x(0),x(5)))

    val optimizeRankResult: RDD[String] = rank.leftOuterJoin(structureRank).map(x => {

      val dataId = x._1
      val rankInfo = x._2._1
      val structureOptimizeRank = x._2._2

      var newLine = ""
      if (structureOptimizeRank.isEmpty) {
        newLine = rankInfo
      } else {
        val fields = rankInfo.split("\t")
        val fieldsLength = fields.length
        newLine = fields.slice(0, fieldsLength - 5).mkString("\t") + "\t" + structureOptimizeRank.get + "\t" +
          fields.slice(fieldsLength - 4, fieldsLength).mkString("\t")
      }
      newLine
    })

    return optimizeRankResult
  }



}

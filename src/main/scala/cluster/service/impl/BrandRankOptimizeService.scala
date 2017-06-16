package cluster.service.impl

import cluster.service.RankOptimizeService
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD

/**
  * Created by admin on 2017/6/16.
  */
class BrandRankOptimizeService extends RankOptimizeService {

  def rankOptimize(rankRdd: RDD[String], brandRdd: RDD[String]): RDD[String] = {
    val brand = brandRdd.map(x => x.split("\t")).map(x => (x(0), "brand_" + x(1)))

    val rank = rankRdd.map(x => (x.split("\t")(5), x))

    val noBrandRank = rank.filter(x => StringUtils.isBlank(x._1)).map(x => x._2)
    val brandRank: RDD[(String, String)] = rank.filter(x => StringUtils.isNotBlank(x._1))


    val newBrandRank: RDD[String] = brandRank.union(brand).combineByKey(
      (v: String) => List(v),
      (c: List[String], v: String) => v :: c,
      (c1: List[String], c2: List[String]) => c1 ++ c2
    ).flatMap(x => optimizeBrandRank(x._2))

    val optimizeRankResult = noBrandRank.union(newBrandRank)
    return optimizeRankResult
  }


  /**
    * 优化品牌rank
    *
    * @param fields
    * @return
    */
  def optimizeBrandRank(fields: List[String]): List[String] = {


    var rankFields = List[String]()

    var brandFields = 0

    fields.foreach(field => {
      if (field.startsWith("brand_")) {
        brandFields = Math.ceil(field.substring(6).toDouble).toInt
      } else {
        rankFields = rankFields.::(field)
      }
    })

    return rankFields.map(x => optimize(x, brandFields))
  }

  def optimize(line: String, brandRank: Int): String = {

    val rankLevel = 1
    val fields = line.split("\t")

    val fieldsLength = fields.length
    val currentRank = fields(fieldsLength - 5).toInt
    var optimizeRankValue = currentRank

    if (brandRank == 0) {
      return line
    }


    val value = Math.abs(currentRank - brandRank)
    if (value > rankLevel) {
      if (currentRank > brandRank) optimizeRankValue = brandRank + rankLevel else optimizeRankValue = brandRank - rankLevel
    }
    val newLine = fields.slice(0, fieldsLength - 5).mkString("\t") + "\t" + optimizeRankValue + "\t" + fields.slice(fieldsLength - 4, fieldsLength).mkString("\t")
    return newLine
  }


}

package cluster.service

import cluster.model.{CellCut, Poi}
import cluster.utils.{Convertor_LL_Mer, Util}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
  * Created by admin on 2016/9/16.
  */
object MatchCountService {

  var blackSubcate = List("地铁站出入口", "公交线路")

  def getMatchCountRDD(sc: SparkContext, poiRdd: RDD[String]): RDD[String] = {

    val cellCut = new CellCut(2000, 6000)
//      .filter(x=>x(3).equals("北京市"))
    val currentIDsInfo: RDD[(String, List[String])] = poiRdd.map(x => x.split("\t")).filter(x=>StringUtils
      .isNoneBlank(x(2)))
      .flatMap(
      x => {
        val name = x(0)
        val dataId = x(1)
        val point = x(2)
        val city = x(3)
        val category = x(4)
        val subCategory = x(5)
        val alias = x(12)
        val pointXY = point.split(",")

        val currentIDs: List[String] = cellCut.getCrossCell(pointXY(0), pointXY(1))
        currentIDs.map(x => (x, Array(dataId, name, city, point, category, subCategory, alias).mkString("\t")))
      }
    ).combineByKey(
      (v: String) => List(v),
      (c: List[String], v: String) => v :: c,
      (c1: List[String], c2: List[String]) => c1 ++ c2
    )
//      .persist(StorageLevel.MEMORY_AND_DISK)
    val ss: RDD[String] = currentIDsInfo.flatMap(
      x => {

        val key = x._1
        //有效value
        val validPoistr: List[String] = x._2.filter(x => {
          val poi = x.split("\t")
          val point = poi(3).split(",")
          val currentId = cellCut.getCurrentCell(point(0), point(1))
          key.equals(currentId)
        })

        val allPoistr: List[String] = x._2
        val validPois: List[Poi] = validPoistr.map(x => x.split("\t")).map(x => {

          val poi = new Poi
          poi.dataId = x(0)
          poi.name = x(1)
          poi.city = x(2)
          poi.point = x(3)
          val pointXY = poi.point.split(",")
          poi.lat=pointXY(0)
          poi.lng=pointXY(1)
          poi.category = x(4)
          poi.subCategory = x(5)
          poi.alias = x(6)
          poi
        })

        val allPois: List[Poi] = allPoistr.map(x => x.split("\t")).map(x => {
          val poi = new Poi
          poi.dataId = x(0)
          poi.name = x(1)
          poi.city = x(2)
          poi.point = x(3)
          val pointXY = poi.point.split(",")
          poi.lat=pointXY(0)
          poi.lng=pointXY(1)
          poi.category = x(4)
          poi.subCategory = x(5)
          poi.alias = x(6)
          poi
        })

        calculate(validPois, allPois)
      }
    )
    return ss

  }

  def calculate(validPois: List[Poi], allPois: List[Poi]): List[String] = {
    var poisResponse = List[String]()

    if(validPois.size>0){
      validPois.foreach(validPoi => {
        val str: StringBuilder = new StringBuilder
        var count: Int = 0
        allPois.foreach(poi => {
          if (validPoi.city.equals(poi.city) && (!validPoi.name.equals(poi.name))) {

            val d: Double = Convertor_LL_Mer.DistanceMer(validPoi.lat.toDouble, validPoi.lng.toDouble, poi.lat.toDouble, poi.lng.toDouble)

            if (d <= 5000.0) {
              val matches1: Boolean = Util.strMatch(validPoi.name, poi.name)
              //current 正式名，target正式名匹配
              if (matches1 && !poi.name.endsWith(validPoi.name)) {
                str.append(poi.name)
                str.append(",")
                count += 1
              }//current 正式名，target别名
              else if (StringUtils.isNotBlank(poi.alias)) {
                val matches4: Boolean = Util.strMatch(validPoi.name, poi.alias)
                if (matches4 && !poi.alias.endsWith(validPoi.name)) {
                  str.append(poi.name)
                  str.append(",")
                  count += 1
                }
              }//current 别名，target正式名
              else if (StringUtils.isNotBlank(validPoi.alias) && !blackSubcate.contains(validPoi.subCategory)) {
                val matches2: Boolean = Util.strMatch(validPoi.alias, poi.name)
                if (matches2 && !poi.name.endsWith(validPoi.alias)) {
                  str.append(poi.name)
                  str.append(",")
                  count += 1
                }
              }//current 别名，target 别名
              else if (StringUtils.isNotBlank(validPoi.alias) && !blackSubcate.contains(validPoi.subCategory) && StringUtils
                .isNotBlank(poi.alias)) {
                val matches3: Boolean = Util.strMatch(validPoi.alias, poi.alias)
                if (matches3 && !poi.alias.endsWith(validPoi.alias)) {
                  str.append(poi.name)
                  str.append(",")
                  count += 1
                }
              }
            }
          }

        })
        if(count>0){
          val poiResponse: String = Array(validPoi.name, validPoi.dataId, validPoi.city, validPoi.point, validPoi.category,
            validPoi.subCategory, count.toString, str.toString()).mkString("\t")
          poisResponse ::= poiResponse
        }
      })
    }
    poisResponse
  }


}

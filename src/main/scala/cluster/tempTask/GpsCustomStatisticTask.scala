package cluster.tempTask

import breeze.numerics.{log10, log}
import cluster.model.{FilterPoiCondition, CellCut}
import cluster.service.{PoiService, GpsPopularityService, PoiBoundService}
import cluster.utils.{GBKFileOutputFormat, Constants, WordUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD

/**
  * Created by admin on 2017/9/7.
  */
object GpsCustomStatisticTask extends PoiBoundService {

  val filter_category = Array("地名", "交通出行", "旅游景点", "政府机关", "学校科研", "体育场馆", "公司企业", "房地产", "场馆会所")

  val filter_subCategory = Array("一般医院", "停车场", "加油站", "加气站", "大型商场", "大型超市", "'4-5星级", "其它星级", "大门", "楼号", "门址")


  val poiSign = "RANK"
  val fpoiSign = "FilterRank"

  def main(args: Array[String]) {

    val conf = new SparkConf()
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[CellCut]))
    val sc: SparkContext = new SparkContext(conf)


    if (args.length < 2) {
      println(args.mkString("\t"))
      println("no outputpath!")
      sc.stop()
    }

    val rankOutputPath = args(1)




    val path = new Path(rankOutputPath+Constants.filterRankPath)
    WordUtils.delDir(sc, path, true)

    val filterPoiRdd: RDD[String] = PoiService.getPoiString(WordUtils.convert(sc, rankOutputPath+Constants.filterPoiInputPath, Constants
      .gbkEncoding)).cache()

    val poiRdd: RDD[String] = WordUtils.convert(sc, rankOutputPath+Constants.multiOptimizeRankOutputPath, Constants
      .gbkEncoding)

    gpsPopularity(sc, poiRdd, rankOutputPath)

    val structureRdd: RDD[String] = WordUtils.convert(sc, rankOutputPath+Constants.structureOutPutPath, Constants
      .gbkEncoding)
    val hitcountCountRdd: RDD[String] = WordUtils.convert(sc, Constants.hitCountInputPath, Constants.gbkEncoding)
    val hotcountRdd: RDD[String] = WordUtils.convert(sc, rankOutputPath+Constants.poiboundPath, Constants.gbkEncoding)

    val similarCountRdd: RDD[String] = WordUtils.convert(sc, rankOutputPath+Constants.similarQueryCountPath,
      Constants.gbkEncoding)
    val vrHitCountCountRdd: RDD[String] = WordUtils.convert(sc, Constants.vrHitCountPath, Constants.gbkEncoding)
    val vrViewCountRdd: RDD[String] = WordUtils.convert(sc, Constants.vrViewCountPath, Constants.gbkEncoding)

    val sogouViewCountRdd: RDD[String] = WordUtils.convert(sc, Constants.sogouViewCount, Constants.gbkEncoding)


    val result = combine(sc, poiRdd, hotcountRdd, hitcountCountRdd, structureRdd, similarCountRdd, vrViewCountRdd,
      vrHitCountCountRdd, sogouViewCountRdd, filterPoiRdd)
      .filter(x => (!x._1.equals("None"))).cache()

    result.map(x => (x._2.split("\t")(63), 1)).reduceByKey(_ + _).collect().foreach(
      x => {
        println(x._1 + " count:" + x._2.toString)
      }
    )

    result.saveAsNewAPIHadoopFile(rankOutputPath+Constants.filterRankPath, classOf[Text],
      classOf[IntWritable],
      classOf[GBKFileOutputFormat[Text, IntWritable]])
    sc.stop()


  }

  def gpsPopularity(sc: SparkContext, poiRdd: RDD[String],rankOutputPath:String): Unit = {
    val path1 = new Path(rankOutputPath+Constants.poiboundPath)
    WordUtils.delDir(sc, path1, true)
    val gpsRdd: RDD[String] = WordUtils.convert(sc, rankOutputPath+Constants.gpsCountInputPath, Constants.gbkEncoding)

    val poiCustomBound = poiRdd.map(x => getLocalBound(x)).filter(_ != null).map(_._2)
    val gpsPopularityService = new GpsPopularityService
    val gpsPopularity: RDD[(String, Int)] = gpsPopularityService.gpsCombine(sc, poiCustomBound, gpsRdd)

    gpsPopularity.saveAsNewAPIHadoopFile(rankOutputPath+Constants.poiboundPath, classOf[Text], classOf[IntWritable],
      classOf[GBKFileOutputFormat[Text, IntWritable]])
  }


  def combine(sc: SparkContext, poiRdd: RDD[String], hotcountRdd: RDD[String], hitcountCountRdd: RDD[String],
              structureRdd: RDD[String], similarCountRdd: RDD[String], vrViewCountRdd: RDD[String], vrHitCountRdd:
              RDD[String], sogouViewCountRdd: RDD[String], filterPoiRdd: RDD[String]):
  RDD[(String, String)] = {

    try {

      val poiRddMap = poiRdd.map(x => (x.split("\t")(1), "poi_" + x))
      val hotcountRddMap = hotcountRdd.map(x => x.split("\t")).map(x => (x(0), "hotCount_" + x(1)))
      val hitcountRddMap = hitcountCountRdd.map(x => x.split("\t")).map(x => (x(0), "searchCount_" + x(1)))

      val similarCountRddMap = similarCountRdd.map(x => x.split("\t")).map(x => ("1_" + x(0), "similarCount_" + x.slice
      (1, 11).mkString("\t")))
      val vrViewCountRddMap = vrViewCountRdd.map(x => x.split("\t")).map(x => (x(0), "vrViewCount_" + x(1)))
      val vrHitCountRddMap = vrHitCountRdd.map(x => x.split("\t")).map(x => (x(0), "vrHitCount_" + x(1)))

      val sogouViewCountRddMap = sogouViewCountRdd.map(x => x.split("\t")).map(x => (x(0), "sogouViewCount_" + x(1)))

      val parentRddMap = structureRdd.map(x => (x.split("\t"))).map(x => (x(0), "parentSign_1"))
      //部分主节点无子节点
      val childRddMap = structureRdd.map(x => (x.split("\t"))).filter(x => x.length == 12).flatMap(x => x(11).split(",").map(y => (y, "childSign_1")))

      //裁剪数据
      val filterPoiRddMap = filterPoiRdd.map(x => (x.split("\t")(1), "filterPoi_" + x))


      val combineInfo: RDD[(String, List[String])] = sc.union(poiRddMap, hotcountRddMap, hitcountRddMap, parentRddMap,
        childRddMap, similarCountRddMap, vrViewCountRddMap, vrHitCountRddMap, sogouViewCountRddMap, filterPoiRddMap)
        .combineByKey(
          (v: String) => List(v),
          (c: List[String], v: String) => v :: c,
          (c1: List[String], c2: List[String]) => c1 ++ c2
        )

      val mergeResult: RDD[(String, String)] = combineInfo.map(x => {


        //相似度频次文件:Query数、总频次、(总频次取对数)、搜狗Query数、搜狗频次、搜狗频次取对数、VRQuery数、VR频次、VR频次取对数、有无结果（1，0）10个字段
        var similarValue: String = Array("0", "0", "0", "0", "0", "0", "0", "0", "0").mkString("\t")
        //poi数据
        var poiValue: String = ""
        //热点人气数据
        var hotCountValue: String = "0"
        //点击量
        var hitCountValue: String = "0"

        //vrHitcount
        var vrHitCountValue: String = "0"

        //vrViewCount
        var vrViewCountValue: String = "0"

        var sogouViewCountValue: String = "0"
        var parentSign = "0"
        var childSign = "0"

        var filterPoiValue = ""

        val dataId = x._1
        x._2.foreach(y => {

          if (y.startsWith("similarCount_")) similarValue = y.substring(13)
          else if (y.startsWith("vrViewCount_")) vrViewCountValue = y.substring(12)
          else if (y.startsWith("vrHitCount_")) vrHitCountValue = y.substring(11)
          else if (y.startsWith("searchCount_")) hitCountValue = y.substring(12)
          else if (y.startsWith("hotCount_")) hotCountValue = y.substring(9)
          else if (y.startsWith("parentSign_")) parentSign = y.substring(11)
          else if (y.startsWith("childSign_")) childSign = y.substring(10)
          else if (y.startsWith("sogouViewCount_")) sogouViewCountValue = y.substring(15)
          else if (y.startsWith("poi_")) poiValue = y.substring(4)
          else if (y.startsWith("filterPoi_")) filterPoiValue = y.substring(10)

        }
        )
        var hotCountLog = 0
        var hitCountLog = 0
        var vrHitCountLog = 0
        var vrViewCountLog = 0
        var sogouViewCountLog = 0
        if (hotCountValue.toInt > 0) {
          hotCountLog = Math.round(log(hotCountValue.toInt)).toInt
        }
        if (hitCountValue.toInt > 0) {
          hitCountLog = Math.round(log(hitCountValue.toInt)).toInt
        }

        if (vrHitCountValue.toInt > 0) {
          vrHitCountLog = Math.round(log(vrHitCountValue.toInt)).toInt
        }

        if (vrViewCountValue.toInt > 0) {
          vrViewCountLog = Math.round(log(vrViewCountValue.toInt)).toInt
        }

        if (sogouViewCountValue.toInt > 0) {
          sogouViewCountLog = Math.round(log(sogouViewCountValue.toInt)).toInt
        }

        val similarValueFields = similarValue.split("\t")

        val queryNum = similarValueFields(0).toDouble.toInt
        val queryCount = similarValueFields(1).toDouble.toInt
        val sogouQueryNum = similarValueFields(2).toDouble.toInt
        val sogouQueryCount = similarValueFields(3).toDouble.toInt
        val VRQueryNum = similarValueFields(5).toDouble.toInt
        val VRQueryCount = similarValueFields(6).toDouble.toInt
        val querySign = similarValueFields(8).toDouble.toInt

        var queryCountLog = 0
        if (queryCount > 0) {
          queryCountLog = Math.round(log(queryCount)).toInt
        }

        var sogouqueryCountLog = 0
        if (sogouQueryCount > 0) {
          sogouqueryCountLog = Math.round(log(sogouQueryCount)).toInt
        }

        var vrqueryCountLog = 0
        if (VRQueryCount > 0) {
          vrqueryCountLog = Math.round(log(VRQueryCount)).toInt
        }


        val newsimilarValue = Array(queryNum, queryCount, queryCountLog, sogouQueryNum, sogouQueryCount,
          sogouqueryCountLog, VRQueryNum, VRQueryCount, vrqueryCountLog, querySign).mkString("\t") //10个字段

        //共22个字段
        val filterRankInfo = Array(hotCountValue, hotCountLog, hitCountValue, hitCountLog,
          queryNum, queryCount, queryCountLog, sogouQueryNum, sogouQueryCount,
          sogouqueryCountLog, VRQueryNum, VRQueryCount, vrqueryCountLog, querySign,
          vrHitCountValue, vrHitCountLog, parentSign, childSign, vrViewCountValue, vrViewCountLog, sogouViewCountValue,
          sogouViewCountLog).mkString("\t")

        //poi rank数据截取所需字段
        if (StringUtils.isBlank(poiValue)) {
          ("None", "")
        } else {

          if (StringUtils.isNotBlank(filterPoiValue)) {

            val poifileds = poiValue.split("\t")

            val category = poifileds(3)
            val subCategory = poifileds(4)
            val rank = poifileds(36).toInt


            val filterPoiCondition = new FilterPoiCondition(
              sogouquerycount = sogouQueryCount,
              vrquerycount = VRQueryCount,
              vrhitcount = vrHitCountValue.toInt,
              hitCount = hitCountValue.toInt,
              viewcount = vrViewCountValue.toInt,
              structparentsign = parentSign.toInt,
              structchildsign = childSign.toInt,
              sogouviewcount = sogouViewCountValue.toInt,
              category = category,
              subCategory = subCategory,
              rank_level = rank
            )

            filter_sign(filterPoiCondition)

            (dataId, Array(filterRankInfo, poiValue, filterPoiCondition.signLevel, fpoiSign).mkString("\t"))
            //包括dataid共25个字段+rank字段41个字段包含dataid
          }else{
            (dataId, Array(filterRankInfo, poiValue, 0, poiSign).mkString("\t"))
          }
        }
      }).filter(x => StringUtils.isNotBlank(x._1))

      return mergeResult
    } catch {
      case ex: Exception => {
        println(ex)
      }
    }
    return null


  }

  /**
    * 裁剪rank标识设置
    * @param filterPoiCondition
    */
  def filter_sign(filterPoiCondition: FilterPoiCondition): Unit = {

    if (filterPoiCondition.vrhitcount == 0 && filterPoiCondition.viewcount == 0 && filterPoiCondition.hitCount == 0 && filterPoiCondition
      .structparentsign == 0 && filterPoiCondition.structchildsign == 0 &&
      filterPoiCondition.rank_level <= 3 && !filter_category.contains(filterPoiCondition.category) &&
      !filter_subCategory.contains(filterPoiCondition.subCategory)) {

      if (filterPoiCondition.sogouquerycount < 50 && filterPoiCondition.vrquerycount < 100 && filterPoiCondition.sogouviewcount <
        10) {

        filterPoiCondition.signLevel = 3

      } else if (filterPoiCondition.sogouquerycount < 50 && filterPoiCondition.vrquerycount < 100 &&
        (filterPoiCondition.sogouviewcount >= 10 && filterPoiCondition.sogouviewcount < 20)) {

        filterPoiCondition.signLevel = 2

      } else if ((filterPoiCondition.sogouquerycount >= 50 && filterPoiCondition.sogouquerycount < 100) &&
        (filterPoiCondition.vrquerycount >= 100 && filterPoiCondition.vrquerycount < 150) &&
        (filterPoiCondition.sogouviewcount >= 20 && filterPoiCondition.sogouviewcount < 100)) {
        filterPoiCondition.signLevel = 1

      } else {
        filterPoiCondition.signLevel = 0
      }
    }
    else {
      filterPoiCondition.signLevel = 0
    }
  }


  def getLocalBound(poi: String): (String, String) = {
    try {
      val poiArray = poi.split('\t')

      val name: String = poiArray(0)
      val dataId: String = poiArray(1)
      val category: String = poiArray(3)
      val subCategory: String = poiArray(4)
      val point: String = poiArray(18)

      if (StringUtils.isNoneBlank(point)) {
        val xy = point.split(",")


        var boundXY: String = ""

        val cateBoundKey: String = category + "-" + subCategory

        val categoryBound = Constants.getCategoryBound

        boundXY = getCrossCellXY(xy(0), xy(1), 200)

        if (StringUtils.isNoneBlank(boundXY)) {
          val value = Array(dataId, name, boundXY).mkString("\t")
          return (dataId, value)
        }

      }
      return null
    } catch {
      case ex: Exception => {
        println(ex)
        return null
      }
    }


  }

  override def getPoiBound(args: RDD[String]*): RDD[(String, String)] = {
    return null
  }


}

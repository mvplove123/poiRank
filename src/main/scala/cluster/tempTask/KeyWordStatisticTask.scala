package cluster.tempTask

import cluster.model.{FeatureValue, Threshold, CellCut}
import cluster.utils.{GBKFileOutputFormat, RDDMultipleTextOutputFormat, Constants, WordUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkContext, SparkConf}

import scala.collection.{Map, mutable}

/**
  * Created by admin on 2017/10/18.
  */
object KeyWordStatisticTask {


  val sign = List("SEARCH","AFB","GAODE","DG","NAV")

  def main(args: Array[String]) {

    val conf = new SparkConf()
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[CellCut]))
//    conf.setAppName("catkey")
    val sc: SparkContext = new SparkContext(conf)
    val keywordstatisticpath = "taoyongbo/output/keywordstatistic"
    val catesubkeyPath = "D:\\structure\\spark\\catesubkey"
    val path = new Path(keywordstatisticpath)
    WordUtils.delDir(sc, path, true)



    val featureThreshold= WordUtils.convert(sc, "taoyongbo/input/featureThreshold/poi-threshold.txt", Constants.gbkEncoding)
    val catesukey = getValue(featureThreshold,catesubkeyPath)
    val poiRdd: RDD[String] = WordUtils.convert(sc,Constants.multiOptimizeRankOutputPath, Constants.gbkEncoding).cache()
    val result: RDD[(String, Integer)] = poiRdd.map(x => x.split("\t")).flatMap(x=>getkeyWordFeatureValue(x(3),x(19),
      catesukey))
      .reduceByKey(_+_).sortBy(x=>x._2,false)

    result.coalesce(1).saveAsNewAPIHadoopFile(keywordstatisticpath, classOf[Text],
          classOf[IntWritable],
          classOf[GBKFileOutputFormat[Text, IntWritable]])

  }


  def getValue(featureThreshold: RDD[String],catesubkeyPath:String):  Array[String] = {


    val result11: Array[String] = featureThreshold.map(x=>x.split("\t")).filter(x=>StringUtils.isNotBlank(x(3)))
      .flatMap(x => x(3).split("\\|")).map(x => x
      .split(":")
    (0)).flatMap(x => x.split("-")).distinct().collect()


//    val catesubkey = featureThreshold.map(x=>x.split("\t")).filter(x=>StringUtils.isNotBlank(x(3))).flatMap(x=>{
//
//      val category = x(0)
//      val subCategory = x(1)
//      val keyWord = x(3)
//
//      keyWord.split("\\|").map(x => x.split(":")(0)).flatMap(x => x.split("-")).map(x=> (category+"\t"+subCategory,x))
//    }).distinct()
//
//
//    catesubkey.coalesce(1).saveAsNewAPIHadoopFile(catesubkeyPath, classOf[Text],
//      classOf[IntWritable],
//      classOf[GBKFileOutputFormat[Text, IntWritable]])


    return result11

  }


  private def getkeyWordFeatureValue(category: String, keyword: String, featureThreshold: Array[String]): List[(String, Integer)]  = {
    var result: String = "0"
    if (StringUtils.isBlank(keyword)) {
      return null
    }

    var keyWordList: List[String] = List()
    var keytongji : List[(String, Integer)] = List()
    val strs: Array[String] = keyword.split(",").filter(x=>{
      if(x.contains("SEARCH") || x.contains("AFB")|| x.contains("GAODE")||x.contains("DG")||x.contains("NAV")){
        true
      }else{
        false
      }
    })


    for (str <- strs) {
      if (StringUtils.isNotBlank(str)) {
        var begin: Int = 0
        var end: Int = str.length
        if (str.contains(":")) {
          begin = str.indexOf(":")
        }
        if (str.contains("$$") && str.endsWith("$$")) {
          end = str.lastIndexOf("$$")
        }
        if (begin + 1 <= end) {
          val newStr: String = str.substring(begin + 1, end)
          val b: Array[String] = newStr.split("-")
          for (str1 <- b) {
            keyWordList ::= str1
          }
        }
      }
    }


    //映射获取关键字分数

    var max_result_score = 0
    for (kd <- keyWordList.distinct) {

      if (!kd.contains("|") && !kd.contains("TP10006")){
        if (featureThreshold.contains(kd)) {
          //需要调整,取最大tag 分数
          keytongji ::=(category + "\t" + kd+"\t1", 1)
        }else{
          keytongji ::=(category + "\t" + kd+"\t0", 1)

        }


      }



    }

    return keytongji
  }


}

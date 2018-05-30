package testTasks

import java.text.DecimalFormat
import java.util.regex.{Matcher, Pattern}

import cluster.utils.{Constants, GBKFileOutputFormat, WordUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by admin on 2016/10/19.
  */
object BrandRankTestTask {

  def main(args: Array[String]) {

    val conf = new SparkConf()
    val sc: SparkContext = new SparkContext(conf)


    val path = new Path(Constants.brandRankOutputPath)
    WordUtils.delDir(sc,path,true)

    val multiRankRdd: RDD[String] = WordUtils.convert(sc, Constants.multiRankOutputPath, Constants.gbkEncoding)

    val brandRankRdd = brandRankRDD(multiRankRdd).map(x => (null, x))

    brandRankRdd.saveAsNewAPIHadoopFile(Constants.brandRankOutputPath, classOf[Text], classOf[IntWritable],
      classOf[GBKFileOutputFormat[Text, IntWritable]])

    sc.stop()

  }


  def brandRankRDD(multiRank: RDD[String]): RDD[String] = {


    val brandMap = multiRank.map(x => x.split('\t')).filter(x => StringUtils.isNotBlank(x(5))).flatMap(x=>{
      val value = x.mkString("\t")
      x(5).split(",").map(x=>(x,value))
    })
      .combineByKey(
        (v: String) => List(v),
        (c: List[String], v: String) => v :: c,
        (c1: List[String], c2: List[String]) => c1 ++ c2
      )

    val brandRankRdd = brandMap.map(x => computeRank(x)).filter(x=>StringUtils.isNoneBlank(x))
      .sortBy(x=>(x.split('\t')(1).toDouble,x.split('\t')(2).toInt), false)
    return brandRankRdd

  }


  def computeRank(brandMap: (String, List[String])): String = {

    var count: Int = 0
    var sum: Double = 0
    var brandSet: Set[String] = Set[String]()

    val brandName = brandMap._1
    brandMap._2.foreach(
      x => {
        val fields = x.split('\t')
        val poiRank = fields(36)
        val keyword = fields(19)
        brandSet = brandSet.+(getBrand(keyword))
        sum += poiRank.toDouble
        count += 1


      }
    )

    val brandSetSign = brandSet.mkString(",")
    val df: DecimalFormat = new DecimalFormat("#.00")

    if (count > 0) {
      val brandRank: String = df.format(sum / count)
      val result: String = Array(brandName, brandRank, count,brandSetSign).mkString("\t")
      return result
    }

    return null


  }


  private def getBrand(keyword: String): String = {
    //    if (StringUtils.isBlank(keyword)) {
    //      return " "
    //    }
    val tagLSBrand: String = ".*LS:(.*)"
    val tagALSBrand: String = ".*ALS:(.*)"

    var brandSet: Set[String] = Set[String]()

    val pLsTagBrand: Pattern = Pattern.compile(tagLSBrand)
    val mLsTagBrand: Matcher = pLsTagBrand.matcher(keyword)

    val pAlsTagBrand: Pattern = Pattern.compile(tagALSBrand)
    val mAlsTagBrand: Matcher = pAlsTagBrand.matcher(keyword)



    if (mAlsTagBrand.matches()) {
      brandSet = brandSet.+("ALS")
    }else if (mLsTagBrand.matches) {
      brandSet = brandSet.+("LS")
    }

    val brands = brandSet.mkString(",")
    if (StringUtils.isBlank(keyword)) {
      return " "
    }
    return brands
  }



}

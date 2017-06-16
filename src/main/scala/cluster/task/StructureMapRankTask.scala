package cluster.task

import cluster.utils.{GBKFileOutputFormat, WordUtils, Constants}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by admin on 2017/5/8.
  */
object StructureMapRankTask {

  def main(args: Array[String]) {

    System.setProperty("spark.sql.warehouse.dir", Constants.wareHouse)
    val ss = SparkSession.builder().getOrCreate()
    val sc = ss.sparkContext
    val path = new Path(Constants.structureMapRankOutputPath)
    WordUtils.delDir(sc, path, true)

    val poiRank: RDD[(String, String)] = WordUtils.convert(sc, Constants.rankCombineOutputPath, Constants
      .gbkEncoding).map(x => x.split('\t')).map(x => (x(1), Array(x(0), x(1), x(2), x(3), x(36)).mkString
    ("\t"))).cache() //dataid->name,id,city,category,tagscore,tag,weight,rank x(0),x(1),x(2),x(3),x(7),x(19),x(34),x(36)

    val structureInfo = WordUtils.convert(sc, Constants.structureOutPutPath, Constants
      .gbkEncoding).map(x => x.split
    ("\t")).filter(x => x.length == 12)

    //cdataid->pdataid
    val childStructureInfo: RDD[(String, String)] = structureInfo.flatMap(x => x(11).split(",").map(y => (y, x(0))))
    structureRank(poiRank, childStructureInfo).saveAsNewAPIHadoopFile(Constants.structureMapRankOutputPath,
      classOf[Text], classOf[IntWritable], classOf[GBKFileOutputFormat[Text, IntWritable]])

  }


  /**
    * 结构化rank优化
    *
    * @param childStructureInfo
    * @return
    */
  def structureRank(poiRank: RDD[(String, String)], childStructureInfo: RDD[(String, String)]): RDD[(Null, String)] = {


    //parentDataId , 子rank
    val childResult: RDD[(String, String)] = childStructureInfo.join(poiRank).map(x => {
      val childDataId = x._1
      val parentDataId = x._2._1
      val rankInfo = x._2._2
      (parentDataId, rankInfo)

    })


    val pareResult: RDD[(Null, String)] = childResult.join(poiRank).map(x => {
      val parentDataId = x._1
      val childInfo = x._2._1
      val parentInfo = x._2._2

      val childCategory = childInfo.split('\t')(3)
      val parentCategory = parentInfo.split('\t')(3)

      val city = parentInfo.split('\t')(2)
      if (childCategory.equals(parentCategory)) {
        (null, parentInfo + "^" + childInfo)
      }
      else (null, null)

    }).filter(x => StringUtils.isNoneBlank(x._2))




    return pareResult

  }


}

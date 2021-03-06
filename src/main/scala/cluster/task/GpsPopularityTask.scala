package cluster.task

import cluster.model.{CellCut, FeatureValue, Threshold}
import cluster.service.GpsPopularityService
import cluster.utils.{Constants, GBKFileOutputFormat, WordUtils}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by admin on 2016/9/18.
  */
object GpsPopularityTask {




  def main(args: Array[String]) {

    val poiboundPath = "/user/go2data_rank/taoyongbo/output/poiBound/"

    val conf = new SparkConf()
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[CellCut]))
    val sc: SparkContext = new SparkContext(conf)


    val path = new Path(Constants.poiHotCountOutputPath)
    WordUtils.delDir(sc,path,true)

    val poiRdd: RDD[String] = WordUtils.convert(sc, Constants.poiOutPutPath, Constants.gbkEncoding).cache()
    val structureRdd: RDD[String] = WordUtils.convert(sc, Constants.structureInputPath, Constants.gbkEncoding)
    val polygonRdd: RDD[String] = WordUtils.convert(sc, Constants.polygonXmlPath, Constants.gbkEncoding)
    val gpsRdd: RDD[String] = WordUtils.convert(sc, Constants.gpsCountInputPath, Constants.gbkEncoding)
    val gpsPopularityService = new GpsPopularityService

    val gpsPopularity = gpsPopularityService.gpsPopularity(sc,poiRdd,structureRdd,polygonRdd,gpsRdd)

//    val boundRdd: RDD[String] = WordUtils.convert(sc, poiboundPath, Constants.gbkEncoding)

//    val gpsPopularity = gpsPopularityService.gpsPopularity1(sc,boundRdd,gpsRdd)

    gpsPopularity.saveAsNewAPIHadoopFile(Constants.poiHotCountOutputPath, classOf[Text], classOf[IntWritable],
      classOf[GBKFileOutputFormat[Text, IntWritable]])


  }


}

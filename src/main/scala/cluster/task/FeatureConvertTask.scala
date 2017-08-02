package cluster.task

import cluster.model.{Threshold, FeatureValue, Poi}
import cluster.service.impl.StructureInfoService
import cluster.service.{PoiService, StructureService, FeatureCombineService, FeatureConvertService}
import cluster.utils.{RDDMultipleTextOutputFormat, Constants, GBKFileOutputFormat, WordUtils}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.Map

/**
  * Created by admin on 2016/10/10.
  */
object FeatureConvertTask {


  def main(args: Array[String]) {

    val conf = new SparkConf()
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[FeatureValue], classOf[Threshold]))
    val sc: SparkContext = new SparkContext(conf)

    val path = new Path(Constants.featureValueOutputPath)
    WordUtils.delDir(sc, path, true)

    val cityPath = new Path(Constants.cityFeatureValueOutputPath)
    WordUtils.delDir(sc, cityPath, true)


    val structurePath = new Path(Constants.structureOutPutPath)
    WordUtils.delDir(sc, structurePath, true)

    val matchCountRdd: RDD[String] = WordUtils.convert(sc, Constants.matchCountOutputPath, Constants.gbkEncoding)
    val searchCountRdd: RDD[String] = WordUtils.convert(sc, Constants.searchCountInputPath, Constants.gbkEncoding)
    val poiHotCountRdd: RDD[String] = WordUtils.convert(sc, Constants.poiHotCountOutputPath, Constants.gbkEncoding)
    val structureXmlRdd: RDD[String] = WordUtils.convert(sc, Constants.structureInputPath, Constants.gbkEncoding)
    val poiRdd: RDD[String] = WordUtils.convert(sc, Constants.poiOutPutPath, Constants.gbkEncoding).cache()


    //结构化数据
    val structureInfoService = new StructureInfoService
    val structureRdd = structureInfoService.StructureRDD(poiRdd, structureXmlRdd).cache()

    val structuresInfo = structureRdd.map(x => (null, x))
    structuresInfo.saveAsNewAPIHadoopFile(Constants.structureOutPutPath, classOf[Text], classOf[IntWritable], classOf[GBKFileOutputFormat[Text, IntWritable]])


    val featureCombineRdd = FeatureCombineService.CombineRDD(sc, matchCountRdd, searchCountRdd, poiHotCountRdd,
      structureRdd, poiRdd)


    val featureThresholdRdd: RDD[String] = WordUtils.convert(sc, Constants.featureThresholdInputPath, Constants.gbkEncoding)
    val featureValueRdd: RDD[String] = FeatureConvertService.FeatureValueRDD(sc, featureCombineRdd,
      featureThresholdRdd)


    val cityFeatureValue: RDD[(String, String)] = featureValueRdd.map(x => (WordUtils.converterToSpell(x.split
    ("\t")(2)) + "-feature", x))

//    val featureValue: RDD[(String, String)] = featureValueRdd.map(x => (WordUtils.converterToSpell(x.split
//    ("\t")(2)) + "-" + WordUtils.converterToSpell(x.split("\t")(3)) + "-feature", x))

    val featureValue: RDD[(String, String)] = featureValueRdd.map(x => (WordUtils.converterToSpell(x.split("\t")(3)) + "-feature", x))


    featureValue.partitionBy(new HashPartitioner(350)).persist().saveAsHadoopFile(Constants.featureValueOutputPath,
      classOf[Text],
      classOf[IntWritable],
      classOf[RDDMultipleTextOutputFormat])

    cityFeatureValue.partitionBy(new HashPartitioner(350)).persist().saveAsHadoopFile(Constants.cityFeatureValueOutputPath,
      classOf[Text],
      classOf[IntWritable],
      classOf[RDDMultipleTextOutputFormat])
    sc.stop()
  }








}

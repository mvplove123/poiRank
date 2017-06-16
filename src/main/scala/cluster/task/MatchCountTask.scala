package cluster.task

import cluster.model.Poi
import cluster.service.MatchCountService
import cluster.utils.{Constants, GBKFileOutputFormat, WordUtils}
import com.esotericsoftware.kryo.Kryo
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by admin on 2016/9/16.
  */



object MatchCountTask {

  def main(args: Array[String]) {


    val conf = new SparkConf()
//    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    conf.set("spark.kryo.registrator", "cluster.task.MyKryoRegistrator")
    val sc: SparkContext = new SparkContext(conf)

    val path = new Path(Constants.matchCountOutputPath)
    WordUtils.delDir(sc, path, true)
    val poiRdd: RDD[String] = WordUtils.convert(sc, Constants.poiOutPutPath, Constants.gbkEncoding)

    val matchResult = MatchCountService.getMatchCountRDD(sc, poiRdd).map(x => (null, x))
    matchResult.saveAsNewAPIHadoopFile(Constants.matchCountOutputPath, classOf[Text], classOf[IntWritable],
      classOf[GBKFileOutputFormat[Text, IntWritable]])
    sc.stop()


  }

}

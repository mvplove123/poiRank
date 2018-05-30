package cluster.task

import cluster.service.PoiService
import cluster.utils.{WordUtils, GBKFileOutputFormat, Constants}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by admin on 2016/9/13.
  */
object PoiTask {
  def main(args: Array[String]) {


    val conf = new SparkConf()
    val sc: SparkContext = new SparkContext(conf)

    if(args.length<2){
      println(args.mkString("\t"))
      println("no outputpath!")
      sc.stop()
    }

    val baseOutPutPath = args(1)


    val path = new Path(baseOutPutPath+Constants.poiOutPutPath)
    WordUtils.delDir(sc, path, true)

    val poi = PoiService.getPoiRDD(sc,baseOutPutPath).map(x => (null, x))

    poi.saveAsNewAPIHadoopFile(baseOutPutPath+Constants.poiOutPutPath, classOf[Text], classOf[IntWritable],
      classOf[GBKFileOutputFormat[Text, IntWritable]])

    sc.stop()


  }
}

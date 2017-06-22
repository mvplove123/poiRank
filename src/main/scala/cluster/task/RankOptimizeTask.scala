package cluster.task

import cluster.service.impl.{BrandRankOptimizeService, StructureRankOptimizeService}
import cluster.utils.{Constants, RDDMultipleTextOutputFormat, WordUtils}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{HashPartitioner, SparkContext}

/**
  * Created by admin on 2016/19/14.
  */
object RankOptimizeTask {


  def main(args: Array[String]) {

    System.setProperty("spark.sql.warehouse.dir", Constants.wareHouse)
    val ss = SparkSession.builder().getOrCreate()
    val sc: SparkContext = ss.sparkContext

    val path = new Path(Constants.multiOptimizeRankOutputPath)
    WordUtils.delDir(sc, path, true)

    val rankRdd = WordUtils.convert(sc, Constants.rankCombineOutputPath, Constants.gbkEncoding)
    val brandRankRdd = WordUtils.convert(sc, Constants.brandRankOutputPath, Constants.gbkEncoding)
    val structureRankRdd = WordUtils.convert(sc, Constants.structureOptimizeRankOutputPath, Constants.gbkEncoding)


    val structureRankOptimize = new StructureRankOptimizeService
    val brandRankOptimize = new BrandRankOptimizeService
    val newRank = structureRankOptimize.rankOptimize(rankRdd, structureRankRdd)
    val brandRank = brandRankOptimize.rankOptimize(newRank, brandRankRdd)

    val result = brandRank.map(x => (WordUtils
      .converterToSpell(x.split
      ("\t")(2)) + "-rank", x))

    result.partitionBy(new HashPartitioner(400)).saveAsHadoopFile(Constants.multiOptimizeRankOutputPath, classOf[Text],
      classOf[IntWritable],
      classOf[RDDMultipleTextOutputFormat])

    sc.stop()

  }
}

package testTasks

import breeze.numerics.{log, log10}
import cluster.tempTask.GpsCustomStatisticTask._
import cluster.utils.WordUtils

import scala.io.Source

/**
  * Created by admin on 2017/7/6.
  */
object Pintest {

  def main(args: Array[String]) {


//    val file=Source.fromFile("D:\\structure\\city",enc = "gb18030")
//
//    var specialMap = Map[String,String]()
//
//    specialMap+="抚州市"->"fuzhoushi0"
//    specialMap+="伊春市"->"yichunshi0"
//    specialMap+="榆林市"->"yulinshi0"
//    specialMap+="台州市"->"taizhoushi0"
//    specialMap+="宿州市"->"suzhoushi0"
//
//
//
//    var pinminlist = List[String]()
//    for (line<-file.getLines){
//      val pinyin = WordUtils.converterToSpell(line)
//      println(pinyin)
//
//
//    }
    var point="1.2988500463E7,4820860.178"
    val xy = point.split(",")
    var boundXY = getCrossCellXY(xy(0), xy(1), 200)

    println(boundXY)

  }
  def getCrossCellXY (sx: String, sy: String , dim:Int):String={
    val r: Double = dim
    val x: Double = sx.toDouble
    val y: Double = sy.toDouble
    val xl: Double = x - r
    val xh: Double = x + r
    val yl: Double = y - r
    val yh: Double = y + r
    return xl + "," + xh + "," + yl + "," + yh
  }

}

package testTasks

import cluster.utils.WordUtils

import scala.io.Source

/**
  * Created by admin on 2017/7/6.
  */
object Pintest {

  def main(args: Array[String]) {


    val file=Source.fromFile("D:\\structure\\city",enc = "gb18030")

    var specialMap = Map[String,String]()

    specialMap+="抚州市"->"fuzhoushi0"
    specialMap+="伊春市"->"yichunshi0"
    specialMap+="榆林市"->"yulinshi0"
    specialMap+="台州市"->"taizhoushi0"
    specialMap+="宿州市"->"suzhoushi0"



    var pinminlist = List[String]()
    for (line<-file.getLines){
      val pinyin = WordUtils.converterToSpell(line)
      println(pinyin)


    }



  }


}

package cluster.service

import java.text.DecimalFormat
import java.util.regex.{Matcher, Pattern}

import breeze.linalg.min
import cluster.model.Poi
import cluster.utils.{Constants, WordUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.xml.sax.SAXParseException

import scala.collection.mutable
import scala.reflect.internal.util.Collections
import scala.xml.{NodeSeq, Elem, Node, XML}

/**
  * Created by admin on 2016/9/10.
  */
object PoiService {

  def getPoiRDD(sc: SparkContext, baseOutPutPath: String): RDD[String] = {

    val poiNochange: RDD[String] = PoiService.getPoiString(WordUtils.convert(sc, baseOutPutPath + Constants
      .poiXmlInputPath, Constants.gbkEncoding))

    val poiMyself: RDD[String] = getPoiString(WordUtils.convert(sc, baseOutPutPath + Constants.poiXmlMyselfPath,
      Constants.gbkEncoding))

    val poi: RDD[String] = sc.union(poiNochange, poiMyself)

    return poi
  }

  def getPoiString(poiLine: RDD[String]): RDD[String] = {

    val poiRdd: RDD[Poi] = getPoiRdd(poiLine)
    val poi = poiRdd.map(x => Array(x.name, x.dataId, x
      .point, x
      .city,
      x.category, x.subCategory,
      x.brand, x.keyword, x.commentNum, x.price, x.grade, x.guid, x.alias).mkString("\t"))
    return poi

  }

  def getPoiRdd(poiLine: RDD[String]): RDD[Poi] = {
    try {
      val line: RDD[Node] = poiLine.flatMap { s =>
        try {
          XML.loadString(s)
        } catch {
          case ex: SAXParseException => {
            println(s)
            None
          }
        }
      }
      val poiRdd: RDD[Poi] = line.map(x => parsePoi(x))
      return poiRdd

    } catch {
      case ex: Exception => {
      }
    }
    return null

  }


  def parsePoi(x: Node): Poi = {

    val poi = new Poi
    poi.name = x.\("SRC_NAME").text
    poi.city = x.\("SRC_CITY").text
    poi.dataId = "1_" + x.\("DATAID").text

    val guid = x.\("GUID").text
    if (!(guid).isEmpty) poi.guid = guid
    poi.lat = x.\("SRC_X").text
    poi.lng = x.\("SRC_Y").text
    if (!poi.lat.isEmpty) poi.point = poi.lat + "," + poi.lng

    val srcType = x.\("SRC_TYPE").text.split(";;")
    if (srcType.length > 0) {
      poi.category = srcType.apply(0)
      poi.subCategory = srcType.apply(1)
    }

    poi.keyword = x.\("KEYWORDS").text + "," + x.\("TAG").text
    poi.province = x.\("SRC_PROVINCE").text
    poi.brand = getBrand(poi.keyword)
    poi.geometry = x.\("GEOMETRY").text
    poi.alias = x.\("SRC_ALIAS").text
    if (StringUtils.isBlank(poi.alias)) {
      poi.alias = " "
    }



    poi.introduction = x.\("INTRODUCTION").text

    var comment = mutable.Map[String, String]()
    var price = mutable.Map[String, String]()
    var grade = mutable.Map[String, String]()


    val deepStr = x.\\("DEEP").text
    if (!deepStr.isEmpty) {

      try {

        val deep = "<root>" + deepStr + "</root>"
        val deepXml: Elem = XML.loadString(deep)


        val items = deepXml \ "additional" \ "data" \ "items" \ "item"



        for (item <- items) {

          val source = item.attributes("source").text

          source match {
            case Constants.DIANPING =>

              //其他类评论数
              val recordCount = (item \ "ReviewList" \ "RecordCount").text
              if (is_valid(recordCount)) comment += (Constants.DIANPING -> featureValueToNumber
              (recordCount))

              //其他类价格
              val avgPrice = (item \ "Shop" \ "AvgPrice").text
              if (is_valid(avgPrice)) price += (Constants.DIANPING -> featureValueToNumber(avgPrice))

              //其他类星级打分
              val scoremap = (item \ "Shop" \ "Scoremap").text
              if (is_valid(scoremap)) grade += (Constants.DIANPING -> featureValueToNumber(scoremap))

            case Constants.CTRIP =>
              //酒店评论数
              val hotelcommentnum = (item \ "hotelcommentnum").text
              if (is_valid(hotelcommentnum)) comment += (Constants.CTRIP -> featureValueToNumber(hotelcommentnum))

              //酒店价格
              val hotelrooms: NodeSeq = (item \ "hotelrooms" \ "hotelroom")
              val priceList = hotelrooms.map(x => (x \ "hotelroomprice").text.toDouble)
              if (!priceList.isEmpty){
                val minPrice = min(priceList.toList).toString
                if (is_valid(minPrice)) price += (Constants.CTRIP -> featureValueToNumber
                (minPrice))
              }


              //酒店星级打分
              val hotelrank = (item \ "hotelrank").text
              if (is_valid(hotelrank)) grade += (Constants.CTRIP -> featureValueToNumber
              (hotelrank))

            case Constants.ZHUNA =>
              //酒店评论数
              val hotelcommentnum = (item \ "hotelcommentnum").text
              if (is_valid(hotelcommentnum)) comment += (Constants.ZHUNA -> featureValueToNumber(hotelcommentnum))

              //酒店价格
              val minPrice = (item \ "minPrice" \ "hotelroomprice").text
              if (is_valid(minPrice)) price += (Constants.ZHUNA -> featureValueToNumber(minPrice))

              //酒店星级打分
              val hotelrank = (item \ "hotelrank").text
              if (is_valid(hotelrank)) grade += (Constants.ZHUNA -> featureValueToNumber((hotelrank.toDouble * 0.05)
                .toString))


            case Constants.TONGCHENG =>

              //景点类评论数
              val commentcount = (item \ "commentcount").text
              if (is_valid(commentcount)) comment += (Constants.TONGCHENG -> featureValueToNumber(commentcount))

              //景点类价格
              val scenicPrice = (item \ "price").text
              if (is_valid(scenicPrice)) price += (Constants.TONGCHENG -> featureValueToNumber(scenicPrice))

              //景点类星级打分
              val praise = (item \ "praise").text
              if (is_valid(praise)) grade += (Constants.TONGCHENG -> featureValueToNumber((praise.toDouble * 0.05).toString))
            case Constants.WUBA =>
              //房地产价格
              val sellingPrice = (item \ "poi" \ "selling_price").text
              if (is_valid(sellingPrice)) price += (Constants.WUBA -> featureValueToNumber(sellingPrice))

            case Constants.CTRIPSCENERY =>
              //景点类评论数
              val commentcount = (item \ "commentcount").text
              if (is_valid(commentcount)) comment += (Constants.CTRIPSCENERY -> featureValueToNumber(commentcount))

              //景点类价格
              val scenicPrice = (item \ "CtripPrice").text
              if (is_valid(scenicPrice)) price += (Constants.CTRIPSCENERY -> featureValueToNumber(scenicPrice))

              //景点类星级打分
              val praise = (item \ "praise").text
              if (is_valid(praise)) grade += (Constants.CTRIPSCENERY -> featureValueToNumber(praise))


            case everythingElse =>
              None
          }
        }


      } catch {
        case ex: Exception => {
          println(ex)
        }
      }

      if (poi.category.equals("宾馆饭店")) {

        if (comment.contains(Constants.CTRIP)) {
          poi.commentNum = featureValueToNumber(comment(Constants.CTRIP))
        } else if (comment.contains(Constants.ZHUNA)) {
          poi.commentNum = featureValueToNumber(comment(Constants.ZHUNA))
        } else if (comment.contains(Constants.DIANPING)) {
          poi.commentNum = featureValueToNumber(comment(Constants.DIANPING))
        }

        if (price.contains(Constants.CTRIP)) {
          poi.price = featureValueToNumber(price(Constants.CTRIP))
        } else if (price.contains(Constants.ZHUNA)) {
          poi.price = featureValueToNumber(price(Constants.ZHUNA))
        } else if (price.contains(Constants.DIANPING)) {
          poi.price = featureValueToNumber(price(Constants.DIANPING))
        }

        if (grade.contains(Constants.CTRIP)) {
          poi.grade = featureValueToNumber(grade(Constants.CTRIP))
        } else if (grade.contains(Constants.ZHUNA)) {
          poi.grade = featureValueToNumber((grade(Constants.ZHUNA)))
        } else if (grade.contains(Constants.DIANPING)) {
          poi.grade = featureValueToNumber(grade(Constants.DIANPING))
        }

      }
      else if (poi.category.equals("旅游景点")) {


        if (comment.contains(Constants.CTRIPSCENERY)) {
          poi.commentNum = featureValueToNumber(comment(Constants.CTRIPSCENERY))
        } else if (comment.contains(Constants.TONGCHENG)) {
          poi.commentNum = featureValueToNumber(comment(Constants.TONGCHENG))
        } else if (comment.contains(Constants.DIANPING)) {
          poi.commentNum = featureValueToNumber(comment(Constants.DIANPING))

        }

        if (price.contains(Constants.CTRIPSCENERY)) {
          poi.price = featureValueToNumber(price(Constants.CTRIPSCENERY))
        } else if (price.contains(Constants.TONGCHENG)) {
          poi.price = featureValueToNumber(price(Constants.TONGCHENG))
        } else if (price.contains(Constants.DIANPING)) {
          poi.price = featureValueToNumber(price(Constants.DIANPING))

        }

        if (grade.contains(Constants.CTRIPSCENERY)) {
          poi.grade = featureValueToNumber(grade(Constants.CTRIPSCENERY))
        } else if (grade.contains(Constants.TONGCHENG)) {
          poi.grade = featureValueToNumber(grade(Constants.TONGCHENG))
        } else if (grade.contains(Constants.DIANPING)) {
          poi.grade = featureValueToNumber(grade(Constants.DIANPING))

        }

      }
      else if (poi.category.equals("房地产")) {

        if (price.contains(Constants.WUBA)) {
          poi.price = featureValueToNumber(price(Constants.WUBA))
        }
      }
      else {

        if (comment.contains(Constants.DIANPING)) {
          poi.commentNum = featureValueToNumber(comment(Constants.DIANPING))
        }
        if (price.contains(Constants.DIANPING)) {
          poi.price = featureValueToNumber(price(Constants.DIANPING))
        }
        if (grade.contains(Constants.DIANPING)) {
          poi.grade = featureValueToNumber(grade(Constants.DIANPING))
        }
      }
    }
    return poi
  }

  def is_valid(info: String): Boolean = {

    if (info.isEmpty || info.equals("0") || info.equals("0.0")) {
      return false
    } else {
      return true
    }


  }


  /**
    * 提取品牌
    *
    * @param keyword
    * @return
    */
  def getBrand(keyword: String): String = {
    //    if (StringUtils.isBlank(keyword)) {
    //      return " "
    //    }

    val brands = keyword.split(",").filter(x => x.contains("$$ALS") || x.contains("$$LS")).map(x => {

      val ss = x.endsWith("$$")
      val start: Int = x.indexOf(":")
      val end: Int = x.length - 2
      x.substring(start + 1, end)

    }).distinct.mkString(",")


    if (StringUtils.isBlank(keyword)) {
      return " "
    }
    return brands
  }

  val df: DecimalFormat = new DecimalFormat("0")

  def featureValueToNumber(fieldValue: String): String = {

    //    if (StringUtils.isBlank(fieldValue)){
    //      return "0.0"
    //    }
    //
    //    return df.format(fieldValue.toDouble)


    var newFieldValue = "0"
    if (fieldValue.contains(".")) newFieldValue = fieldValue.substring(0, fieldValue.indexOf("."))
    else
      newFieldValue = fieldValue
    return newFieldValue

  }


  def main(args: Array[String]) {
    val keyword: String = "$$NEWMANUAL$$,$$AFB:汽车服务-4S店-沃尔沃4S店-沃尔沃亚太$$,$$AFB:汽车服务-4S店-沃尔沃4S店-进口沃尔沃$$,$$ALS:沃尔沃4S店$$," +
      "$$ALS:沃尔沃亚太$$,$$ALS:进口沃尔沃$$,$$DG:点评爱车-4S店-汽车销售$$"

    //,$$ALS:云南过桥米线$$
    val key = "1111"

    val brand = getBrand(keyword = keyword)

    print(brand)
  }


}

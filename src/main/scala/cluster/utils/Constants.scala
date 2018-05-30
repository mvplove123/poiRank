package cluster.utils

import scala.collection.mutable

/**
  * Created by admin on 2016/9/12.
  */
object Constants{

  val CTRIP = "CTRIP"
  val DIANPING = "DIANPING"
  val ZHUNA = "ZHUNA"
  val TONGCHENG = "TONGCHENG"
  val WUBA = "58"
  val CTRIPSCENERY = "CTRIPSCENERY"


  val MultiRank = "multiRank"
  val SingleRank = "singleRank"
  val gbkEncoding = "gb18030"


  val wareHouse = "/user/go2data_rank/taoyongbo/output/warehouse/"
  val featureThresholdInputPath = "/config/poi-threshold.txt"
  val weightInputPath = "/config/poiWeight.txt"
  val citySizeInputPath = "taoyongbo/rank/config/citysize.txt"
  val hitCountInputPath = "taoyongbo/rank/default/hitCount/"
  val newMatchCountOutputPath = "taoyongbo/rank/default/newMatchCount/"
  val matchCountOutputPath = "taoyongbo/rank/default/matchCount/"

  val poiHotCountInputPath = "/poiHotCount/"
  val gpsCountInputPath = "/gps/"
  val structureInputPath = "/nameStructure/"


  val poiXmlInputPath = "/poiXml1/"
  val busXmlPoiPath = "/poiXml2/"
  val poiXmlMyselfPath = "/poiXml3/"
  val polygonXmlPath = "/polygonXml/"


  val poiOutPutPath = "/poi/"
  val structureOutPutPath = "/structureInfo/"
  val featureCombineOutputPath = "/featureCombine/"
  val featureValueOutputPath = "/featureValue/"
  val cityFeatureValueOutputPath = "/cityFeatureValue/"
  val brandFeatureOutputPath = "/brandFeatureValue/"


  val poiHotCountOutputPath = "/poiHotCount/"


  //rank
  val multiRankOutputPath = "/multiRank/"

  val allmultiRankOutputPath = "/user/go2data_rank/taoyongbo/output/rank/multiRank/*/"

  val hotCountRankOutputPath = "/hotCountRank/"
  val hitCountRankOutputPath = "/hitCountRank/"
  val rankCombineOutputPath = "/rankCombine/"
  val brandRankOutputPath = "/brandRank/"
  val polygonRankOutputPath = "/polygonRank/"

  val keyPath = "/user/go2data_rank/taoyongbo/output/key/"

  val multiOptimizeRankOutputPath = "/multiOptimizeRank"


  val structureMapRankOutputPath = "/structureMapRank"
  val structureOptimizeRankOutputPath = "/structureOptimizeRank"

  val multiRank = "/rank/multiRank/"
  val hotCountRank = "/rank/hotCountRank/"
  val rankCombine = "/rank/rankCombine/"
  val brandRank = "/rank/brandRank/"
  val multiOptimizeRank = "/output/multiOptimizeRank/"
  val structureMapRank = "/rank/structureMapRank/"
  val structureOptimizeRank = "/rank/structureOptimizeRank/"


  //frank
  val filterPoiInputPath = "/filterPoi/"
  val filterRankPath = "/FRank/"
  val poiboundPath = "/gpsCustomStatistic/"
  val similarQueryCountPath = "/similarQueryCount/"
  val vrViewCountPath = "taoyongbo/input/filterRank/vrViewCount/"
  val vrHitCountPath = "taoyongbo/input/filterRank/vrHitCount/"
  val sogouViewCount = "taoyongbo/input/filterRank/sogouViewCount/"


  /**
    * 分类边距
    *
    * @return
    */
  def getCategoryBound: mutable.Map[String, Int] = {
    val categoryBound = mutable.Map[String, Int]()
    categoryBound += ("地名-村庄" -> 300)
    categoryBound += ("地名-地名" -> 500)
    //    categoryBound+=("地名-其它"-> 0)
    //    categoryBound+=("地名-区县"-> 0)
    categoryBound += ("地名-乡镇" -> 1000)
    categoryBound += ("旅游景点-5A4A景点" -> 700)
    categoryBound += ("房地产-别墅" -> 500)
    categoryBound += ("房地产-居民小区" -> 500)
    categoryBound += ("房地产-楼号" -> 500)
    categoryBound += ("房地产-楼盘" -> 500)
    categoryBound += ("交通出行-大型火车站" -> 500)
    categoryBound += ("交通出行-航站楼" -> 500)
    categoryBound += ("交通出行-火车站" -> 500)
    categoryBound += ("交通出行-机场" -> 500)
    categoryBound += ("旅游景点-1-3A景点" -> 500)
    categoryBound += ("旅游景点-知名景点" -> 500)
    categoryBound += ("其它-大型化工厂" -> 500)
    categoryBound += ("其它-大型热电厂" -> 500)
    categoryBound += ("其它-大型制药厂" -> 500)
    categoryBound += ("其它-垃圾处理厂" -> 500)
    categoryBound += ("其它-垃圾填埋场" -> 500)
    categoryBound += ("其它-污水处理厂" -> 500)
    categoryBound += ("汽车服务-驾校" -> 500)
    categoryBound += ("汽车服务-检测场" -> 500)
    categoryBound += ("体育场馆-大型体育场馆" -> 500)
    categoryBound += ("学校科研-知名大学" -> 500)
    categoryBound += ("房地产-公寓" -> 300)
    categoryBound += ("场馆会所-大型博物馆" -> 200)
    categoryBound += ("场馆会所-大型展览馆" -> 200)
    categoryBound += ("房地产-高档楼盘" -> 200)
    categoryBound += ("公司企业-工厂" -> 200)
    categoryBound += ("公司企业-知名工厂" -> 200)
    categoryBound += ("交通出行-长途客运站" -> 200)
    categoryBound += ("交通出行-地铁站" -> 200)
    categoryBound += ("旅游景点-度假村" -> 200)
    categoryBound += ("旅游景点-公园" -> 200)
    categoryBound += ("旅游景点-景点" -> 200)
    categoryBound += ("旅游景点-绿地点" -> 200)
    categoryBound += ("宾馆饭店-4-5星级" -> 100)
    categoryBound += ("宾馆饭店-酒店式公寓" -> 100)
    categoryBound += ("宾馆饭店-普通" -> 100)
    categoryBound += ("宾馆饭店-其它" -> 100)
    categoryBound += ("宾馆饭店-其它星级" -> 100)
    categoryBound += ("宾馆饭店-招待所" -> 100)
    categoryBound += ("场馆会所-博物馆" -> 100)
    categoryBound += ("场馆会所-大型图书馆" -> 100)
    categoryBound += ("场馆会所-展览馆" -> 100)
    categoryBound += ("地名-水系点" -> 100)
    categoryBound += ("房地产-高档写字楼" -> 100)
    categoryBound += ("房地产-其它" -> 100)
    categoryBound += ("房地产-写字楼" -> 100)
    categoryBound += ("购物场所-大型商场" -> 100)
    categoryBound += ("购物场所-电脑城" -> 100)
    categoryBound += ("购物场所-电器城" -> 100)
    categoryBound += ("购物场所-服装市场" -> 100)
    categoryBound += ("购物场所-花鸟市场" -> 100)
    categoryBound += ("购物场所-家居市场" -> 100)
    categoryBound += ("购物场所-建材市场" -> 100)
    categoryBound += ("购物场所-旧货市场" -> 100)
    categoryBound += ("购物场所-农贸市场" -> 100)
    categoryBound += ("购物场所-批发市场" -> 100)
    categoryBound += ("旅游景点-教堂" -> 100)
    categoryBound += ("旅游景点-其它" -> 100)
    categoryBound += ("汽车服务-停车场" -> 100)
    categoryBound += ("汽车服务-修理厂" -> 100)
    categoryBound += ("体育场馆-健身场所" -> 100)
    categoryBound += ("体育场馆-体育场馆" -> 100)
    categoryBound += ("新闻媒体-电视台" -> 100)
    categoryBound += ("学校科研-大学" -> 100)
    categoryBound += ("学校科研-大专" -> 100)
    categoryBound += ("学校科研-一般大学" -> 100)
    categoryBound += ("医疗卫生-二级医院" -> 100)
    categoryBound += ("医疗卫生-三级医院" -> 100)
    categoryBound += ("政府机关-政府驻地" -> 100)
    categoryBound += ("宾馆饭店-楼号" -> 50)
    categoryBound += ("餐饮服务-楼号" -> 50)
    categoryBound += ("场馆会所-大门" -> 50)
    categoryBound += ("场馆会所-俱乐部" -> 50)
    categoryBound += ("场馆会所-楼号" -> 50)
    categoryBound += ("场馆会所-其它" -> 50)
    categoryBound += ("场馆会所-图书馆" -> 50)
    categoryBound += ("地名-大门" -> 50)
    categoryBound += ("地名-楼号" -> 50)
    categoryBound += ("房地产-大门" -> 50)
    categoryBound += ("公司企业-大门" -> 50)
    categoryBound += ("公司企业-公司" -> 50)
    categoryBound += ("公司企业-楼号" -> 50)
    categoryBound += ("公司企业-其它" -> 50)
    categoryBound += ("公司企业-知名公司" -> 50)
    categoryBound += ("购物场所-大门" -> 50)
    categoryBound += ("购物场所-大型超市" -> 50)
    categoryBound += ("购物场所-其它" -> 50)
    categoryBound += ("购物场所-一般商场" -> 50)
    categoryBound += ("购物场所-专卖店" -> 50)
    categoryBound += ("交通出行-大门" -> 50)
    categoryBound += ("交通出行-立交桥" -> 50)
    categoryBound += ("旅游景点-大门" -> 50)
    categoryBound += ("其它-大门" -> 50)
    categoryBound += ("其它-积水点" -> 50)
    categoryBound += ("其它-楼号" -> 50)
    categoryBound += ("其它-其它" -> 50)
    categoryBound += ("汽车服务-4S店" -> 50)
    categoryBound += ("汽车服务-加气站" -> 50)
    categoryBound += ("汽车服务-加油站" -> 50)
    categoryBound += ("汽车服务-其它" -> 50)
    categoryBound += ("汽车服务-专卖店" -> 50)
    categoryBound += ("体育场馆-楼号" -> 50)
    categoryBound += ("体育场馆-其它" -> 50)
    categoryBound += ("体育场馆-游泳馆" -> 50)
    categoryBound += ("新闻媒体-广播" -> 50)
    categoryBound += ("新闻媒体-其它" -> 50)
    categoryBound += ("新闻媒体-艺术团体" -> 50)
    categoryBound += ("休闲娱乐-KTV" -> 50)
    categoryBound += ("休闲娱乐-歌舞厅" -> 50)
    categoryBound += ("休闲娱乐-楼号" -> 50)
    categoryBound += ("休闲娱乐-洗浴中心" -> 50)
    categoryBound += ("休闲娱乐-夜总会" -> 50)
    categoryBound += ("休闲娱乐-影剧院" -> 50)
    categoryBound += ("休闲娱乐-娱乐城" -> 50)
    categoryBound += ("学校科研-科研院所" -> 50)
    categoryBound += ("学校科研-楼号" -> 50)
    categoryBound += ("学校科研-其它" -> 50)
    categoryBound += ("学校科研-研究生院" -> 50)
    categoryBound += ("学校科研-一般小学" -> 50)
    categoryBound += ("学校科研-一般中学" -> 50)
    categoryBound += ("学校科研-知名小学" -> 50)
    categoryBound += ("医疗卫生-一般医院" -> 50)
    categoryBound += ("医疗卫生-一级医院" -> 50)
    categoryBound += ("政府机关-事业单位" -> 50)
    categoryBound += ("政府机关-政府机关" -> 50)
    categoryBound += ("政府机关-主要政府机关" -> 50)
    categoryBound += ("宾馆饭店-大门" -> 30)
    categoryBound += ("餐饮服务-茶馆" -> 30)
    categoryBound += ("餐饮服务-大门" -> 30)
    categoryBound += ("餐饮服务-酒吧" -> 30)
    categoryBound += ("餐饮服务-咖啡馆" -> 30)
    categoryBound += ("餐饮服务-快餐小吃" -> 30)
    categoryBound += ("餐饮服务-冷饮" -> 30)
    categoryBound += ("餐饮服务-面包甜点" -> 30)
    categoryBound += ("餐饮服务-其它" -> 30)
    categoryBound += ("餐饮服务-一般西餐" -> 30)
    categoryBound += ("餐饮服务-一般中餐" -> 30)
    categoryBound += ("餐饮服务-一般综合" -> 30)
    categoryBound += ("餐饮服务-异国风味" -> 30)
    categoryBound += ("餐饮服务-知名中餐" -> 30)
    categoryBound += ("餐饮服务-知名综合" -> 30)
    categoryBound += ("购物场所-礼品店" -> 30)
    categoryBound += ("购物场所-书店" -> 30)
    categoryBound += ("购物场所-一般超市" -> 30)
    categoryBound += ("交通出行-码头" -> 30)
    categoryBound += ("交通出行-其它" -> 30)
    categoryBound += ("交通出行-桥梁" -> 30)
    categoryBound += ("金融银行-分理处和储蓄所" -> 30)
    categoryBound += ("金融银行-银行" -> 30)
    categoryBound += ("金融银行-证券" -> 30)
    categoryBound += ("金融银行-支行" -> 30)
    categoryBound += ("金融银行-总部" -> 30)
    categoryBound += ("新闻媒体-报社" -> 30)
    categoryBound += ("新闻媒体-出版社" -> 30)
    categoryBound += ("学校科研-一般幼儿园" -> 30)
    categoryBound += ("学校科研-知名幼儿园" -> 30)
    categoryBound += ("学校科研-知名中学" -> 30)
    categoryBound += ("学校科研-中专" -> 30)
    categoryBound += ("医疗卫生-防疫站" -> 30)
    categoryBound += ("医疗卫生-楼号" -> 30)
    categoryBound += ("医疗卫生-其它" -> 30)
    categoryBound += ("邮政电信-邮局" -> 30)
    categoryBound += ("公司企业-火车票代售处" -> 20)
    categoryBound += ("购物场所-鲜花店" -> 20)
    categoryBound += ("购物场所-眼镜店" -> 20)
    categoryBound += ("购物场所-音像店" -> 20)
    categoryBound += ("交通出行-地铁站出入口" -> 20)
    categoryBound += ("交通出行-服务区" -> 20)
    categoryBound += ("交通出行-高速公路出口" -> 20)
    categoryBound += ("交通出行-高速公路入口" -> 20)
    categoryBound += ("交通出行-公交车站" -> 20)
    categoryBound += ("交通出行-红绿灯" -> 20)
    categoryBound += ("交通出行-落客区" -> 20)
    categoryBound += ("交通出行-收费站" -> 20)
    categoryBound += ("金融银行-基金" -> 20)
    categoryBound += ("金融银行-楼号" -> 20)
    categoryBound += ("金融银行-门址" -> 20)
    categoryBound += ("金融银行-期货" -> 20)
    categoryBound += ("金融银行-其它" -> 20)
    categoryBound += ("金融银行-信托" -> 20)
    categoryBound += ("金融银行-资产管理" -> 20)
    categoryBound += ("金融银行-租赁" -> 20)
    categoryBound += ("汽车服务-充电桩" -> 20)
    categoryBound += ("汽车服务-大门" -> 20)
    categoryBound += ("体育场馆-大门" -> 20)
    categoryBound += ("新闻媒体-大门" -> 20)
    categoryBound += ("新闻媒体-杂志社" -> 20)
    categoryBound += ("休闲娱乐-大门" -> 20)
    categoryBound += ("休闲娱乐-其它" -> 20)
    categoryBound += ("休闲娱乐-网吧" -> 20)
    categoryBound += ("学校科研-大门" -> 20)
    categoryBound += ("医疗卫生-宠物医院" -> 20)
    categoryBound += ("医疗卫生-大门" -> 20)
    categoryBound += ("医疗卫生-药店" -> 20)
    categoryBound += ("医疗卫生-诊所" -> 20)
    categoryBound += ("邮政电信-大门" -> 20)
    categoryBound += ("邮政电信-电信" -> 20)
    categoryBound += ("邮政电信-联通" -> 20)
    categoryBound += ("邮政电信-其它" -> 20)
    categoryBound += ("邮政电信-铁通" -> 20)
    categoryBound += ("邮政电信-移动" -> 20)
    categoryBound += ("政府机关-大门" -> 20)
    categoryBound += ("政府机关-楼号" -> 20)
    categoryBound += ("政府机关-其它" -> 20)
    categoryBound += ("金融银行-保险" -> 10)
    categoryBound += ("金融银行-大门" -> 10)
    categoryBound += ("购物场所-楼号" -> 5)
    categoryBound += ("金融银行-ATM" -> 5)
    return categoryBound
  }

  var specialPinyinMap = Map[String,String]()

  specialPinyinMap+="抚州市"->"fuzhoushi0"
  specialPinyinMap+="伊春市"->"yichunshi0"
  specialPinyinMap+="榆林市"->"yulinshi0"
  specialPinyinMap+="台州市"->"taizhoushi0"
  specialPinyinMap+="宿州市"->"suzhoushi0"

  val cityList = Array("shanghaishi", "guangzhoushi", "wuhanshi", "hangzhoushi", "chengdoushi",
    "nanjingshi", "beijingshi", "shenzhenshi")


  val categorys: Array[String] = Array("lvYouJingDian", "binGuanFanDian", "yiLiaoWeiSheng", "fangDiChan", "xueXiaoKeYan",
    "canYinFuWu",
    "xiuXianYuLe", "jinRongYinHang", "changGuanHuiSuo", "gongSiQiYe"
    , "youZhengDianXin", "zhengFuJiGuan", "qiCheFuWu", "gouWuChangSuo", "jiaoTongChuXing", "diMing", "xinWenMeiTi",
    "tiYuChangGuan"
    , "qiTa")


  val categoryList = Array("地名地址信息", "交通设施服务", "通行设施")

  /**
    * 获取城市分类
    *
    * @return
    */
  def getCityCategory(): Array[String] = {

    val cityCategory: Array[String] = cityList.flatMap(city => categorys.map(category => (city + "-" + category)))

    return cityCategory

  }


}

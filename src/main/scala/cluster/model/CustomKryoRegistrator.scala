package cluster.model

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

/**
  * Created by admin on 2017/5/13.
  */
class CustomKryoRegistrator extends KryoRegistrator {

  //注册使用Kryo序列化的类，要求MyClass1和MyClass2必须实现java.io.Serializable
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[Poi])
  }


}

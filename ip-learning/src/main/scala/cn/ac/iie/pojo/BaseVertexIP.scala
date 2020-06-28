package cn.ac.iie.pojo

import com.arangodb.entity.DocumentField
import com.arangodb.entity.DocumentField.Type

import scala.beans.BeanProperty

class BaseVertexIP {
  @BeanProperty
  @DocumentField(Type.KEY)
  var key: String=""

  @BeanProperty
  @DocumentField(Type.ID)
  var id: String=""

  @BeanProperty
  var FIRST_FOUND_TIME:Long = 0

  @BeanProperty
  var LAST_FOUND_TIME:Long = 0

  @BeanProperty
  var IP_APPEAR_COUNT:Long = 0

  @BeanProperty
  var IP:String = ""

  @BeanProperty
  var IP_LOCATION:String = ""

}

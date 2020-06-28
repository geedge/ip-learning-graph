package cn.ac.iie.pojo

import com.arangodb.entity.DocumentField
import com.arangodb.entity.DocumentField.Type

import scala.beans.BeanProperty

class BaseVertexFqdn {

  @BeanProperty
  @DocumentField(Type.KEY)
  var key: String=""

  @BeanProperty
  @DocumentField(Type.ID)
  var id: String=""

  @BeanProperty
  var FQDN_FIRST_FOUND_TIME:Long = 0

  @BeanProperty
  var FQDN_LAST_FOUND_TIME:Long = 0

  @BeanProperty
  var FQDN_COUNT_TOTAL:Long = 0

  @BeanProperty
  var FQDN_NAME:String = ""

}

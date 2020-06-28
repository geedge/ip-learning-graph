package cn.ac.iie.test

import com.arangodb.entity.DocumentField
import com.arangodb.entity.DocumentField.Type

import scala.beans.BeanProperty

class TestBaseEdgeDocument {

  @BeanProperty
  @DocumentField(Type.FROM)
  var from: String=""
  @BeanProperty
  @DocumentField(Type.TO)
  var to: String=""
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
  var COUNT_TOTAL:Long = 0

}

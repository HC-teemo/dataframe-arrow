import org.apache.arrow.vector.util.Text
import org.apache.arrow.vector.{BitVector, VarCharVector, VectorSchemaRoot}
import org.junit.{Before, Test}

import java.util.Arrays.asList

class ArrowTest {
  var nodes: Iterator[VectorSchemaRoot] = _
  var rels:  Iterator[VectorSchemaRoot] = _
  val VECTOR_SIZE = 99999


  @Before
  def loadData(): Unit = {
    val startTime = System.currentTimeMillis()
//    nodes = ArrowDataReader.nodes(IO.nodesFile10)
//    rels  = ArrowDataReader.relationships(IO.relsFile10)
    nodes = ArrowDataReader.nodes(IO.nodesFile1, VECTOR_SIZE)
    rels  = ArrowDataReader.relationships(IO.relsFile1, VECTOR_SIZE)
    println("load time: ", System.currentTimeMillis() - startTime)
  }

  /*
    filter By firstname = Ali
    return count(id)
   */
  @Test
  def filterByOneColumn(): Unit = {
    val startTime = System.currentTimeMillis()
    val ali = new Text("Ali")
//    nodes.map(_.contentToTSVString()) foreach println
    val result = nodes.filter{ vector =>
      val flag = vector.getVector(0).asInstanceOf[BitVector]
      val firstname = vector.getVector("firstname").asInstanceOf[VarCharVector]
      val reader = firstname.getReader
      for (i <- 0 until firstname.getValueCount) {
        reader.setPosition(i)
        if (!reader.readText().equals(ali)) flag.setNull(i)
      }
      flag.getNullCount != flag.getValueCount
    }.map{ vector => // TODO close?
      val flag = vector.getVector(0)
      flag.getValueCount - flag.getNullCount
    }.sum
    println("filterByOneColumn time: ", System.currentTimeMillis() - startTime)
    println(result)
  }

  /*
    filter By lastname = Sharma and gender = male
    return count(id)
   */
  @Test
  def filterByMultiColumn(): Unit ={
    val startTime = System.currentTimeMillis()
    val sharma = new Text("Sharma")
    val male = new Text("male")
    //    nodes.map(_.contentToTSVString()) foreach println
    val result = nodes.filter{ vector =>
      val flag = vector.getVector(0).asInstanceOf[BitVector]
      val firstname = vector.getVector("lastname").asInstanceOf[VarCharVector]
      val gender = vector.getVector("gender").asInstanceOf[VarCharVector]
      val reader = firstname.getReader
      val reader2 = gender.getReader
      for (i <- 0 until firstname.getValueCount) {
        reader.setPosition(i)
        reader2.setPosition(i)
        if (!reader.readText().equals(sharma) || reader2.readText().equals(male)) flag.setNull(i)
      }
      flag.getNullCount != flag.getValueCount
    }.map{ vector => // TODO close?
      val flag = vector.getVector(0)
      flag.getValueCount - flag.getNullCount
    }.sum
    println("filterByMultiColumn time: ", System.currentTimeMillis() - startTime)
    println(result)
  }

}

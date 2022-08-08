import org.grapheco.lynx.LynxType
import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.LynxString
import org.junit.{Before, Test}

class DataFrameTest {
  var nodes: DataFrame = _
  var rels: DataFrame = _

  @Before
  def loadData(): Unit ={
    val stringType = LynxString("").lynxType
    this.nodes = new DataFrame{
      override def schema: Seq[(String, LynxType)] = Seq(
        ("creationDate", stringType),
        ("id", stringType),
        ("label", stringType),
        ("firstName", stringType),
        ("lastName", stringType),
        ("gender", stringType),
        ("birthday", stringType),
        ("locationIP", stringType),
        ("browserUsed", stringType),
        ("emails", stringType),
        ("languages", stringType)
      )

      override def records: Iterator[Seq[LynxValue]] = IO.readCSV(IO.nodesFile1).map(_.map(LynxString))
    }

    this.rels  = new DataFrame {
      override def schema: Seq[(String, LynxType)] = Seq(
        ("relId", stringType),
        ("tyoe", stringType),
        ("creationDate", stringType),
        ("startId", stringType),
        ("endId", stringType)
      )

      override def records: Iterator[Seq[LynxValue]] = IO.readCSV(IO.relsFile1).map(_.map(LynxString))
    }
  }


  /*
    filter By firstname = Ali
    return count(id)
   */
  @Test
  def filterByOneColumn(): Unit = {
    val startTime = System.currentTimeMillis()
    val firstname = nodes.schema.map(_._1).indexOf("firstName")
    val id = nodes.schema.map(_._1).indexOf("id")
    val result = nodes.records.filter{ r =>
      r(firstname) == LynxString("Ali")
    }.map{r => Seq(r(id))
    }.count(_ => true)
    println("filterByOneColumn time: ", System.currentTimeMillis() - startTime)
    println(result)
  }

  /*
   filter By lastname = Sharma and gender = male
   return count(id)
  */
  @Test
  def filterByMultiColumn(): Unit = {
    val startTime = System.currentTimeMillis()
    val lastname = nodes.schema.map(_._1).indexOf("lastName")
    val gender = nodes.schema.map(_._1).indexOf("gender")
    val id = nodes.schema.map(_._1).indexOf("id")
    val result = nodes.records.filter{ r =>
      r(lastname) == LynxString("Sharma") &&
        r(gender) == LynxString("male")
    }.map{r => Seq(r(id))
    }.count(_ => true)
    println("filterByOneColumn time: ", System.currentTimeMillis() - startTime)
    println(result)
  }
}

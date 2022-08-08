import com.github.tototoshi.csv.{CSVReader, DefaultCSVFormat}

import java.io.File

object IO {
  val nodesFile = new File("/Users/huchuan/Documents/GitHub/dataframe-arrow/src/main/resources/person-output.csv")
  val relsFile = new File("/Users/huchuan/Documents/GitHub/dataframe-arrow/src/main/resources/person_knows_person-output.csv")
  val nodesFile1 = new File("/Users/huchuan/Documents/GitHub/dataframe-arrow/src/main/resources/ldbc1/person-output.csv")
  val relsFile1 = new File("/Users/huchuan/Documents/GitHub/dataframe-arrow/src/main/resources/ldbc1/person_knows_person-output.csv")
  val nodesFile10 = new File("/Users/huchuan/Documents/GitHub/dataframe-arrow/src/main/resources/ldbc10/person-output.csv")
  val relsFile10 = new File("/Users/huchuan/Documents/GitHub/dataframe-arrow/src/main/resources/ldbc10/person_knows_person-output.csv")

  val CSV_FORMAT: DefaultCSVFormat = new DefaultCSVFormat{
    override val delimiter: Char = '|'
  }

  def readCSV(f: File): Iterator[Seq[String]] = CSVReader.open(f)(CSV_FORMAT).iterator


}

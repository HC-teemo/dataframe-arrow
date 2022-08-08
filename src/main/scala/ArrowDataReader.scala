import IO.{CSV_FORMAT, nodesFile, readCSV, relsFile}
import org.apache.arrow.dataset.file.{FileFormat, FileSystemDatasetFactory}
import org.apache.arrow.dataset.jni.NativeMemoryPool
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.{BitVector, DateDayVector, FieldVector, VarCharVector, VectorSchemaRoot}
import org.apache.arrow.vector.complex.impl.{BigIntWriterImpl, DateDayHolderReaderImpl}
import org.apache.arrow.vector.holders.DateDayHolder
import org.apache.arrow.vector.types.DateUnit
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.arrow.vector.util.Text

import java.io.File
import scala.collection.JavaConverters._
import java.util.Arrays.{asList, fill}
import scala.::

object ArrowDataReader {

  val SIZE = 10

  def nodes(f :File, size: Int = SIZE): Iterator[VectorSchemaRoot] = {
    val data = readCSV(f)
    val head = data.next()
    val rootAllocator = new RootAllocator(Int.MaxValue)
    new Iterator[VectorSchemaRoot] {
      override def hasNext: Boolean = data.hasNext

      override def next(): VectorSchemaRoot = {
        var i = 0
//        val creationDate = new DateDayVector("creationDate", rootAllocator)
        val creationDate = new VarCharVector("creationDate", rootAllocator)
        val id = new VarCharVector("id", rootAllocator)
        val label = new VarCharVector("label", rootAllocator)
        val firstname = new VarCharVector("firstname", rootAllocator)
        val lastname = new VarCharVector("lastname", rootAllocator)
        val gender = new VarCharVector("gender", rootAllocator)
        val birthday = new VarCharVector("birthday", rootAllocator)
        val locationIP = new VarCharVector("locationIP", rootAllocator)
        val browserUsed = new VarCharVector("browserUsed", rootAllocator)
//        val email = new ListVector("email", rootAllocator, FieldType.nullable(ArrowType.Utf8), null)
//        val languages = new ListVector("email", rootAllocator, FieldType.nullable(ArrowType.Utf8), null)
        val email = new VarCharVector("email", rootAllocator)
        val languages = new VarCharVector("email", rootAllocator)
        val vectors: List[VarCharVector] = List(
          creationDate, id, label, firstname, lastname, gender, birthday, locationIP, browserUsed, email, languages
        )
        vectors.foreach{v => v.allocateNew(SIZE)}
        while (data.hasNext && i < SIZE) {
          val line = data.next()
          if (line.size != vectors.size) throw new Exception("size error")
          line.zipWithIndex.foreach{ case (str, index) =>
            vectors(index).set(i, new Text(str))
          }
          i += 1
        }
        vectors.foreach(_.setValueCount(i))
        val flag = new BitVector("flag", rootAllocator)
        flag.allocateNew(SIZE)
        flag.setRangeToOne(0, SIZE)
        flag.setValueCount(SIZE)
        // sb
        val vectorSB: List[FieldVector] = vectors.+:(flag)
        new VectorSchemaRoot(vectors.+:(flag).map(_.getField).asJava, vectorSB.asJava, i)
      }
    }
  }

  def relationships(f :File, size: Int = SIZE): Iterator[VectorSchemaRoot] = {
    val data = readCSV(f)
    val head = data.next()
    val rootAllocator = new RootAllocator(Int.MaxValue)
    new Iterator[VectorSchemaRoot] {
      override def hasNext: Boolean = data.hasNext

      override def next(): VectorSchemaRoot = {
        var i = 0
        val relId = new VarCharVector("relId", rootAllocator)
        val t = new VarCharVector("type", rootAllocator)
        val creationDate = new VarCharVector("creationDate", rootAllocator)
        val startId = new VarCharVector("startId", rootAllocator)
        val endId = new VarCharVector("endId", rootAllocator)
        val vectors: List[VarCharVector] = List(
          relId, t, creationDate, startId, endId
        )
        vectors.foreach{v => v.allocateNew()}
        while (data.hasNext && i < SIZE) {
          val line = data.next()
          if (line.size != vectors.size) throw new Exception("size error")
          line.zipWithIndex.foreach{ case (str, index) =>
            vectors(index).set(i, new Text(str))
          }
          i += 1
        }
        vectors.foreach(_.setValueCount(i))
        // sb
        val vectorSB: List[FieldVector] = vectors
        new VectorSchemaRoot(vectors.map(_.getField).asJava, vectorSB.asJava, i)
      }
    }
  }

  def getNodeSchema: Schema = {
    // creationDate:Date
    // id:ID
    // :LABEL
    // firstName
    // lastName
    // gender
    // birthday
    // locationIP
    // browserUsed
    // emails:string[]
    // languages:string[]
    val creationDate = new Field("creationDate",
      FieldType.nullable(new ArrowType.Date(DateUnit.DAY)), /*children*/ null)
    val id = new Field("id",
      FieldType.nullable(new ArrowType.Utf8), /*children*/ null)
    val label = new Field("label",
      FieldType.nullable(new ArrowType.Utf8), /*children*/ null)
    val firstName = new Field("firstName",
      FieldType.nullable(new ArrowType.Utf8), /*children*/ null)
    val lastname = new Field("lastname",
      FieldType.nullable(new ArrowType.Utf8), /*children*/ null)
    val gender = new Field("gender",
      FieldType.nullable(new ArrowType.Utf8), /*children*/ null)
    val birthday = new Field("birthday",
      FieldType.nullable(new ArrowType.Utf8), /*children*/ null)
    val locationIP = new Field("locationIP",
      FieldType.nullable(new ArrowType.Utf8), /*children*/ null)
    val browserUsed = new Field("browserUsed",
      FieldType.nullable(new ArrowType.Utf8), /*children*/ null)
    val emails = new Field("emails",
      FieldType.nullable(new ArrowType.List), /*children*/ null)
    val languages = new Field("languages",
      FieldType.nullable(new ArrowType.List), /*children*/ null)

    new Schema(asList(creationDate, id, label, firstName, lastname, gender, birthday, locationIP, browserUsed, emails, languages))
  }

  def getRelSchema: Schema = {
    // REL_ID:IGNORE
    // :TYPE
    // creationDate:Date
    // :START_ID
    // :END_ID
    val relId = new Field("relId",
      FieldType.nullable(new ArrowType.Utf8), /*children*/ null)
    val t = new Field("type",
      FieldType.nullable(new ArrowType.Utf8), /*children*/ null)
    val creationDate = new Field("creationDate",
      FieldType.nullable(new ArrowType.Date(DateUnit.DAY)), /*children*/ null)
    val startId = new Field("startId",
      FieldType.nullable(new ArrowType.Utf8), /*children*/ null)
    val endId = new Field("endId",
      FieldType.nullable(new ArrowType.Utf8), /*children*/ null)
    new Schema(asList(relId, t, creationDate, startId, endId))
  }
}
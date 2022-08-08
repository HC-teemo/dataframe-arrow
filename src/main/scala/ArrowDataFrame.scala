import org.grapheco.lynx.LynxType
import org.grapheco.lynx.dataframe.{DataFrame, DataFrameOperator, JoinType}
import org.grapheco.lynx.evaluator.ExpressionContext
import org.grapheco.lynx.types.LynxValue
import org.opencypher.v9_0.expressions.Expression

class ArrowDataFrame extends DataFrame{
  override def schema: Seq[(String, LynxType)] = ???

  override def records: Iterator[Seq[LynxValue]] = ???
}

class ArrowDataFrameOpt extends DataFrameOperator {
  override def select(df: DataFrame, columns: Seq[(String, Option[String])]): DataFrame = ???

  override def filter(df: DataFrame, predicate: Seq[LynxValue] => Boolean)(ctx: ExpressionContext): DataFrame = ???

  override def project(df: DataFrame, columns: Seq[(String, Expression)])(ctx: ExpressionContext): DataFrame = ???

  override def groupBy(df: DataFrame, groupings: Seq[(String, Expression)], aggregations: Seq[(String, Expression)])(ctx: ExpressionContext): DataFrame = ???

  override def skip(df: DataFrame, num: Int): DataFrame = ???

  override def take(df: DataFrame, num: Int): DataFrame = ???

  override def join(a: DataFrame, b: DataFrame, joinColumn: Seq[String], joinType: JoinType): DataFrame = ???

  override def distinct(df: DataFrame): DataFrame = ???

  override def orderBy(df: DataFrame, sortItem: Seq[(Expression, Boolean)])(ctx: ExpressionContext): DataFrame = ???
}
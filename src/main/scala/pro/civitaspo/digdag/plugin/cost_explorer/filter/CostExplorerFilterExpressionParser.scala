package pro.civitaspo.digdag.plugin.cost_explorer.filter
import com.amazonaws.services.costexplorer
import com.amazonaws.services.costexplorer.model.{Dimension, DimensionValues, TagValues}
import io.digdag.client.config.ConfigException
import net.sf.jsqlparser.expression
import net.sf.jsqlparser.expression.{
  AllComparisonExpression,
  AnalyticExpression,
  AnyComparisonExpression,
  CaseExpression,
  CastExpression,
  DateTimeLiteralExpression,
  DateValue,
  DoubleValue,
  Expression,
  ExpressionVisitorAdapter,
  ExtractExpression,
  HexValue,
  IntervalExpression,
  JdbcNamedParameter,
  JdbcParameter,
  JsonExpression,
  KeepExpression,
  LongValue,
  MySQLGroupConcat,
  NotExpression,
  NumericBind,
  OracleHierarchicalExpression,
  OracleHint,
  Parenthesis,
  RowConstructor,
  SignedExpression,
  StringValue,
  TimeKeyExpression,
  TimestampValue,
  TimeValue,
  UserVariable,
  ValueListExpression,
  WhenClause
}
import net.sf.jsqlparser.expression.operators.arithmetic.{
  Addition,
  BitwiseAnd,
  BitwiseLeftShift,
  BitwiseOr,
  BitwiseRightShift,
  BitwiseXor,
  Concat,
  Division,
  Modulo,
  Multiplication,
  Subtraction
}
import net.sf.jsqlparser.expression.operators.conditional.{AndExpression, OrExpression}
import net.sf.jsqlparser.expression.operators.relational.{
  Between,
  EqualsTo,
  ExistsExpression,
  ExpressionList,
  GreaterThan,
  GreaterThanEquals,
  InExpression,
  IsNullExpression,
  ItemsListVisitorAdapter,
  JsonOperator,
  LikeExpression,
  Matches,
  MinorThan,
  MinorThanEquals,
  MultiExpressionList,
  NotEqualsTo,
  RegExpMatchOperator,
  RegExpMySQLOperator}
import net.sf.jsqlparser.parser.CCJSqlParserUtil.parseCondExpression
import net.sf.jsqlparser.schema.Column
import net.sf.jsqlparser.statement.select.{AllColumns, AllTableColumns, Pivot, PivotXml, SelectExpressionItem, SubSelect}

import scala.collection.JavaConverters._
import scala.util.Try
import scala.util.matching.Regex

trait TagValuesGetter {
  def getTagValues(key: String, filter: Regex): Seq[String]
}

trait DimensionValuesGetter {
  def getDimensionValues(key: String, filter: Regex): Seq[String]
}

object LikeRegexConverter {
  private val meta: String = "[](){}.*+?$^|#\\"

  def convert(s: String): Regex = {
    quoteMeta(s)
      .replaceAll("""(?<!\\)_""", ".")
      .replaceAll("""(?<!\\)%""", ".*")
      .r
  }

  private def quoteMeta(s: String): String = {
    val sb = new StringBuilder
    s.iterator.foreach { char =>
      if (meta.contains(char)) sb.append("\\")
      sb.append(char)
    }
    sb.result()
  }
}

object CostExplorerFilterExpressionParser {

  def apply(tagValuesGetter: TagValuesGetter, dimensionValuesGetter: DimensionValuesGetter): CostExplorerFilterExpressionParser =
    new CostExplorerFilterExpressionParser(tagValuesGetter, dimensionValuesGetter)
}

/*
NOTE: The below code includes two expr, so be careful not to get confused.
        1. net.sf.jsqlparser.expression.Expression
        2. com.amazonaws.services.costexplorer.model.Expression
      Here, I express 1 as `expr`, 2 as `ceExpr`.
 */
class CostExplorerFilterExpressionParser(tagValuesGetter: TagValuesGetter, dimensionValuesGetter: DimensionValuesGetter) {

  def parse(filterExpr: String): costexplorer.model.Expression = {
    val ceExpr = new costexplorer.model.Expression
    parseCondExpression(filterExpr).accept(new CostExplorerFilterExpressionVisitor(ceExpr))
    ceExpr
  }

  private class CostExplorerFilterExpressionVisitor(ceExpr: costexplorer.model.Expression = new costexplorer.model.Expression)
      extends ExpressionVisitorAdapter {

    override def visit(expr: EqualsTo): Unit = {
      val k: String = stringifyLeftExpression(expr.getLeftExpression)
      if (isDimension(k)) {
        ceExpr.setDimensions(new DimensionValues().withKey(k).withValues(stringifyRightExpression(expr.getRightExpression)))
      }
      else {
        ceExpr.setTags(new TagValues().withKey(k).withValues(stringifyRightExpression(expr.getRightExpression)))
      }
    }

    override def visit(expr: LikeExpression): Unit = {
      val k: String = stringifyLeftExpression(expr.getLeftExpression)
      val likeRegex: Regex = LikeRegexConverter.convert(stringifyRightExpression(expr.getRightExpression))
      if (isDimension(k)) {
        setNotOr(expr.isNot) { newOrCEExpr =>
          val vals: Seq[String] = dimensionValuesGetter.getDimensionValues(k, likeRegex)
          val dimVals: DimensionValues = new DimensionValues().withKey(k).withValues(vals: _*)
          newOrCEExpr.setDimensions(dimVals)
        }
      }
      else {
        setNotOr(expr.isNot) { newOrCEExpr =>
          val vals: Seq[String] = tagValuesGetter.getTagValues(k, likeRegex)
          val tagVals: TagValues = new TagValues().withKey(k).withValues(vals: _*)
          newOrCEExpr.setTags(tagVals)
        }
      }
    }

    override def visit(parenthesis: Parenthesis): Unit = {
      setNotOr(parenthesis.isNot) { newOrCEExpr => parenthesis.getExpression.accept(new CostExplorerFilterExpressionVisitor(ceExpr = newOrCEExpr))
      }
    }

    override def visit(expr: AndExpression): Unit = {
      ceExpr.setAnd(seqAsJavaList(Seq(withNewCEExpr { newCEExpr => expr.getLeftExpression.accept(new CostExplorerFilterExpressionVisitor(ceExpr = newCEExpr))
      }, withNewCEExpr { newCEExpr => expr.getRightExpression.accept(new CostExplorerFilterExpressionVisitor(ceExpr = newCEExpr))
      })))
    }

    override def visit(expr: OrExpression): Unit = {
      ceExpr.setOr(seqAsJavaList(Seq(withNewCEExpr { newCEExpr => expr.getLeftExpression.accept(new CostExplorerFilterExpressionVisitor(ceExpr = newCEExpr))
      }, withNewCEExpr { newCEExpr => expr.getRightExpression.accept(new CostExplorerFilterExpressionVisitor(ceExpr = newCEExpr))
      })))
    }

    override def visit(expr: NotEqualsTo): Unit = {
      val k: String = stringifyLeftExpression(expr.getLeftExpression)
      if (isDimension(k)) {
        ceExpr.setNot(
          withNewCEExpr { newCEExpr => newCEExpr.setDimensions(new DimensionValues().withKey(k).withValues(stringifyRightExpression(expr.getRightExpression)))
        })
      }
      else {
        ceExpr.setNot(withNewCEExpr { newCEExpr => newCEExpr.setTags(new TagValues().withKey(k).withValues(stringifyRightExpression(expr.getRightExpression)))
        })
      }
    }

    private def isDimension(s: String): Boolean = {
      Try(Dimension.fromValue(s)).isSuccess
    }

    override def visit(expr: InExpression): Unit = {
      val k: String = stringifyLeftExpression(expr.getLeftExpression)
      if (isDimension(k)) {
        expr.getRightItemsList.accept(new ItemsListVisitorAdapter {
          override def visit(expressionList: ExpressionList): Unit = {
            setNotOr(expr.isNot) { newOrCEExpr =>
              val dimVals: DimensionValues = new DimensionValues().withKey(k).withValues(expressionList.getExpressions.asScala.map(stringifyRightExpression): _*)
              newOrCEExpr.setDimensions(dimVals)
            }
          }
        })
      }
      else {
        expr.getRightItemsList.accept(new ItemsListVisitorAdapter {
          override def visit(expressionList: ExpressionList): Unit = {
            setNotOr(expr.isNot) { newOrCEExpr =>
              val tagVals: TagValues = new TagValues().withKey(k).withValues(expressionList.getExpressions.asScala.map(stringifyRightExpression): _*)
              newOrCEExpr.setTags(tagVals)
            }
          }
        })
      }
    }

    private def setNotOr(isNot: Boolean)(f: costexplorer.model.Expression => Unit) = {
      if (isNot) ceExpr.setNot(withNewCEExpr(f)) else f(ceExpr)
    }

    private def withNewCEExpr(f: costexplorer.model.Expression => Unit): costexplorer.model.Expression = {
      val newCEExpr = new costexplorer.model.Expression
      f(newCEExpr)
      newCEExpr
    }

    private def stringifyRightExpression(right: Expression): String = {
      val s = right.toString
      if (!s.matches("""\A'.*'\z""")) throw new ConfigException("Values must be surrounded by single quotes.")
      s.replaceAll("""\A'""", "")
        .replaceAll("""'\z""", "")
    }

    private def stringifyLeftExpression(left: Expression): String = {
      val s = left.toString
      if (!s.matches("""\A".*"\z""")) throw new ConfigException("Values must be surrounded by double quotes.")
      s.replaceAll("""\A"""", "")
        .replaceAll(""""\z""", "")
    }

    override def visit(function: expression.Function): Unit = throw new UnsupportedOperationException(s"Unsupported: ${function.toString}")
    override def visit(expr: SignedExpression): Unit = throw new UnsupportedOperationException(s"Unsupported: ${expr.toString}")
    override def visit(parameter: JdbcParameter): Unit = throw new UnsupportedOperationException(s"Unsupported: ${parameter.toString}")
    override def visit(parameter: JdbcNamedParameter): Unit = throw new UnsupportedOperationException(s"Unsupported: ${parameter.toString}")
    override def visit(value: DoubleValue): Unit = throw new UnsupportedOperationException(s"Unsupported: ${value.toString}")
    override def visit(value: LongValue): Unit = throw new UnsupportedOperationException(s"Unsupported: ${value.toString}")
    override def visit(value: DateValue): Unit = throw new UnsupportedOperationException(s"Unsupported: ${value.toString}")
    override def visit(value: TimeValue): Unit = throw new UnsupportedOperationException(s"Unsupported: ${value.toString}")
    override def visit(value: TimestampValue): Unit = throw new UnsupportedOperationException(s"Unsupported: ${value.toString}")
    override def visit(value: StringValue): Unit = throw new UnsupportedOperationException(s"Unsupported: ${value.toString}")
    override def visit(expr: Addition): Unit = throw new UnsupportedOperationException(s"Unsupported: ${expr.toString}")
    override def visit(expr: Division): Unit = throw new UnsupportedOperationException(s"Unsupported: ${expr.toString}")
    override def visit(expr: Multiplication): Unit = throw new UnsupportedOperationException(s"Unsupported: ${expr.toString}")
    override def visit(expr: Subtraction): Unit = throw new UnsupportedOperationException(s"Unsupported: ${expr.toString}")
    override def visit(expr: Between): Unit = throw new UnsupportedOperationException(s"Unsupported: ${expr.toString}")
    override def visit(expr: GreaterThan): Unit = throw new UnsupportedOperationException(s"Unsupported: ${expr.toString}")
    override def visit(expr: GreaterThanEquals): Unit = throw new UnsupportedOperationException(s"Unsupported: ${expr.toString}")
    override def visit(expr: IsNullExpression): Unit = throw new UnsupportedOperationException(s"Unsupported: ${expr.toString}")
    override def visit(expr: MinorThan): Unit = throw new UnsupportedOperationException(s"Unsupported: ${expr.toString}")
    override def visit(expr: MinorThanEquals): Unit = throw new UnsupportedOperationException(s"Unsupported: ${expr.toString}")
    override def visit(column: Column): Unit = throw new UnsupportedOperationException(s"Unsupported: ${column.toString}")
    override def visit(subSelect: SubSelect): Unit = throw new UnsupportedOperationException(s"Unsupported: ${subSelect.toString}")
    override def visit(expr: CaseExpression): Unit = throw new UnsupportedOperationException(s"Unsupported: ${expr.toString}")
    override def visit(expr: WhenClause): Unit = throw new UnsupportedOperationException(s"Unsupported: ${expr.toString}")
    override def visit(expr: ExistsExpression): Unit = throw new UnsupportedOperationException(s"Unsupported: ${expr.toString}")
    override def visit(expr: AllComparisonExpression): Unit = throw new UnsupportedOperationException(s"Unsupported: ${expr.toString}")
    override def visit(expr: AnyComparisonExpression): Unit = throw new UnsupportedOperationException(s"Unsupported: ${expr.toString}")
    override def visit(expr: Concat): Unit = throw new UnsupportedOperationException(s"Unsupported: ${expr.toString}")
    override def visit(expr: Matches): Unit = throw new UnsupportedOperationException(s"Unsupported: ${expr.toString}")
    override def visit(expr: BitwiseAnd): Unit = throw new UnsupportedOperationException(s"Unsupported: ${expr.toString}")
    override def visit(expr: BitwiseOr): Unit = throw new UnsupportedOperationException(s"Unsupported: ${expr.toString}")
    override def visit(expr: BitwiseXor): Unit = throw new UnsupportedOperationException(s"Unsupported: ${expr.toString}")
    override def visit(expr: CastExpression): Unit = throw new UnsupportedOperationException(s"Unsupported: ${expr.toString}")
    override def visit(expr: Modulo): Unit = throw new UnsupportedOperationException(s"Unsupported: ${expr.toString}")
    override def visit(expr: AnalyticExpression): Unit = throw new UnsupportedOperationException(s"Unsupported: ${expr.toString}")
    override def visit(expr: ExtractExpression): Unit = throw new UnsupportedOperationException(s"Unsupported: ${expr.toString}")
    override def visit(expr: IntervalExpression): Unit = throw new UnsupportedOperationException(s"Unsupported: ${expr.toString}")
    override def visit(expr: OracleHierarchicalExpression): Unit = throw new UnsupportedOperationException(s"Unsupported: ${expr.toString}")
    override def visit(expr: RegExpMatchOperator): Unit = throw new UnsupportedOperationException(s"Unsupported: ${expr.toString}")
    override def visit(expressionList: ExpressionList): Unit = throw new UnsupportedOperationException(s"Unsupported: ${expressionList.toString}")
    override def visit(multiExprList: MultiExpressionList): Unit = throw new UnsupportedOperationException(s"Unsupported: ${multiExprList.toString}")
    override def visit(notExpr: NotExpression): Unit = throw new UnsupportedOperationException(s"Unsupported: ${notExpr.toString}")
    override def visit(expr: BitwiseRightShift): Unit = throw new UnsupportedOperationException(s"Unsupported: ${expr.toString}")
    override def visit(expr: BitwiseLeftShift): Unit = throw new UnsupportedOperationException(s"Unsupported: ${expr.toString}")
    override def visit(jsonExpr: JsonExpression): Unit = throw new UnsupportedOperationException(s"Unsupported: ${jsonExpr.toString}")
    override def visit(expr: JsonOperator): Unit = throw new UnsupportedOperationException(s"Unsupported: ${expr.toString}")
    override def visit(expr: RegExpMySQLOperator): Unit = throw new UnsupportedOperationException(s"Unsupported: ${expr.toString}")
    override def visit(`var`: UserVariable): Unit = throw new UnsupportedOperationException(s"Unsupported: ${`var`.toString}")
    override def visit(bind: NumericBind): Unit = throw new UnsupportedOperationException(s"Unsupported: ${bind.toString}")
    override def visit(expr: KeepExpression): Unit = throw new UnsupportedOperationException(s"Unsupported: ${expr.toString}")
    override def visit(groupConcat: MySQLGroupConcat): Unit = throw new UnsupportedOperationException(s"Unsupported: ${groupConcat.toString}")
    override def visit(valueListExpression: ValueListExpression): Unit = throw new UnsupportedOperationException(s"Unsupported: ${valueListExpression.toString}")
    override def visit(pivot: Pivot): Unit = throw new UnsupportedOperationException(s"Unsupported: ${pivot.toString}")
    override def visit(pivot: PivotXml): Unit = throw new UnsupportedOperationException(s"Unsupported: ${pivot.toString}")
    override def visit(allColumns: AllColumns): Unit = throw new UnsupportedOperationException(s"Unsupported: ${allColumns.toString}")
    override def visit(allTableColumns: AllTableColumns): Unit = throw new UnsupportedOperationException(s"Unsupported: ${allTableColumns.toString}")
    override def visit(selectExpressionItem: SelectExpressionItem): Unit =
      throw new UnsupportedOperationException(s"Unsupported: ${selectExpressionItem.toString}")
    override def visit(rowConstructor: RowConstructor): Unit = throw new UnsupportedOperationException(s"Unsupported: ${rowConstructor.toString}")
    override def visit(hexValue: HexValue): Unit = throw new UnsupportedOperationException(s"Unsupported: ${hexValue.toString}")
    override def visit(hint: OracleHint): Unit = throw new UnsupportedOperationException(s"Unsupported: ${hint.toString}")
    override def visit(timeKeyExpression: TimeKeyExpression): Unit = throw new UnsupportedOperationException(s"Unsupported: ${timeKeyExpression.toString}")
    override def visit(literal: DateTimeLiteralExpression): Unit = throw new UnsupportedOperationException(s"Unsupported: ${literal.toString}")
  }
}

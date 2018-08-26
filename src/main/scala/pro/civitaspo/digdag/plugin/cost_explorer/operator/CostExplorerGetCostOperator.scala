package pro.civitaspo.digdag.plugin.cost_explorer.operator

import java.time.LocalDate

import com.amazonaws.services.costexplorer.model.{Context, DateInterval, GetDimensionValuesRequest, GetTagsRequest, Granularity}
import com.amazonaws.services.costexplorer.model.Granularity.DAILY
import com.google.common.base.Optional
import io.digdag.client.config.{Config, ConfigException}
import io.digdag.spi.{OperatorContext, TaskResult, TemplateEngine}
import pro.civitaspo.digdag.plugin.cost_explorer.filter.{CostExplorerFilterExpressionParser, DimensionValuesGetter, TagValuesGetter}

import scala.collection.JavaConverters._
import scala.util.matching.Regex

class CostExplorerGetCostOperator (operatorName: String, context: OperatorContext, systemConfig: Config, templateEngine: TemplateEngine)
  extends AbstractCostExplorerOperator(operatorName, context, systemConfig, templateEngine) {

  val filter: Optional[String] = params.getOptional("filter", classOf[String])
  val granularity: Granularity = params.get("granularity", classOf[Granularity], DAILY)
  val groupBy: Optional[String] = params.getOptional("group_by", classOf[String])
  val metrics: Seq[String] = {
    val m = params.getListOrEmpty("metrics", classOf[String]).asScala
    if (!m.forall(_m => Seq("AmortizedCost", "BlendedCost", "UnblendedCost", "UsageQuantity").exists(_.equals(_m)))) {
      throw new ConfigException(
        s"""[${operatorName}] metrics must be some of AmortizedCost, BlendedCost, UnblendedCost and UsageQuantity. Input values: ${m.mkString(",")} """)
    }
    m.groupBy(identity).mapValues { v =>
      if (v.size != 1) logger.warn(s"[${operatorName}] `${v.head}` is duplicated: ${v.size}")
    }
    if (m.nonEmpty) m.distinct else Seq("UnblendedCost")
  }
  val startDate: String = {
    val sd = params.get("start_date", classOf[String], LocalDate.now().minusDays(1L).toString)
    if (!sd.matches("""\A\d{4}-\d{2}-\d{2}\z""")) throw new ConfigException(s"""[${operatorName}] "start_date" must be `\d{4}-\d{2}-\d{2}`: $sd""")
    sd
  }
  val endDate: String = {
    val ed = params.get("end_date", classOf[String], startDate)
    if (!ed.matches("""\A\d{4}-\d{2}-\d{2}\z""")) throw new ConfigException(s"""[${operatorName}] "start_date" must be `\d{4}-\d{2}-\d{2}`: $ed""") throw new ConfigException(s"""[${operatorName}] "end_date" must be `\d{4}-\d{2}-\d{2}`: $ed""")
    ed
  }

  lazy val timePeriod: DateInterval = new DateInterval().withStart(startDate).withEnd(endDate)
  lazy val filterParser =
    CostExplorerFilterExpressionParser(dimensionValuesGetter = new DimensionValuesGetter {

      def get(key: String, filter: Regex, nextPageToken: Option[String] = None): Seq[String] = {
        val req = new GetDimensionValuesRequest()
          .withContext(Context.COST_AND_USAGE)
          .withDimension(key)
          .withTimePeriod(timePeriod)

        if (nextPageToken.isDefined) req.setNextPageToken(nextPageToken.get)

        val builder = Seq.newBuilder[String]
        val res = withAWSCostExplorer(_.getDimensionValues(req))
        // NOTE: Is there the case that dimVal.getAttributes is required?
        res.getDimensionValues.asScala.map(_.getValue).filter(_.matches(filter.regex)).foreach(builder += _)
        Option(res.getNextPageToken) match {
          case Some(t) => get(key, filter, Option(t)).foreach(builder += _)
          case None => // do nothing
        }

        builder.result()
      }

      override def getDimensionValues(key: String, filter: Regex): Seq[String] = get(key, filter)
    }, tagValuesGetter = new TagValuesGetter {

      def get(key: String, filter: Regex, nextPageToken: Option[String] = None): Seq[String] = {
        val req = new GetTagsRequest()
          .withTagKey(key)
          .withTimePeriod(timePeriod)

        if (nextPageToken.isDefined) req.setNextPageToken(nextPageToken.get)

        val builder = Seq.newBuilder[String]
        val res = withAWSCostExplorer(_.getTags(req))
        res.getTags.asScala.filter(_.matches(filter.regex)).foreach(builder += _)
        Option(res.getNextPageToken) match {
          case Some(t) => get(key, filter, Option(t)).foreach(builder += _)
          case None => // do nothing
        }

        builder.result()
      }
      override def getTagValues(key: String, filter: Regex): Seq[String] = get(key, filter)
    })


  override def runTask(): TaskResult = {
    TaskResult.empty(request)
  }


}

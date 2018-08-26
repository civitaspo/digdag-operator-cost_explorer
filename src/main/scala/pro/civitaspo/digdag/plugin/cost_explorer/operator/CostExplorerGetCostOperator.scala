package pro.civitaspo.digdag.plugin.cost_explorer.operator

import java.util.{Map => JMap}
import java.time.LocalDate

import com.amazonaws.services.costexplorer.model.{Context, DateInterval, Dimension, GetCostAndUsageRequest, GetDimensionValuesRequest, GetTagsRequest, Granularity, GroupDefinition, GroupDefinitionType, MetricValue, ResultByTime}
import com.amazonaws.services.costexplorer.model.Granularity.DAILY
import com.amazonaws.services.costexplorer.model.GroupDefinitionType.{DIMENSION, TAG}
import com.google.common.base.Optional
import com.google.common.collect.ImmutableList
import io.digdag.client.config.{Config, ConfigException, ConfigKey}
import io.digdag.spi.{OperatorContext, TaskResult, TemplateEngine}
import pro.civitaspo.digdag.plugin.cost_explorer.filter.{CostExplorerFilterExpressionParser, DimensionValuesGetter, TagValuesGetter}

import scala.collection.JavaConverters._
import scala.util.Try
import scala.util.matching.Regex

class CostExplorerGetCostOperator (operatorName: String, context: OperatorContext, systemConfig: Config, templateEngine: TemplateEngine)
  extends AbstractCostExplorerOperator(operatorName, context, systemConfig, templateEngine) {

  val filter: Optional[String] = params.getOptional("filter", classOf[String])
  val granularity: Granularity = params.get("granularity", classOf[Granularity], DAILY)
  val groupBy: Seq[String] = params.getListOrEmpty("group_by", classOf[String]).asScala
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
    if (!ed.matches("""\A\d{4}-\d{2}-\d{2}\z""")) throw new ConfigException(s"""[${operatorName}] "end_date" must be `\d{4}-\d{2}-\d{2}`: $ed""")
    ed
  }

  lazy val timePeriod: DateInterval = new DateInterval().withStart(startDate).withEnd(endDate)
  lazy val filterParser =
    CostExplorerFilterExpressionParser(dimensionValuesGetter = new DimensionValuesGetter {

      def getDimensionValuesRecursive(key: String, filter: Regex, nextPageToken: Option[String] = None): Seq[String] = {
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
          case Some(t) => getDimensionValuesRecursive(key, filter, Option(t)).foreach(builder += _)
          case None => // do nothing
        }

        builder.result()
      }

      override def getDimensionValues(key: String, filter: Regex): Seq[String] = getDimensionValuesRecursive(key, filter)
    }, tagValuesGetter = new TagValuesGetter {

      def getTagsRecursive(key: String, filter: Regex, nextPageToken: Option[String] = None): Seq[String] = {
        val req = new GetTagsRequest()
          .withTagKey(key)
          .withTimePeriod(timePeriod)

        if (nextPageToken.isDefined) req.setNextPageToken(nextPageToken.get)

        val builder = Seq.newBuilder[String]
        val res = withAWSCostExplorer(_.getTags(req))
        res.getTags.asScala.filter(_.matches(filter.regex)).foreach(builder += _)
        Option(res.getNextPageToken) match {
          case Some(t) => getTagsRecursive(key, filter, Option(t)).foreach(builder += _)
          case None => // do nothing
        }

        builder.result()
      }
      override def getTagValues(key: String, filter: Regex): Seq[String] = getTagsRecursive(key, filter)
    })


  override def runTask(): TaskResult = {
    val p = cf.create()
    p.getNestedOrSetEmpty("cost_explorer").getNestedOrSetEmpty("last_get_cost").set("results", seqAsJavaList(getCost))

    val builder = TaskResult.defaultBuilder(request)
    builder.storeParams(p)
    builder.resetStoreParams(ImmutableList.of(ConfigKey.of("cost_explorer", "last_get_cost")))
    builder.build()
  }

  // TODO: Refactoring
  def getCost: Seq[Config] = {
    val results: Seq[ResultByTime] = getCostAndUsageRecursive()

    val builder = Seq.newBuilder[Config]
    results.foreach { r =>
      if (r.getTotal.isEmpty) {
        r.getGroups.asScala.foreach{ g =>
          val c = cf.create()
          c.set("group_values", g.getKeys)
          val m: JMap[String, MetricValue] = g.getMetrics

          {
            val a = c.getNestedOrSetEmpty("amortized_cost")
            a.set("amount", Try(m.get("AmortizedCost").getAmount).getOrElse(Optional.absent()))
            a.set("unit", Try(m.get("AmortizedCost").getUnit).getOrElse(Optional.absent()))
          }
          {
            val a = c.getNestedOrSetEmpty("blended_cost")
            a.set("amount", Try(m.get("BlendedCost").getAmount).getOrElse(Optional.absent()))
            a.set("unit", Try(m.get("BlendedCost").getUnit).getOrElse(Optional.absent()))
          }
          {
            val a = c.getNestedOrSetEmpty("unblended_cost")
            a.set("amount", Try(m.get("UnblendedCost").getAmount).getOrElse(Optional.absent()))
            a.set("unit", Try(m.get("UnblendedCost").getUnit).getOrElse(Optional.absent()))
          }
          {
            val a = c.getNestedOrSetEmpty("usage_quantity")
            a.set("amount", Try(m.get("UsageQuantity").getAmount).getOrElse(Optional.absent()))
            a.set("unit", Try(m.get("UsageQuantity").getUnit).getOrElse(Optional.absent()))
          }

          c.set("start_date", r.getTimePeriod.getStart)
          c.set("end_date", r.getTimePeriod.getEnd)
          c.set("estimated", r.getEstimated)

          builder += c
        }
      }
      else {
        val c = cf.create()
        c.set("group_values", seqAsJavaList(Seq.empty))
        val m: JMap[String, MetricValue] = r.getTotal

        {
          val a = c.getNestedOrSetEmpty("amortized_cost")
          a.set("amount", Try(m.get("AmortizedCost").getAmount).getOrElse(Optional.absent()))
          a.set("unit", Try(m.get("AmortizedCost").getUnit).getOrElse(Optional.absent()))
        }
        {
          val a = c.getNestedOrSetEmpty("blended_cost")
          a.set("amount", Try(m.get("BlendedCost").getAmount).getOrElse(Optional.absent()))
          a.set("unit", Try(m.get("BlendedCost").getUnit).getOrElse(Optional.absent()))
        }
        {
          val a = c.getNestedOrSetEmpty("unblended_cost")
          a.set("amount", Try(m.get("UnblendedCost").getAmount).getOrElse(Optional.absent()))
          a.set("unit", Try(m.get("UnblendedCost").getUnit).getOrElse(Optional.absent()))
        }
        {
          val a = c.getNestedOrSetEmpty("usage_quantity")
          a.set("amount", Try(m.get("UsageQuantity").getAmount).getOrElse(Optional.absent()))
          a.set("unit", Try(m.get("UsageQuantity").getUnit).getOrElse(Optional.absent()))
        }

        c.set("start_date", r.getTimePeriod.getStart)
        c.set("end_date", r.getTimePeriod.getEnd)
        c.set("estimated", r.getEstimated)

        builder += c
      }
    }

    builder.result()
  }

  def getCostAndUsageRecursive(nextPageToken: Option[String] = None): Seq[ResultByTime] = {
    val req = new GetCostAndUsageRequest()
      .withGranularity(granularity)
      .withMetrics(metrics: _*)
      .withTimePeriod(timePeriod)

    if (nextPageToken.isDefined) req.setNextPageToken(nextPageToken.get)
    if (filter.isPresent) req.setFilter(filterParser.parse(filter.get))
    if (groupBy.nonEmpty) {
      val groups = groupBy.map { key =>
        new GroupDefinition()
          .withKey(key)
          .withType(if (Try(Dimension.fromValue(key)).isSuccess) DIMENSION else TAG)
      }
      req.setGroupBy(seqAsJavaList(groups))
    }

    val builder = Seq.newBuilder[ResultByTime]
    val res = withAWSCostExplorer(_.getCostAndUsage(req))
    res.getResultsByTime.asScala.foreach(builder += _)
    Option(res.getNextPageToken) match {
      case Some(t) => getCostAndUsageRecursive(Option(t)).foreach(builder += _)
      case None => // do nothing
    }

    builder.result()
  }

}

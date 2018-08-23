package pro.civitaspo.digdag.plugin.cost_explorer.operator

import io.digdag.client.config.Config
import io.digdag.spi.{OperatorContext, TaskResult, TemplateEngine}

class CostExplorerGetCostOperator (operatorName: String, context: OperatorContext, systemConfig: Config, templateEngine: TemplateEngine)
  extends AbstractCostExplorerOperator(operatorName, context, systemConfig, templateEngine) {

  override def runTask(): TaskResult = {
    TaskResult.empty(request)
  }


}

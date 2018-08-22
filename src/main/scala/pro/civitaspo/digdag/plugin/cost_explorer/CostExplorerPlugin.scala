package pro.civitaspo.digdag.plugin.cost_explorer
import java.lang.reflect.Constructor
import java.util.{Arrays => JArrays, List => JList}

import io.digdag.client.config.Config
import io.digdag.spi.{Operator, OperatorContext, OperatorFactory, OperatorProvider, Plugin, TemplateEngine}
import javax.inject.Inject
import pro.civitaspo.digdag.plugin.cost_explorer.operator.AbstractCostExplorerOperator

object CostExplorerPlugin {

  class CostExplorerOperatorProvider extends OperatorProvider {

    @Inject protected var systemConfig: Config = null
    @Inject protected var templateEngine: TemplateEngine = null

    override def get(): JList[OperatorFactory] = {
      JArrays.asList()
    }

    private def operatorFactory[T <: AbstractCostExplorerOperator](operatorName: String, klass: Class[T]): OperatorFactory = {
      new OperatorFactory {
        override def getType: String = operatorName
        override def newOperator(context: OperatorContext): Operator = {
          val constructor: Constructor[T] = klass.getConstructor(classOf[String], classOf[OperatorContext], classOf[Config], classOf[TemplateEngine])
          constructor.newInstance(operatorName, context, systemConfig, templateEngine)
        }
      }
    }

  }

}

class CostExplorerPlugin extends Plugin {
  override def getServiceProvider[T](`type`: Class[T]): Class[_ <: T] = {
    if (`type` ne classOf[OperatorProvider]) return null
    classOf[CostExplorerPlugin.CostExplorerOperatorProvider].asSubclass(`type`)
  }
}
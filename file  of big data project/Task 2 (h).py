# Databricks notebook source
         how Spark SQL’s Catalyst optimizer converts logical plans to optimized physical plans on databricks

# COMMAND ----------


tree.transform {
  case Add(Literal(c1), Literal(c2)) => Literal(c1+c2)
}

# COMMAND ----------

tree.transform {
case Add(Literal(c1), Literal(c2)) => Literal(c1+c2)
case Add(left, Literal(0)) => left
case Add(Literal(0), right) => right
}

# COMMAND ----------

object DecimalAggregates extends Rule[LogicalPlan] {

  val MAX_LONG_DIGITS = 18
  def apply(plan: LogicalPlan): LogicalPlan = {
    plan transformAllExpressions {
      case Sum(e @ DecimalType.Expression(prec, scale))
          if prec + 10 <= MAX_LONG_DIGITS =>
        MakeDecimal(Sum(UnscaledValue(e)), prec + 10, scale) }
}

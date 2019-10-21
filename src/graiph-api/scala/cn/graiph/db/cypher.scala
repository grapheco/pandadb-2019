package cn.graiph.db

import cn.graiph.{CustomPropertyProvider, ValueMatcher}
import org.neo4j.blob.utils.ReflectUtils._
import org.neo4j.blob.utils.{ContextMap, Logging}
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.{ExpressionConverters, ExtendedCommandExpr}
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{Expression => CommandExpression}
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Predicate
import org.neo4j.cypher.internal.runtime.interpreted.commands.values.KeyToken
import org.neo4j.cypher.internal.runtime.interpreted.commands.values.TokenType._
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.interpreted.{ExecutionContext, UpdateCountingQueryContext}
import org.neo4j.cypher.internal.v3_5.ast.semantics._
import org.neo4j.cypher.internal.v3_5.expressions.Expression.SemanticContext
import org.neo4j.cypher.internal.v3_5.expressions._
import org.neo4j.cypher.internal.v3_5.parser.{ExprExtensions, Expressions}
import org.neo4j.cypher.internal.v3_5.util.InputPosition
import org.neo4j.cypher.internal.v3_5.util.attribution.Id
import org.neo4j.cypher.internal.v3_5.util.symbols._
import org.neo4j.cypher.internal.v3_5.{expressions => ast}
import org.neo4j.kernel.configuration.Config
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.{Value, _}
import org.neo4j.values.virtual.VirtualValues
import org.parboiled.scala._

/**
  * Created by bluejoe on 2019/7/16.
  */
object CypherInjection extends Expressions with Logging with Touchable {
  private def AlgoNameWithThreshold: Rule1[AlgoNameWithThresholdExpr] = rule("an algorithm with threshold") {
    group(SymbolicNameString ~ optional(operator("/") ~ DoubleLiteral)) ~~>>
      ((a, b) => AlgoNameWithThresholdExpr(Some(a), b.map(_.value))) |
      group(DoubleLiteral ~ optional(operator("/") ~ SymbolicNameString)) ~~>>
        ((a, b) => AlgoNameWithThresholdExpr(b, Some(a.value)))
  }

  private def AlgoName: Rule1[AlgoNameWithThresholdExpr] = rule("an algorithm with threshold") {
    group(SymbolicNameString) ~~>>
      ((a) => AlgoNameWithThresholdExpr(Some(a), None))
  }

  logger.debug(s"injecting cypher expression extensions...");

  ExprExtensions.extendsExpr2((Expression1: Rule1[org.neo4j.cypher.internal.v3_5.expressions.Expression]) => {
    operator("->") ~~ (PropertyKeyName ~~>> (CustomPropertyExpr(_: ast.Expression, _))) ////NOTE: cypher plus
  });

  ExprExtensions.extendsExpr3((Expression2: Rule1[org.neo4j.cypher.internal.v3_5.expressions.Expression]) => {
    group(operator("~:") ~ optional(AlgoNameWithThreshold) ~~ Expression2) ~~>>
      ((a: ast.Expression, b, c) =>
        SemanticLikeExpr(a, b, c)) |
      group(operator("!:") ~ optional(AlgoNameWithThreshold) ~~ Expression2) ~~>>
        ((a: ast.Expression, b, c) =>
          SemanticUnlikeExpr(a, b, c)) |
      group(operator(":::") ~ optional(AlgoName) ~~ Expression2) ~~>>
        ((a: ast.Expression, b, c) =>
          SemanticSetCompareExpr(a, b, c)) |
      group(operator(">>:") ~ optional(AlgoNameWithThreshold) ~~ Expression2) ~~>>
        ((a: ast.Expression, b, c) =>
          SemanticContainSetExpr(a, b, c)) |
      group(operator("<<:") ~ optional(AlgoNameWithThreshold) ~~ Expression2) ~~>>
        ((a: ast.Expression, b, c) =>
          SemanticSetInExpr(a, b, c)) |
      group(operator("::") ~ optional(AlgoName) ~~ Expression2) ~~>>
        ((a: ast.Expression, b, c) =>
          SemanticCompareExpr(a, b, c)) |
      group(operator(">:") ~ optional(AlgoNameWithThreshold) ~~ Expression2) ~~>>
        ((a: ast.Expression, b, c) =>
          SemanticContainExpr(a, b, c)) |
      group(operator("<:") ~ optional(AlgoNameWithThreshold) ~~ Expression2) ~~>>
        ((a: ast.Expression, b, c) =>
          SemanticInExpr(a, b, c))
  });
}

case class AlgoNameWithThresholdExpr(algorithm: Option[String], threshold: Option[Double])(val position: InputPosition)
  extends Expression with ExtendedExpr {

  override def check(ctx: SemanticContext): SemanticCheck =
    (state: SemanticState) =>
      threshold.map(y =>
        if (y > 1.0 || y < 0.0) {
          SemanticCheckResult.error(state, SemanticError(
            s"wrong threshold value: ${y}, should be in [0,1]", position))
        }
        else {
          SemanticCheckResult.success(state)
        }
      ).getOrElse(SemanticCheckResult.success(state))
}

case class CustomPropertyExpr(map: Expression, propertyKey: PropertyKeyName)(val position: InputPosition)
  extends LogicalProperty with ExtendedExpr with ExtendedCommandExpr {
  override def asCanonicalStringVal = s"${map.asCanonicalStringVal}.${propertyKey.asCanonicalStringVal}"

  override def makeCommand(id: Id, self: ExpressionConverters): CommandExpression =
    CustomPropertyCommand(self.toCommandExpression(id, map), PropertyKey(this.propertyKey.name))

  override def check(ctx: SemanticContext): SemanticCheck =
    SemanticExpressionCheck.check(ctx, this.map) chain
      SemanticExpressionCheck.expectType(CTAny.covariant, this.map) chain //NOTE: enable property of blob property
      SemanticExpressionCheck.specifyType(CTAny.covariant, this)
}

case class SemanticLikeExpr(lhs: Expression, ant: Option[AlgoNameWithThresholdExpr], rhs: Expression)(val position: InputPosition)
  extends Expression with BinaryOperatorExpression with ExtendedExpr with ExtendedCommandExpr {
  override val signatures = Vector(
    TypeSignature(argumentTypes = Vector(CTAny, CTAny), outputType = CTBoolean)
  )

  override def canonicalOperatorSymbol = this.getClass.getSimpleName

  override def makeCommand(id: Id, self: ExpressionConverters): CommandExpression =
    SemanticLikeCommand(self.toCommandExpression(id, this.lhs), this.ant, self.toCommandExpression(id, this.rhs))

  override def check(ctx: SemanticContext): SemanticCheck =
    SemanticExpressionCheck.check(ctx, this.arguments) chain
      SemanticExpressionCheck.checkTypes(this, this.signatures)
}

case class SemanticUnlikeExpr(lhs: Expression, ant: Option[AlgoNameWithThresholdExpr], rhs: Expression)(val position: InputPosition)
  extends Expression with BinaryOperatorExpression with ExtendedExpr with ExtendedCommandExpr {
  override val signatures = Vector(
    TypeSignature(argumentTypes = Vector(CTAny, CTAny), outputType = CTBoolean)
  )

  override def makeCommand(id: Id, self: ExpressionConverters): CommandExpression =
    SemanticUnlikeCommand(self.toCommandExpression(id, this.lhs), this.ant, self.toCommandExpression(id, this.rhs))

  override def canonicalOperatorSymbol = this.getClass.getSimpleName

  override def check(ctx: SemanticContext): SemanticCheck =
    SemanticExpressionCheck.check(ctx, this.arguments) chain
      SemanticExpressionCheck.checkTypes(this, this.signatures)
}

case class SemanticCompareExpr(lhs: Expression, ant: Option[AlgoNameWithThresholdExpr], rhs: Expression)(val position: InputPosition)
  extends Expression with BinaryOperatorExpression with ExtendedExpr with ExtendedCommandExpr {
  override val signatures = Vector(
    TypeSignature(argumentTypes = Vector(CTAny, CTAny), outputType = CTFloat)
  )

  override def makeCommand(id: Id, self: ExpressionConverters): CommandExpression =
    SemanticCompareCommand(self.toCommandExpression(id, this.lhs), this.ant, self.toCommandExpression(id, this.rhs))

  override def canonicalOperatorSymbol = this.getClass.getSimpleName

  override def check(ctx: SemanticContext): SemanticCheck =
    SemanticExpressionCheck.check(ctx, this.arguments) chain
      SemanticExpressionCheck.checkTypes(this, this.signatures)
}

case class SemanticSetCompareExpr(lhs: Expression, ant: Option[AlgoNameWithThresholdExpr], rhs: Expression)(val position: InputPosition)
  extends Expression with BinaryOperatorExpression with ExtendedExpr with ExtendedCommandExpr {
  override val signatures = Vector(
    TypeSignature(argumentTypes = Vector(CTAny, CTAny), outputType = CTList(CTList(CTFloat)))
  )

  override def makeCommand(id: Id, self: ExpressionConverters): CommandExpression =
    SemanticSetCompareCommand(self.toCommandExpression(id, this.lhs), this.ant, self.toCommandExpression(id, this.rhs))

  override def canonicalOperatorSymbol = this.getClass.getSimpleName

  override def check(ctx: SemanticContext): SemanticCheck =
    SemanticExpressionCheck.check(ctx, this.arguments) chain
      SemanticExpressionCheck.checkTypes(this, this.signatures)
}

case class SemanticContainExpr(lhs: Expression, ant: Option[AlgoNameWithThresholdExpr], rhs: Expression)(val position: InputPosition)
  extends Expression with BinaryOperatorExpression with ExtendedExpr with ExtendedCommandExpr {
  override val signatures = Vector(
    TypeSignature(argumentTypes = Vector(CTAny, CTAny), outputType = CTBoolean)
  )

  override def makeCommand(id: Id, self: ExpressionConverters): CommandExpression =
    SemanticContainCommand(self.toCommandExpression(id, this.lhs), this.ant, self.toCommandExpression(id, this.rhs))

  override def canonicalOperatorSymbol = this.getClass.getSimpleName

  override def check(ctx: SemanticContext): SemanticCheck =
    SemanticExpressionCheck.check(ctx, this.arguments) chain
      SemanticExpressionCheck.checkTypes(this, this.signatures)
}

case class SemanticInExpr(lhs: Expression, ant: Option[AlgoNameWithThresholdExpr], rhs: Expression)(val position: InputPosition)
  extends Expression with BinaryOperatorExpression with ExtendedExpr with ExtendedCommandExpr {
  override val signatures = Vector(
    TypeSignature(argumentTypes = Vector(CTAny, CTAny), outputType = CTBoolean)
  )

  override def makeCommand(id: Id, self: ExpressionConverters): CommandExpression =
    SemanticInCommand(self.toCommandExpression(id, this.lhs), this.ant, self.toCommandExpression(id, this.rhs))

  override def canonicalOperatorSymbol = this.getClass.getSimpleName

  override def check(ctx: SemanticContext): SemanticCheck =
    SemanticExpressionCheck.check(ctx, this.arguments) chain
      SemanticExpressionCheck.checkTypes(this, this.signatures)
}

case class SemanticContainSetExpr(lhs: Expression, ant: Option[AlgoNameWithThresholdExpr], rhs: Expression)(val position: InputPosition)
  extends Expression with BinaryOperatorExpression with ExtendedExpr with ExtendedCommandExpr {
  override val signatures = Vector(
    TypeSignature(argumentTypes = Vector(CTAny, CTAny), outputType = CTBoolean)
  )

  override def makeCommand(id: Id, self: ExpressionConverters): CommandExpression =
    SemanticContainSetCommand(self.toCommandExpression(id, this.lhs), this.ant, self.toCommandExpression(id, this.rhs))

  override def canonicalOperatorSymbol = this.getClass.getSimpleName

  override def check(ctx: SemanticContext): SemanticCheck =
    SemanticExpressionCheck.check(ctx, this.arguments) chain
      SemanticExpressionCheck.checkTypes(this, this.signatures)
}

case class SemanticSetInExpr(lhs: Expression, ant: Option[AlgoNameWithThresholdExpr], rhs: Expression)(val position: InputPosition)
  extends Expression with BinaryOperatorExpression with ExtendedExpr with ExtendedCommandExpr {
  override val signatures = Vector(
    TypeSignature(argumentTypes = Vector(CTAny, CTAny), outputType = CTBoolean)
  )

  override def canonicalOperatorSymbol = this.getClass.getSimpleName

  override def makeCommand(id: Id, self: ExpressionConverters): CommandExpression =
    SemanticSetInCommand(self.toCommandExpression(id, this.lhs), this.ant, self.toCommandExpression(id, this.rhs))

  override def check(ctx: SemanticContext): SemanticCheck =
    SemanticExpressionCheck.check(ctx, this.arguments) chain
      SemanticExpressionCheck.checkTypes(this, this.signatures)
}

/////////////commands/////////////

case class QueryStateEx(state: QueryState) {
  def getInstanceContext(): ContextMap = {
    val config = state match {
      case x: UpdateCountingQueryContext => state._get("query.inner.inner.transactionalContext.tc.graph.graph.config")
      case _ => state._get("query.inner.transactionalContext.tc.graph.graph.config")
    }
    config.asInstanceOf[Config].getInstanceContext
  }

  def getCustomPropertyProvider(): CustomPropertyProvider =
    getInstanceContext().get[CustomPropertyProvider]()

  def getValueMatcher(): ValueMatcher =
    getInstanceContext().get[ValueMatcher]()
}

case class CustomPropertyCommand(mapExpr: CommandExpression, propertyKey: KeyToken)
  extends CommandExpression with Product with Serializable {

  def apply(ctx: ExecutionContext, state: QueryState): AnyValue =
    mapExpr(ctx, state) match {
      case n if n == Values.NO_VALUE => Values.NO_VALUE

      case x: Value =>
        val pv = QueryStateEx(state).getCustomPropertyProvider
          .getCustomProperty(x.asObject, propertyKey.name)

        pv.map(Values.unsafeOf(_, true)).getOrElse(Values.NO_VALUE)
    }

  def rewrite(f: (CommandExpression) => CommandExpression) = f(CustomPropertyCommand(mapExpr.rewrite(f), propertyKey.rewrite(f)))

  override def children = Seq(mapExpr, propertyKey)

  def arguments = Seq(mapExpr)

  def symbolTableDependencies = mapExpr.symbolTableDependencies

  override def toString = s"$mapExpr.${propertyKey.name}"
}

trait SemanticOperatorSupport {
  val lhsExpr: CommandExpression;
  val ant: Option[AlgoNameWithThresholdExpr];
  val rhsExpr: CommandExpression;

  val (algorithm, threshold) = ant match {
    case None => (None, None)
    case Some(AlgoNameWithThresholdExpr(a, b)) => (a, b)
  }

  def getOperatorString: String

  def rewriteMethod: (CommandExpression, Option[AlgoNameWithThresholdExpr], CommandExpression) => CommandExpression

  def execute[T](m: ExecutionContext, state: QueryState)(f: (AnyValue, AnyValue) => T): T = {
    val lValue = lhsExpr(m, state)
    val rValue = rhsExpr(m, state)
    f(lValue, rValue)
  }

  override def toString: String = lhsExpr.toString() + this.getOperatorString + rhsExpr.toString()

  def containsIsNull = false

  def rewrite(f: (CommandExpression) => CommandExpression) = f(rhsExpr.rewrite(f) match {
    case other => rewriteMethod(lhsExpr.rewrite(f), ant, other)
  })

  def arguments = Seq(lhsExpr, rhsExpr)

  def symbolTableDependencies = lhsExpr.symbolTableDependencies ++ rhsExpr.symbolTableDependencies
}

case class SemanticLikeCommand(lhsExpr: CommandExpression, ant: Option[AlgoNameWithThresholdExpr], rhsExpr: CommandExpression)
                              (implicit converter: TextValue => TextValue = identity)
  extends Predicate with SemanticOperatorSupport {
  override def isMatch(m: ExecutionContext, state: QueryState): Option[Boolean] = {
    execute(m, state) { (lValue: AnyValue, rValue: AnyValue) =>
      (lValue, rValue) match {
        case (Values.NO_VALUE, Values.NO_VALUE) => Some(true)
        case (_, Values.NO_VALUE) => Some(false)
        case (Values.NO_VALUE, _) => Some(false)
        case (a: Value, b: Value) => QueryStateEx(state).getValueMatcher.like(a.asObject(), b.asObject(), algorithm, threshold)
      }
    }
  }

  override def getOperatorString: String = "~:"

  override def rewriteMethod = SemanticLikeCommand(_, _, _)(converter)
}


case class SemanticUnlikeCommand(lhsExpr: CommandExpression, ant: Option[AlgoNameWithThresholdExpr], rhsExpr: CommandExpression)
                                (implicit converter: TextValue => TextValue = identity)
  extends Predicate with SemanticOperatorSupport {

  override def isMatch(m: ExecutionContext, state: QueryState): Option[Boolean] = {
    new SemanticLikeCommand(lhsExpr, ant, rhsExpr)(converter).isMatch(m, state).map(!_);
  }

  override def getOperatorString: String = "!:"

  override def rewriteMethod = SemanticUnlikeCommand(_, _, _)(converter)
}

case class SemanticContainCommand(lhsExpr: CommandExpression, ant: Option[AlgoNameWithThresholdExpr], rhsExpr: CommandExpression)
                                 (implicit converter: TextValue => TextValue = identity)
  extends Predicate with SemanticOperatorSupport {

  override def isMatch(m: ExecutionContext, state: QueryState): Option[Boolean] = {
    execute(m, state) { (lValue: AnyValue, rValue: AnyValue) =>
      (lValue, rValue) match {
        case (Values.NO_VALUE, Values.NO_VALUE) => Some(true)
        case (_, Values.NO_VALUE) => Some(false)
        case (Values.NO_VALUE, _) => Some(false)
        case (a: Value, b: Value) => QueryStateEx(state).getValueMatcher.containsOne(a.asObject(), b.asObject(), algorithm, threshold)
      }
    }
  }

  override def getOperatorString: String = ">:"

  override def rewriteMethod = SemanticContainCommand(_, _, _)(converter)
}

case class SemanticInCommand(lhsExpr: CommandExpression, ant: Option[AlgoNameWithThresholdExpr], rhsExpr: CommandExpression)
                            (implicit converter: TextValue => TextValue = identity)
  extends Predicate with SemanticOperatorSupport {
  override def isMatch(m: ExecutionContext, state: QueryState): Option[Boolean] = {
    new SemanticContainCommand(rhsExpr, ant, lhsExpr)(converter).isMatch(m, state)
  }

  override def getOperatorString: String = "<:"

  override def rewriteMethod = SemanticInCommand(_, _, _)(converter)
}

case class SemanticContainSetCommand(lhsExpr: CommandExpression, ant: Option[AlgoNameWithThresholdExpr], rhsExpr: CommandExpression)
                                    (implicit converter: TextValue => TextValue = identity)
  extends Predicate with SemanticOperatorSupport {

  override def isMatch(m: ExecutionContext, state: QueryState): Option[Boolean] = {
    execute(m, state) { (lValue: AnyValue, rValue: AnyValue) =>
      (lValue, rValue) match {
        case (Values.NO_VALUE, Values.NO_VALUE) => Some(true)
        case (_, Values.NO_VALUE) => Some(false)
        case (Values.NO_VALUE, _) => Some(false)
        case (a: Value, b: Value) => QueryStateEx(state).getValueMatcher.containsSet(a.asObject(), b.asObject(), algorithm, threshold)
      }
    }
  }

  override def getOperatorString: String = ">>:"

  override def rewriteMethod = SemanticContainSetCommand(_, _, _)(converter)
}

case class SemanticSetInCommand(lhsExpr: CommandExpression, ant: Option[AlgoNameWithThresholdExpr], rhsExpr: CommandExpression)
                               (implicit converter: TextValue => TextValue = identity)
  extends Predicate with SemanticOperatorSupport {
  override def isMatch(m: ExecutionContext, state: QueryState): Option[Boolean] = {
    new SemanticContainSetCommand(rhsExpr, ant, lhsExpr)(converter).isMatch(m, state)
  }

  override def getOperatorString: String = "<<:"

  override def rewriteMethod = SemanticSetInCommand(_, _, _)(converter)
}

case class SemanticCompareCommand(lhsExpr: CommandExpression, ant: Option[AlgoNameWithThresholdExpr], rhsExpr: CommandExpression)
                                 (implicit converter: TextValue => TextValue = identity)
  extends CommandExpression with SemanticOperatorSupport {

  override def apply(ctx: ExecutionContext, state: QueryState): AnyValue = {
    execute(ctx, state) { (lValue: AnyValue, rValue: AnyValue) =>
      (lValue, rValue) match {
        case (x, y) if x == Values.NO_VALUE || y == Values.NO_VALUE => Values.NO_VALUE
        case (a: Value, b: Value) => QueryStateEx(state).getValueMatcher.compareOne(a.asObject, b.asObject(), algorithm).
          map(Values.doubleValue(_)).getOrElse(Values.NO_VALUE)
      }
    }
  }

  override def getOperatorString: String = "::"

  override def rewriteMethod = SemanticCompareCommand(_, _, _)(converter)
}

case class SemanticSetCompareCommand(lhsExpr: CommandExpression, ant: Option[AlgoNameWithThresholdExpr], rhsExpr: CommandExpression)
                                    (implicit converter: TextValue => TextValue = identity)
  extends CommandExpression with SemanticOperatorSupport {

  override def apply(ctx: ExecutionContext, state: QueryState): AnyValue = {
    execute(ctx, state) { (lValue: AnyValue, rValue: AnyValue) =>
      (lValue, rValue) match {
        case (x, y) if x == Values.NO_VALUE || y == Values.NO_VALUE => Values.NO_VALUE
        case (a: Value, b: Value) => QueryStateEx(state).getValueMatcher.compareSet(a.asObject, b.asObject(), algorithm).
          map {
            aa => VirtualValues.list(aa.map {
              a => VirtualValues.list(a.map(x => Values.doubleValue(x)).toSeq: _*)
            }.toSeq: _*)
          }.getOrElse(Values.NO_VALUE)
      }
    }
  }

  override def getOperatorString: String = ":::"

  override def rewriteMethod = SemanticSetCompareCommand(_, _, _)(converter)
}

class InvalidSemanticOperatorException(compared: AnyValue) extends RuntimeException {

}
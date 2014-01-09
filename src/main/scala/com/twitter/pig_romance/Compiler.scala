package com.twitter.pig_romance

import com.twitter.pig_romance.antlr.{ PigRomanceBaseListener, PigRomanceParser, PigRomanceLexer }
import org.antlr.v4.runtime.{ ANTLRInputStream, CommonTokenStream, RuleContext }
import org.antlr.v4.runtime.atn.PredictionMode
import org.antlr.v4.runtime.tree.{ ParseTree, ParseTreeProperty, ParseTreeWalker, TerminalNode }
import scala.collection.JavaConversions._
import scala.io.Source
import scala.collection.mutable
import scala.util.{ Failure, Success, Try }
import shapeless.HMap

object Main extends App {
  Compiler.compile(Source.fromFile(args(0)).mkString)
}

object Compiler {
  def parse(input: String): ParseTree = {
    val cs = new ANTLRInputStream(input)
    val lexer = new PigRomanceLexer(cs)
    val tokens = new CommonTokenStream(lexer)
    val parser = new PigRomanceParser(tokens)
    parser.start()
  }

  def compile(input: String): LogicalPlan = {
    val tree = parse(input)
    val walker = new ParseTreeWalker
    val listener = new PigLogicalPlanBuilderListener
    walker.walk(listener, tree)
    null
  }
}

class ContextProperty[T: Manifest] {
  private val property = new ParseTreeProperty[T]
  def set(ctx: ParseTree, v: T) { property.put(ctx, v) }
  def get(ctx: ParseTree): T = Option(property.get(ctx)) match {
      case Some(t) => t
      case None => throw new IllegalStateException(
        "No value of type " + implicitly[Manifest[T]].erasure + " found for given Context: " + ctx.getClass)
    }
}

object Scope {
  def apply(parent: Option[Scope]): Scope = new Scope(parent)
}
class Scope(parent: Option[Scope]) {
  val relations = mutable.Map.empty[RelationIdentifier, LogicalPlan]
  var lastRel: Option[RelationIdentifier] = None

  def getLastLp: LogicalPlan = (lastRel, parent) match {
      case (Some(rel), _) => relations(rel)
      case (None, Some(p)) => p.getLastLp
      case (None, None) => throw new IllegalStateException("There is no last logical plan!")
    }

  def getLpForRelation(rel: RelationIdentifier): LogicalPlan = (relations.get(rel), parent) match {
      case (Some(lp), _) => lp
      case (None, Some(p)) => p.getLpForRelation(rel)
      case (None, None) => throw new IllegalStateException("No logical plan available for relation: " + rel)
    }

  def put(rel: RelationIdentifier, lp: LogicalPlan) {
    relations += ((rel, lp))
    lastRel = Some(rel)
  }

  def putGlobal(rel: RelationIdentifier, lp: LogicalPlan) {
    // Note that this means that this global rel will be the last rel in all of the containing scopes
    put(rel, lp)
    parent match {
      case Some(p) => p.putGlobal(rel, lp)
      case None =>
    }
  }
}

class ContextProperties {
  case class LP
  case class STR
  case class LOADER

  val lp = LP()
  val str = STR()
  val loader = LOADER()

  class NodeMap[K, V]

  implicit val si = new NodeMap[LP, ContextProperty[LogicalPlan]]
  implicit val sd = new NodeMap[STR, ContextProperty[String]]
  implicit val sb = new NodeMap[LOADER, ContextProperty[PigLoader]]

  val hm = HMap[NodeMap](
    lp -> new ContextProperty[LogicalPlan],
    str -> new ContextProperty[String],
    loader -> new ContextProperty[PigLoader]
    )

  //TODO I think we could collapse this with type classes...
  def getLp(ctx: ParseTree): LogicalPlan = hm.get(lp).get.get(ctx)
  def setLp(ctx: ParseTree, v: LogicalPlan) { hm.get(lp).get.set(ctx, v) }

  def getStr(ctx: ParseTree): String = hm.get(str).get.get(ctx)
  def setStr(ctx: ParseTree, v: String) { hm.get(str).get.set(ctx, v) }

  def getLoader(ctx: ParseTree): PigLoader = hm.get(loader).get.get(ctx)
  def setLoader(ctx: ParseTree, v: PigLoader) { hm.get(loader).get.set(ctx, v) }

}

class PigLogicalPlanBuilderListener extends PigRomanceBaseListener {
  var scopes = Scope(None)
  val executions = mutable.Queue.empty[Executable]

  val props = new ContextProperties

  override def enterStart(ctx: PigRomanceParser.StartContext) {
    scopes = Scope(None)
    executions.clear()
  }

  override def exitStart(ctx: PigRomanceParser.StartContext) {
    println(executions)
  }

  override def exitAnywhereCommandInner(ctx: PigRomanceParser.AnywhereCommandInnerContext) {
    val name = RelationIdentifier(Option(ctx.relation) match {
      case Some(rel) => props.getStr(rel.identifier)
      case None => "__previous"
    })
    scopes.put(name, props.getLp(ctx))
  }

  override def exitCommandInnerLoad(ctx: PigRomanceParser.CommandInnerLoadContext) {
    props.setLp(ctx.getParent, Load(props.getStr(ctx.load), props.getLoader(ctx.load)))
  }

  override def exitLoad(ctx: PigRomanceParser.LoadContext) {
    //TODO support realiasing
    props.setStr(ctx, props.getStr(ctx.quoted_path))
    props.setLoader(ctx, Option(ctx.using) match {
      case Some(usingCtx) => throw new UnsupportedOperationException("Can't accept user defined loader yet")
      case None => TextLoader()
    })
  }

  override def exitCommandInnerForeach(ctx: PigRomanceParser.CommandInnerForeachContext) {
    //TODO need to get the transformations
    props.setLp(ctx.getParent, Foreach(props.getLp(ctx.foreach), Vector(IdentityColumn())))
  }

  override def exitQuoted_path(ctx: PigRomanceParser.Quoted_pathContext) {
    props.setStr(ctx, props.getStr(ctx))
  }

  override def exitRelPath(ctx: PigRomanceParser.RelPathContext) {
    props.setStr(
      ctx.getParent,
      ctx.relative_path.path_piece.map { c => props.getStr(c.identifier) }.mkString("/"))
  }

  override def exitDumpExec(ctx: PigRomanceParser.DumpExecContext) {
    executions += Dump(props.getLp(ctx.dump))
  }

  override def exitNestedCommandPrevious(ctx: PigRomanceParser.NestedCommandPreviousContext) {
    //TODO is it possible to make it so we don't have to set the parent, and can set the nested_command node?
    props.setLp(ctx.getParent, scopes.getLastLp)
  }

  override def exitNestedCommandIdentifier(ctx: PigRomanceParser.NestedCommandIdentifierContext) {
    props.setLp(ctx.getParent, scopes.getLpForRelation(RelationIdentifier(props.getStr(ctx.identifier))))
  }

  override def exitNestedCommandInner(ctx: PigRomanceParser.NestedCommandInnerContext) {
    props.setLp(ctx.getParent, props.getLp(ctx))
  }

  override def exitIdentifier(ctx: PigRomanceParser.IdentifierContext) {
    props.setStr(ctx, ctx.IDENTIFIER.getText)
  }
}

case class RelationIdentifier(id: String)
//TODO need to think about what this should look like
trait PigLoader {
  def getSchema: PigTuple
}
case class TextLoader extends PigLoader {
  override val getSchema = PigTuple(None, Vector(PigString(None)))
}

// PIG SCHEMA
//TODO we would like to implement all of this in such a way that we don't have the n^2 explosion that pig has...
sealed abstract class PigSchema(val name: Option[String])
sealed abstract class PigNumber(override val name: Option[String]) extends PigSchema(name)
case class PigInt(override val name: Option[String]) extends PigNumber(name)
case class PigLong(override val name: Option[String]) extends PigNumber(name)
case class PigFloat(override val name: Option[String]) extends PigNumber(name)
case class PigDouble(override val name: Option[String]) extends PigNumber(name)
case class PigString(override val name: Option[String]) extends PigSchema(name)
case class PigByteArray(override val name: Option[String]) extends PigSchema(name)
case class PigTuple(override val name: Option[String], columns: Vector[PigSchema]) extends PigSchema(name) {
  require(
    {val names = columns flatMap { _.name }
    names.size == names.toSet.size},
    "No names in PigTuple can be repeated!"
  )
}
case class PigBag(override val name: Option[String], rows: PigTuple) extends PigSchema(name)
case class PigMap(override val name: Option[String], values: PigTuple) extends PigSchema(name)

sealed trait LogicalPlan {
  val columns: PigTuple
}
// This represents things like store, dump which need to actually be run.
sealed abstract class Executable(plan: LogicalPlan)
// TODO also have this require a parent LogicalPlan?
sealed abstract class LogicalPlanRelation extends LogicalPlan
case class Load(location: String, loader: PigLoader) extends LogicalPlanRelation {
  override val columns = loader.getSchema
}
case class Foreach(plan: LogicalPlan, transformations: Vector[Column]) extends LogicalPlanRelation {
  // Note that this is a bit of a departure from Pig. This means that if rel A has type a,b then
  // B = foreach A generate *,*; in old pig makes B type a,b,a,b whereas in this it'd be (a,b), (a,b)
  // which seems more principled. We can inject a flatten if this is too annoying but the naming can get weird.
  override val columns = PigTuple(None, transformations map { _(plan) })
}
case class Dump(plan: LogicalPlan) extends Executable(plan)
case class Store(plan: LogicalPlan) extends Executable(plan)

sealed abstract class Column extends (LogicalPlan => PigSchema)
case class IdentityColumn extends Column {
  override def apply(plan: LogicalPlan) = plan.columns
}
case class ByNameSelector(name: String) extends Column {
  override def apply(plan: LogicalPlan) =
    plan.columns.columns find { _.name.exists { _ == name } } match {
      case Some(schema) => schema
      case None => throw new IllegalStateException("There is no column with the requested name: " + name)
    }
}
case class PositionalSelector(index: Int) extends Column {
  override def apply(plan: LogicalPlan) =
    Try(plan.columns.columns(index)) match {
      case Success(schema) => schema
      case Failure(_) => throw new IllegalStateException("There is no column at the given index: " + index)
    }
}

// The following is a dummy physical plan for testing
sealed trait PigPhysicalType
sealed trait PPNumber extends PigPhysicalType
sealed trait PPInt extends PPNumber
sealed trait PPLong extends PPNumber
sealed trait PPFloat extends PPNumber
sealed trait PPDouble extends PPNumber
sealed trait PPTuple extends PigPhysicalType
sealed trait PPBag extends PigPhysicalType
sealed trait PPMap extends PigPhysicalType

sealed trait PhysicalPlan {
  def getData: Iterator[PigTuple]
}

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

  //TODO should probably return an intermediate structure
  def compile(input: String) {
    val tree = parse(input)
    val walker = new ParseTreeWalker
    val listener = new PigLogicalPlanBuilderListener
    walker.walk(listener, tree)
    println("==========")
    println("TO EXECUTE")
    println("==========")
    listener.executions foreach println
    //TODO note that this will not currently do the memoization or anything like that. My focus is on the logical plan,
    // but need a dummy local mode for testing.
    println("==========")
    println("EXECUTING")
    println("==========")
    listener.executions foreach { ExecutionPP(_).exec() }
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
  case object LP
  case object STR
  case object LOADER
  case object COLUMN
  case object COLUMNS
  case object INT

  class NodeMap[K, V]

  implicit val i1 = new NodeMap[LP.type, ContextProperty[LogicalPlan]]
  implicit val i2 = new NodeMap[STR.type, ContextProperty[String]]
  implicit val i3 = new NodeMap[LOADER.type, ContextProperty[PigLoader]]
  implicit val i4 = new NodeMap[COLUMN.type, ContextProperty[Column]]
  implicit val i5 = new NodeMap[COLUMNS.type, ContextProperty[Vector[Column]]]
  implicit val i6 = new NodeMap[INT.type, ContextProperty[Int]]

  val hm = HMap[NodeMap](
    LP -> new ContextProperty[LogicalPlan],
    STR -> new ContextProperty[String],
    LOADER -> new ContextProperty[PigLoader],
    COLUMN -> new ContextProperty[Column],
    COLUMNS -> new ContextProperty[Vector[Column]],
    INT -> new ContextProperty[Int]
    )

  //TODO I think we could collapse this with type classes...
  def getLp(ctx: ParseTree): LogicalPlan = hm.get(LP).get.get(ctx)
  def setLp(ctx: ParseTree, v: LogicalPlan) { hm.get(LP).get.set(ctx, v) }

  def getStr(ctx: ParseTree): String = hm.get(STR).get.get(ctx)
  def setStr(ctx: ParseTree, v: String) { hm.get(STR).get.set(ctx, v) }

  def getLoader(ctx: ParseTree): PigLoader = hm.get(LOADER).get.get(ctx)
  def setLoader(ctx: ParseTree, v: PigLoader) { hm.get(LOADER).get.set(ctx, v) }

  def getColumn(ctx: ParseTree): Column = hm.get(COLUMN).get.get(ctx)
  def setColumn(ctx: ParseTree, v: Column) { hm.get(COLUMN).get.set(ctx, v) }

  def getColumns(ctx: ParseTree): Vector[Column] = hm.get(COLUMNS).get.get(ctx)
  def setColumns(ctx: ParseTree, v: Vector[Column]) { hm.get(COLUMNS).get.set(ctx, v) }
  def addColumn(ctx: ParseTree, v: Column) = setColumns(ctx, getColumns(ctx) :+ v)

  def getInt(ctx: ParseTree): Int = hm.get(INT).get.get(ctx)
  def setInt(ctx: ParseTree, v: Int) { hm.get(INT).get.set(ctx, v) }
}

class PigLogicalPlanBuilderListener extends PigRomanceBaseListener {
  var scopes = Scope(None)
  val executions = mutable.Queue.empty[Executable]

  val props = new ContextProperties

  override def enterStart(ctx: PigRomanceParser.StartContext) {
    scopes = Scope(None)
    executions.clear()
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
    props.setLp(ctx.getParent, Foreach(props.getLp(ctx.foreach), Columns(props.getColumns(ctx.foreach))))
  }

  override def enterColumn_transformations(ctx: PigRomanceParser.Column_transformationsContext) {
    props.setColumns(ctx, Vector())
  }

  override def exitColumn_transformations(ctx: PigRomanceParser.Column_transformationsContext) {
    props.setColumns(ctx.getParent, props.getColumns(ctx))
  }

  override def exitColumn_expression_realias(ctx: PigRomanceParser.Column_expression_realiasContext) {
    props.addColumn(ctx.getParent, props.getColumn(ctx))
  }

  override def exitColumnExpressionStar(ctx: PigRomanceParser.ColumnExpressionStarContext) {
    props.setColumn(ctx.getParent, IdentityColumn())
  }

  override def exitColumnExpressionTuple(ctx: PigRomanceParser.ColumnExpressionTupleContext) {
    props.setColumn(ctx.getParent, Columns(props.getColumns(ctx.tuple)))
  }

  override def exitColumnExpressionFlatten(ctx: PigRomanceParser.ColumnExpressionFlattenContext) {
    props.setColumn(ctx.getParent, props.getColumn(ctx.flatten))
  }

  override def exitFlatten(ctx: PigRomanceParser.FlattenContext) {
    props.setColumn(ctx, FlattenColumn(Columns(props.getColumns(ctx.tuple))))
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

  override def exitShellDescribe(ctx: PigRomanceParser.ShellDescribeContext) {
    //TODO cleaner printing, also say what we are printing
    executions += Describe(props.getLp(ctx.describe))
  }

  override def exitColumnExpressionTransform(ctx: PigRomanceParser.ColumnExpressionTransformContext) {
    props.setColumn(ctx.getParent, props.getColumn(ctx))
  }

  override def exitColumnTransformColIdentifier(ctx: PigRomanceParser.ColumnTransformColIdentifierContext) {
    props.setColumn(ctx.getParent, props.getColumn(ctx))
  }

  override def exitColumnIdentifierName(ctx: PigRomanceParser.ColumnIdentifierNameContext) {
    props.setColumn(ctx.getParent, ByNameSelector(props.getStr(ctx.identifier)))
  }

  override def exitColumnIdentifierPos(ctx: PigRomanceParser.ColumnIdentifierPosContext) {
    props.setColumn(ctx.getParent, PositionalSelector(props.getInt(ctx.relative_identifier.integer)))
  }

  override def exitInteger(ctx: PigRomanceParser.IntegerContext) {
    val int = ctx.POSITIVE_INTEGER.getText.toInt
    props.setInt(ctx, if (Option(ctx.NEG).isDefined) -int else int)
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

//TODO we would like to implement all of this in such a way that we don't have the n^2 explosion that pig has...
//TODO pretty printing will make this a lot easier
sealed abstract class PigSchema(val name: Option[String])
sealed abstract class PigNumber(override val name: Option[String]) extends PigSchema(name)
case class PigInt(override val name: Option[String]) extends PigNumber(name)
case class PigLong(override val name: Option[String]) extends PigNumber(name)
case class PigFloat(override val name: Option[String]) extends PigNumber(name)
case class PigDouble(override val name: Option[String]) extends PigNumber(name)
case class PigString(override val name: Option[String]) extends PigSchema(name)
case class PigByteArray(override val name: Option[String]) extends PigSchema(name)
//TODO is this the right way to do this? Can't be pattern matched on, can it?
sealed abstract class PigFlattenable(override val name: Option[String]) extends PigSchema(name)
case class PigTuple(override val name: Option[String], columns: Vector[PigSchema]) extends PigFlattenable(name) {
  require(
    {val names = columns flatMap { _.name }
    names.size == names.toSet.size},
    "No names in a PigTuple can be repeated!"
  )
}
case class PigBag(override val name: Option[String], rows: PigTuple) extends PigFlattenable(name)
case class PigMap(override val name: Option[String], values: PigTuple) extends PigSchema(name)

sealed trait LogicalPlan {
  val columns: PigTuple
}

// TODO also have this require a parent LogicalPlan?
sealed abstract class LogicalPlanRelation extends LogicalPlan
case class Load(location: String, loader: PigLoader) extends LogicalPlanRelation {
  override val columns = loader.getSchema
}
//TODO make sure we can support column pruning
case class Foreach(plan: LogicalPlan, transformations: Columns) extends LogicalPlanRelation {
  // Note that this is a bit of a departure from Pig. This means that if rel A has type a,b then
  // B = foreach A generate *,*; in old pig makes B type a,b,a,b whereas in this it'd be (a,b), (a,b)
  // which seems more principled. We can inject a flatten if this is too annoying but the naming can get weird.
  override val columns = transformations(plan.columns)
}

// This represents things like store, dump which need to actually be run. This ensures that we can control which side
// effects run before code executes.
sealed abstract class Executable(plan: LogicalPlan)
case class Dump(plan: LogicalPlan) extends Executable(plan)
case class Store(plan: LogicalPlan) extends Executable(plan)
//TODO ShellExecutable parent?
case class Describe(plan: LogicalPlan) extends Executable(plan)

//TODO I'm not sure if this is the right way to do this, as it doesn't really make doing a flatten easy, as well as
// expressing columns that depend on results of other columns
case class Columns(columns: Vector[Column]) extends Column {
  private def merge(left: Option[PigTuple], right: PigSchema): PigTuple =
    left match {
      case Some(PigTuple(name, columns)) => PigTuple(name, columns :+ right)
      case None => PigTuple(None, Vector(right))
    }

  def apply(schema: PigSchema): PigTuple = {
    val start: Option[PigTuple] = None
    columns.foldLeft(start) { (cum, col) =>
      Some(col match {
        case IdentityColumn() => merge(cum, schema)
        case ByNameSelector(name) =>
          merge(cum, schema match {
            case PigTuple(_, columns) =>
              columns find { _.name.exists { _ == name } } match {
                case Some(s) => s
                case None => throw new IllegalStateException("There is no column with the requested name: " + name)
              }
            case _: PigBag => throw new UnsupportedOperationException("Bags not supported yet")
            case _ => throw new IllegalStateException("Cannot select by name on field of type: " + schema.getClass)
          })
        case PositionalSelector(index) =>
          merge(cum, schema match {
            case PigTuple(_, columns) =>
              Try(columns(index)) match {
                case Success(schema) => schema
                case Failure(_) => throw new IllegalStateException("There is no column at the given index: " + index)
              }
            case _: PigBag => throw new UnsupportedOperationException("Bags not supported yet")
            case _ => throw new IllegalStateException("Cannot select by position on field of type: " + schema.getClass)
          })
        case FlattenColumn(dep) =>
          //TODO yuck
          (Columns(Vector(dep))(schema).columns.head match {
            case PigTuple(_, columns) => columns.foldLeft(cum) { (inCum, inSchema) => Some(merge(inCum, inSchema)) }
            case _: PigBag => throw new UnsupportedOperationException("Bags not supported yet")
            case _ => throw new IllegalStateException("Cannot flatten type: " + schema.getClass)
          }) match {
            case Some(s) => s
            case None => throw new IllegalStateException("Flattened something empty")
          }
        //TODO implement
        case c: Columns => merge(cum, c(schema))
      })
    } match {
      case Some(s) => s
      case None => throw new IllegalStateException("Got no schema back!")
    }
  }
}

sealed trait Column
case class IdentityColumn extends Column
case class ByNameSelector(name: String) extends Column
case class PositionalSelector(index: Int) extends Column
sealed abstract class DependantColumn(dep: Column) extends Column
case class FlattenColumn(dep: Column) extends DependantColumn(dep)

// The following is a dummy physical plan for testing
sealed trait PigPhysicalType
sealed trait NumberPP extends PigPhysicalType
case class IntPP(v: Int) extends NumberPP
case class LongPP(v: Long) extends NumberPP
case class FloatPP(v: Float) extends NumberPP
case class DoublePP(v: Double) extends NumberPP
case class StringPP(v: String) extends PigPhysicalType
case class TuplePP(v: Vector[PigPhysicalType]) extends PigPhysicalType
case class BagPP(v: Iterator[TuplePP]) extends PigPhysicalType
case class MapPP(v: Map[String, TuplePP]) extends PigPhysicalType

sealed trait PhysicalPlan {
  def getData: Iterator[TuplePP]
}
object ExecutionPP {
  def apply(e: Executable): ExecutionPP = {
    e match {
      case Dump(lp) => throw new UnsupportedOperationException("Working on dump")
      case Store(lp) => throw new UnsupportedOperationException("Working on store")
      case Describe(lp) => DescribeEPP(lp.columns)
    }
  }
}
sealed trait ExecutionPP {
  def exec()
}
case class LoadPP(location: String) extends PhysicalPlan {
  override def getData = Source.fromFile(location).getLines map { l => TuplePP(Vector(StringPP(l))) }
}
case class DumpEPP(dep: PhysicalPlan) extends ExecutionPP {
  override def exec() { dep.getData foreach println }
}
case class DescribeEPP(s: PigSchema) extends ExecutionPP {
  override def exec() { println(s) }
}

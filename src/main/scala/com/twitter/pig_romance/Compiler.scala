package com.twitter.pig_romance

import com.twitter.pig_romance.antlr.{ PigRomanceBaseListener, PigRomanceParser, PigRomanceLexer }
import org.antlr.v4.runtime.{ ANTLRInputStream, CommonTokenStream, RuleContext }
import org.antlr.v4.runtime.atn.PredictionMode
import org.antlr.v4.runtime.tree.{ ParseTree, ParseTreeProperty, ParseTreeWalker, TerminalNode }
import scala.collection.JavaConversions._
import scala.io.Source
import scala.collection.mutable

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

class ContextProperty[T] {
  private val property = new ParseTreeProperty[T]
  def set(ctx: ParseTree, v: T) { property.put(ctx, v) }
  def get(ctx: ParseTree): T = Option(property.get(ctx)) match {
      case Some(t) => t
      case None => throw new IllegalStateException("No value found for given Context: " + ctx)
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

class PigLogicalPlanBuilderListener extends PigRomanceBaseListener {
  var scopes = Scope(None)
  val executions = mutable.Queue.empty[ExecutableLogicalPlan]
  val lpProperty = new ContextProperty[LogicalPlan]
  val strProperty = new ContextProperty[String]

  override def enterStart(ctx: PigRomanceParser.StartContext) {
    scopes = Scope(None)
    executions.clear()
  }

  override def exitStart(ctx: PigRomanceParser.StartContext) {
    println(executions)
  }

  override def exitAnywhereCommandInner(ctx: PigRomanceParser.AnywhereCommandInnerContext) {
    val name = RelationIdentifier(Option(ctx.relation) match {
      case Some(rel) => rel.identifier.IDENTIFIER.getText
      case None => "__previous"
    })
    scopes.put(name, lpProperty.get(ctx))
  }

  override def exitCommandInnerLoad(ctx: PigRomanceParser.CommandInnerLoadContext) {
    //TODO need to make this correct
    val rel = strProperty.get(ctx.load.quoted_path)
    lpProperty.set(ctx.getParent, Load(rel, null, null))
  }

  override def exitQuoted_path(ctx: PigRomanceParser.Quoted_pathContext) {
    strProperty.set(ctx, strProperty.get(ctx))
  }

  override def exitRelPath(ctx: PigRomanceParser.RelPathContext) {
    strProperty.set(ctx.getParent, ctx.relative_path.path_piece.map { _.identifier.IDENTIFIER.getText }.mkString("/"))
  }

  override def exitDumpExec(ctx: PigRomanceParser.DumpExecContext) {
    executions += Dump(lpProperty.get(ctx.dump))
  }

  override def exitNestedCommandPrevious(ctx: PigRomanceParser.NestedCommandPreviousContext) {
    //TODO is it possible to make it so we don't have to set the parent, and can set the nested_command node?
    lpProperty.set(ctx.getParent, scopes.getLastLp)
  }

  override def exitNestedCommandIdentifier(ctx: PigRomanceParser.NestedCommandIdentifierContext) {
    lpProperty.set(ctx.getParent, scopes.getLpForRelation(RelationIdentifier(ctx.identifier.IDENTIFIER.getText)))
  }

  override def exitNestedCommandInner(ctx: PigRomanceParser.NestedCommandInnerContext) {
    lpProperty.set(ctx.getParent, lpProperty.get(ctx))
  }
}

case class RelationIdentifier(id: String)
//TODO need to think about what this should look like
case class PigLoader(id: String)
//TODO need to make this real, this is just a placeholder atm
case class PigSchema(id: String)

sealed trait LogicalPlan
// This represents things like store, dump which need to actually be run.
sealed abstract class ExecutableLogicalPlan(plan: LogicalPlan) extends LogicalPlan
sealed abstract class LogicalPlanRelation(schema: PigSchema) extends LogicalPlan
case class Load(location: String, loader: PigLoader, schema: PigSchema) extends LogicalPlanRelation(schema)
case class Dump(plan: LogicalPlan) extends ExecutableLogicalPlan(plan)
case class Store(plan: LogicalPlan) extends ExecutableLogicalPlan(plan)

//TODO a shell command takes as a dependency everything in scope... it should apply to any stores, dumps, etc

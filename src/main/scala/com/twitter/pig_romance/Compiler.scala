package com.twitter.pig_romance

import com.twitter.pig_romance.antlr.{ PigRomanceBaseListener, PigRomanceParser, PigRomanceLexer }
import org.antlr.v4.runtime.{ BailErrorStrategy, DiagnosticErrorListener, ANTLRInputStream, CommonTokenStream, RuleContext }
import org.antlr.v4.runtime.atn.PredictionMode
import org.antlr.v4.runtime.tree.{ ParseTreeWalker, ParseTree, TerminalNode }
import scala.collection.JavaConversions._
import scala.io.Source

object Main extends App {
  Compiler.parse(Source.fromFile(args(0)).mkString)
}

object Compiler {
  def parse(input: String): ParseTree = {
    val cs = new ANTLRInputStream(input)
    val lexer = new PigRomanceLexer(cs)
    val tokens = new CommonTokenStream(lexer)
    val parser = new PigRomanceParser(tokens)
    // This is expensive and eventually we won't want it but for now we want it as we debug
    //parser.getInterpreter.setPredictionMode(PredictionMode.LL_EXACT_AMBIG_DETECTION)
    //parser.removeErrorListeners()
    //parser.addErrorListener(new DiagnosticErrorListener)
    //parser.setErrorHandler(new BailErrorStrategy)
    parser.start()
  }
}

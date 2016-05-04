package sqlest.ast

case class Merge[A <: Table, B <: Relation](into: A, using: B, subqueryAlias: String, whenMatched: Update, whenNotMatched: Insert) extends Command

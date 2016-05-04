package sqlest.ast.syntax

import sqlest.ast._

trait MergeSyntax {
  def apply(table: Table) = MergeBuilder(table)
}

case class MergeBuilder(table: Table) {
  def using(relation: Relation) = MergeOnBuilder(table, relation)
}

case class MergeOnBuilder(table: Table, using: Relation) {
  def on(condition: Column[Boolean]) = MergeMatchedBuilder(table, using, condition)
}

case class MergeMatchedBuilder(table: Table, using: Relation, condition: Column[Boolean]) {
  def whenMatchedThen(whenMatched: UpdateWhereBuilder) = MergeNotMatchedBuilder(table, using, condition, whenMatched.where(ConstantColumn[Boolean](true)))
}

case class MergeNotMatchedBuilder(table: Table, using: Relation, condition: Column[Boolean], whenMatched: Command) {
  def whenNotMatchedThen(whenNotMatched: Command) =
    Merge(
      into = table,
      using = using,
      on = condition,
      whenMatched = Some(whenMatched),
      whenNotMatched = Some(whenNotMatched)
    )
}

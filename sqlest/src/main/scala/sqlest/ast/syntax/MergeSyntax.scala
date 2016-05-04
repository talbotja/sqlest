package sqlest.ast.syntax

import sqlest.ast._

trait MergeSyntax {
  def apply(table: Table) = MergeBuilder(table)
}

case class MergeBuilder(table: Table) {
  def using(relation: Relation) = MergeAsBuilder(table, relation)
}

case class MergeAsBuilder(table: Table, using: Relation) {
  def as(alias: String) = MergeOnBuilder(table, using, alias)
}

case class MergeOnBuilder(table: Table, using: Relation, alias: String) {
  def on(condition: Column[Boolean]) = MergeMatchedBuilder(table, using, alias, condition)
}

case class MergeMatchedBuilder(table: Table, using: Relation, alias: String, condition: Column[Boolean]) {
  def whenMatchedThen(update: UpdateWhereBuilder) = MergeNotMatchedBuilder(table, using, alias, condition, update.where(condition))
}

case class MergeNotMatchedBuilder(table: Table, using: Relation, alias: String, condition: Column[Boolean], whenMatched: Update) {
  def whenNotMatchedThen(insert: Insert) =
    Merge(
      into = table,
      using = table.innerJoin(using).on(condition),
      subqueryAlias = alias,
      whenMatched = whenMatched,
      whenNotMatched = insert
    )
}

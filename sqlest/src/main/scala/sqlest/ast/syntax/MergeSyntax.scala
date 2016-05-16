package sqlest.ast.syntax

import sqlest.ast._

trait MergeSyntax {
  def into(table: Table) = MergeBuilder(table)
}

case class MergeBuilder(table: Table) {
  def using(relation: Relation) = MergeOnBuilder(table, relation)
}

case class MergeOnBuilder(table: Table, using: Relation) {
  def on(condition: Column[Boolean]) = MergeMatchedBuilder(table, using, condition)
}

case class MergeMatchedBuilder(table: Table, using: Relation, condition: Column[Boolean]) {
  def whenMatchedThen(whenMatched: MergeCommand) = MergeNotMatchedBuilder(table, using, condition, whenMatched)
}

case class MergeNotMatchedBuilder(table: Table, using: Relation, condition: Column[Boolean], whenMatched: MergeCommand) {
  def whenNotMatchedThen(whenNotMatched: MergeCommand) =
    Merge(
      into = table,
      using = using,
      on = condition,
      whenMatched = Some(whenMatched),
      whenNotMatched = Some(whenNotMatched)
    )
}

trait MergeUpdateSyntax {
  def set(setters: Setter[_, _]*): MergeUpdate =
    MergeUpdate(setters)

  def set(setters: => Seq[Setter[_, _]]): MergeUpdate =
    MergeUpdate(setters)
}

trait MergeInsertSyntax {
  def columns(columns: TableColumn[_]*) =
    MergeInsertColumnsBuilder(columns)

  def set(setters: Setter[_, _]*) =
    MergeInsert(Seq(setters))

  def set(setters: Seq[Setter[_, _]])(implicit d: DummyImplicit) =
    MergeInsert(Seq(setters))
}

case class MergeInsertColumnsBuilder(columns: Seq[TableColumn[_]]) {
  def values(setters: Setter[_, _]*) = {
    if (columns != setters.map(_.column)) throw new AssertionError(s"Cannot insert value to the columns declared")
    MergeInsert(Seq(setters))
  }

  def values(setters: Seq[Setter[_, _]])(implicit d: DummyImplicit) = {
    if (columns != setters.map(_.column)) throw new AssertionError(s"Cannot insert value to the columns declared")
    MergeInsert(Seq(setters))
  }
}
package sqlest.ast

case class Merge[A <: Table, B <: Relation](
  into: A,
  using: B,
  on: Column[Boolean],
  whenMatched: Option[MergeCommand],
  whenNotMatched: Option[MergeCommand]) extends Command

trait MergeCommand
case class MergeUpdate(set: Seq[Setter[_, _]]) extends MergeCommand
case class MergeInsert(setterLists: Seq[Seq[Setter[_, _]]]) extends MergeCommand {
  def columns = setterLists.head.map(_.column)
}
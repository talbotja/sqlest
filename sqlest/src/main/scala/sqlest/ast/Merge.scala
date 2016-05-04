package sqlest.ast

case class Merge[A <: Table, B <: Relation](into: A, using: B, on: Column[Boolean], whenMatched: Option[Command], whenNotMatched: Option[Command]) extends Command

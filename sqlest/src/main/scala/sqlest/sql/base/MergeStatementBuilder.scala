package sqlest.sql.base

import sqlest.ast._

trait MergeStatementBuilder extends BaseStatementBuilder {
  // Only implemented for DB2
  def mergeSql[A <: Table, B <: Relation](merge: Merge[A, B]): String = throw new UnsupportedOperationException

  def mergeArgs[A <: Table, B <: Relation](merge: Merge[A, B]): List[List[LiteralColumn[_]]] = throw new UnsupportedOperationException
}

/*
 * Copyright 2014 JHC Systems Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sqlest.ast.syntax

import sqlest.ast.{ Lateral, Relation, Select, ExistsColumn, NotExistsColumn }

trait QuerySyntax {
  object select extends SelectSyntax
  object insert extends InsertSyntax with MergeInsertSyntax
  object update extends UpdateSyntax with MergeUpdateSyntax
  object delete extends DeleteSyntax
  object merge extends MergeSyntax

  implicit def selectOps[A, R <: Relation](select: Select[A, R]) = SelectOps(select)
  def lateral[A, R <: Relation](select: Select[A, R]) = Lateral(select)
  def exists[A, R <: Relation](select: Select[A, R]) = ExistsColumn(select)
  def notExists[A, R <: Relation](select: Select[A, R]) = NotExistsColumn(select)

}

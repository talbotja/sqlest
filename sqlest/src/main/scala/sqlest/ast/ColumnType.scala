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

package sqlest.ast

import org.joda.time.{ DateTime, LocalDate }
import scala.language.experimental.macros
import scala.reflect.macros.whitebox.Context

/**
 * Object representing a mapping between a Scala data type and an underlying SQL data type.
 * Column types are broadly divided into three categories:
 *
 *  - `BaseColumnType` represent Scala data types that can be directly mapped to SQL types;
 *  - `OptionColumnTypes` represent Scala `Option` types that are mapped to nullable SQL types;
 *  - `MappedColumnTypes` represent arbitrary mappings between Scala types and SQL types.
 */
sealed trait ColumnType[A] {
  type Database
  def read(database: Option[Database]): Option[A]
  def write(value: A): Database
}

/** Trait representing a column type read directly from the database. */
sealed trait BaseColumnType[A] extends ColumnType[A] {
  type Database = A
  def read(database: Option[Database]) = database
  def write(value: A) = value
}
sealed trait NumericColumnType[A] extends BaseColumnType[A]
sealed trait NonNumericColumnType[A] extends BaseColumnType[A]

case object IntColumnType extends NumericColumnType[Int]
case object LongColumnType extends NumericColumnType[Long]
case object DoubleColumnType extends NumericColumnType[Double]
case object BigDecimalColumnType extends NumericColumnType[BigDecimal]

case object BooleanColumnType extends NonNumericColumnType[Boolean]
case object StringColumnType extends NonNumericColumnType[String]
case object DateTimeColumnType extends NonNumericColumnType[DateTime]
case object LocalDateColumnType extends NonNumericColumnType[LocalDate]
case object ByteArrayColumnType extends NonNumericColumnType[Array[Byte]]

/**
 * Class representing an nullable SQL column type that is mapped to an `Option` in Scala.
 *
 * For every `OptionColumnType` there is an underlying `BaseColumnType`.
 */
case class OptionColumnType[A, B](nullValue: B, isNull: B => Boolean)(implicit val innerColumnType: ColumnType.Aux[A, B]) extends ColumnType[Option[A]] {
  type Database = B
  val baseColumnType: BaseColumnType[B] = innerColumnType match {
    case baseColumnType: ColumnType[A] with BaseColumnType[B] => baseColumnType
    case optionColumnType: ColumnType[A] with OptionColumnType[A, B] => optionColumnType.baseColumnType
    case mappedColumnType: ColumnType[A] with MappedColumnType[A, B] => mappedColumnType.baseColumnType
  }

  def read(database: Option[Database]): Option[Option[A]] = {
    if (database.map(isNull).getOrElse(false)) Some(None)
    else Some(innerColumnType.read(database))
  }

  def write(value: Option[A]): Database =
    if (value.isEmpty) nullValue
    else innerColumnType.write(value.get)

  def hasNullNullValue = nullValue == null
  override def toString = s"OptionColumnType($nullValue, $isNull)($innerColumnType)"
}

object OptionColumnType {
  def apply[A, B](nullValue: B)(implicit innerColumnType: ColumnType.Aux[A, B]): OptionColumnType[A, B] = apply(nullValue, (_: B) == nullValue)
  def apply[A, B](innerColumnType: ColumnType.Aux[A, B]): OptionColumnType[A, B] = apply(null.asInstanceOf[B])(innerColumnType)
}

/**
 * Object representing a custom column type `A` with an underlying database type `B`.
 *
 * For every `MappedColumnType` there is an underlying `BaseColumnType`.
 */
abstract class MappedColumnType[A, B](implicit val innerColumnType: ColumnType.Aux[B, B]) extends ColumnType[A] {
  type Database = B
  val baseColumnType: BaseColumnType[B]
  def mappedRead(database: B): A
  def mappedWrite(value: A): B
  def read(database: Option[Database]): Option[A] = innerColumnType.read(database).map(mappedRead)
  def write(value: A): Database = innerColumnType.write(mappedWrite(value))
}

object MappedColumnType {
  /**
   * Convenience method for constructing a `MappedColumnType` from a pair of
   * bidirectional mapping functions and an implicitly provided `BaseColumnType`.
   */
  def apply[A, B: BaseColumnType](r: B => A, w: A => B)(implicit innerColumnType: ColumnType.Aux[B, B]) = new MappedColumnType[A, B]()(innerColumnType) {
    val baseColumnType = implicitly[BaseColumnType[B]]
    def mappedRead(database: B): A = r(database)
    def mappedWrite(value: A): B = w(value)
  }

  implicit class MappedColumnTypeOps[B, C: BaseColumnType](baseColumnType: ColumnType.Aux[B, C]) {
    def compose[A](outerColumnType: ColumnType.Aux[A, B]): MappedColumnType[A, C] = {
      MappedColumnType(
        (database: C) => outerColumnType.read(baseColumnType.read(Some(database))).get,
        (value: A) => baseColumnType.write(outerColumnType.write(value))
      )
    }
  }
}

object ColumnType {
  // The ColumnType.Aux type provides access the Database type member on ColumnType
  type Aux[A, B] = ColumnType[A] { type Database = B }

  implicit val booleanColumnType = BooleanColumnType
  implicit val intColumnType = IntColumnType
  implicit val longColumnType = LongColumnType
  implicit val doubleColumnType = DoubleColumnType
  implicit val bigDecimalColumnType = BigDecimalColumnType
  implicit val stringColumnType = StringColumnType
  implicit val dateTimeColumnType = DateTimeColumnType
  implicit val localDateColumnType = LocalDateColumnType
  implicit val byteArrayColumnType = ByteArrayColumnType
  implicit def materialize[A, B]: MappedColumnType[A, B] = macro MaterializeColumnTypeMacro.materializeImpl[A, B]

  implicit def optionType[A, B](implicit base: ColumnType.Aux[A, B]) = OptionColumnType[A, B](base)

  implicit class OptionColumnTypeOps[A, B](left: ColumnType.Aux[A, B]) {
    def toOptionColumnType = left match {
      case option: ColumnType[A] with OptionColumnType[A, B] => option
      case base => OptionColumnType.apply[A, base.Database](base)
    }
  }
}

case class MaterializeColumnTypeMacro(c: Context) {
  import c.universe._

  def materializeImpl[A: c.WeakTypeTag, B: c.WeakTypeTag] = {
    val typeOfA = c.weakTypeOf[A]
    val companion = typeOfA.typeSymbol.companion
    val applyMethod = findMethod(companion.typeSignature, "apply", typeOfA)
    val unapplyMethod = findMethod(companion.typeSignature, "unapply", typeOfA)
    val typeOfB = applyMethod.paramLists.head.head.asTerm.typeSignature
    val registryKey = typeOfA.toString
    val registryLocation = q"_root_.sqlest.ast.MaterializeColumnTypeMacro"
    val materializeMappedColumnType = q"MappedColumnType[$typeOfA, $typeOfB]($companion.$applyMethod, $companion.$unapplyMethod(_).get)"
    q"""
      if (!$registryLocation.registry.contains($registryKey)) $registryLocation.safeAddToRegistry($registryKey, $materializeMappedColumnType)
      $registryLocation.registry($registryKey).asInstanceOf[MappedColumnType[$typeOfA, $typeOfB]]
    """
  }

  def findMethod(companionType: Type, name: String, typeOfA: Type) = {
    val applyMethods = companionType.member(TermName(name)) match {
      case method: MethodSymbol => List(method)
      case termSymbol: TermSymbol => termSymbol.alternatives.collect { case method: MethodSymbol => method }
      case _ => Nil
    }

    val singleParamApplyMethods = applyMethods.filter(_.paramLists.flatten.length == 1)

    if (singleParamApplyMethods.length == 1) singleParamApplyMethods.head
    else c.abort(c.enclosingPosition, s"No matching $name method found on $typeOfA")
  }
}

object MaterializeColumnTypeMacro {
  val registry = scala.collection.mutable.Map[String, MappedColumnType[_, _]]()
  def safeAddToRegistry(key: String, columnType: MappedColumnType[_, _]): Unit = registry.synchronized {
    if (!registry.contains(key)) registry += (key -> columnType)
  }
}

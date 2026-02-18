/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.api

import ai.chronon.api.ScalaJavaConversions._

import java.util

sealed trait DataType extends Serializable

object DataType {
  def toString(dataType: DataType): String =
    dataType match {
      case IntType                       => "int"
      case LongType                      => "long"
      case DoubleType                    => "double"
      case FloatType                     => "float"
      case ShortType                     => "short"
      case BooleanType                   => "bool"
      case ByteType                      => "byte"
      case StringType                    => "string"
      case BinaryType                    => "binary"
      case ListType(elementType)         => s"list_${toString(elementType)}"
      case MapType(keyType, valueType)   => s"map_${toString(keyType)}_${toString(valueType)}"
      case DateType                      => "date"
      case TimestampType                 => "timestamp"
      case DecimalType(precision, scale) => s"decimal_${precision}_${scale}"
      case StructType(name, _)           => s"struct_$name"
      case UnknownType(_)                => "unknown_type"
    }

  def isScalar(dt: DataType): Boolean =
    dt match {
      case IntType | LongType | ShortType | DoubleType | FloatType | StringType | BinaryType | BooleanType =>
        true
      case _ => false
    }

  def isNumeric(dt: DataType): Boolean =
    dt match {
      case IntType | LongType | DoubleType | FloatType | ShortType | ByteType | DecimalType(_, _) => true
      case _                                                                                      => false
    }

  def isList(dt: DataType): Boolean =
    dt match {
      case ListType(_) => true
      case _           => false
    }

  def isMap(dt: DataType): Boolean =
    dt match {
      case MapType(_, _) => true
      case _             => false
    }

  def fromTDataType(tDataType: TDataType): DataType = {
    val typeParams = tDataType.params
    tDataType.kind match {
      // non parametric types
      case DataKind.BOOLEAN   => BooleanType
      case DataKind.BYTE      => ByteType
      case DataKind.SHORT     => ShortType
      case DataKind.INT       => IntType
      case DataKind.LONG      => LongType
      case DataKind.FLOAT     => FloatType
      case DataKind.DOUBLE    => DoubleType
      case DataKind.STRING    => StringType
      case DataKind.BINARY    => BinaryType
      case DataKind.DATE      => DateType
      case DataKind.TIMESTAMP => TimestampType
      case DataKind.DECIMAL => {
        assert(typeParams != null && typeParams.size() == 2,
               s"TDataType needs non null `params` with length 2 when kind == DECIMAL. Given: $typeParams")
        val precision = typeParams.get(0).name.toInt
        val scale = typeParams.get(1).name.toInt
        require(precision > 0, s"Decimal precision must be > 0, got $precision")
        require(scale >= 0 && scale <= precision,
                s"Decimal scale must be between 0 and precision. precision=$precision scale=$scale")
        DecimalType(precision, scale)
      }

      // parametric types
      case DataKind.MAP => {
        assert(typeParams != null && typeParams.size() == 2,
               s"TDataType needs non null `params` with length 2 when kind == MAP. Given: $typeParams")
        MapType(fromTDataType(typeParams.get(0).dataType), fromTDataType(typeParams.get(1).dataType))
      }
      case DataKind.LIST => {
        assert(typeParams != null && typeParams.size() == 1,
               s"TDataType needs non null `params` with length 1 when kind == LIST. Given: $typeParams")
        ListType(fromTDataType(typeParams.get(0).dataType))
      }
      case DataKind.STRUCT => {
        assert(typeParams != null && !typeParams.isEmpty,
               s"TDataType needs non null `params` with non-zero length when kind == Struct. Given: $typeParams")
        val fields = typeParams.toScala
          .map(param => StructField(param.name, fromTDataType(param.dataType)))
        StructType(tDataType.name, fields.toArray)
      }
    }
  }

  def toTDataType(dataType: DataType): TDataType = {
    def toParams(params: (String, DataType)*): util.List[DataField] = {
      params
        .map { case (name, dType) =>
          new DataField().setName(name).setDataType(toTDataType(dType))
        }
        .toList
        .toJava
    }
    dataType match {
      case IntType               => new TDataType(DataKind.INT)
      case LongType              => new TDataType(DataKind.LONG)
      case DoubleType            => new TDataType(DataKind.DOUBLE)
      case FloatType             => new TDataType(DataKind.FLOAT)
      case ShortType             => new TDataType(DataKind.SHORT)
      case BooleanType           => new TDataType(DataKind.BOOLEAN)
      case ByteType              => new TDataType(DataKind.BYTE)
      case StringType            => new TDataType(DataKind.STRING)
      case BinaryType            => new TDataType(DataKind.BINARY)
      case ListType(elementType) => new TDataType(DataKind.LIST).setParams(toParams("elem" -> elementType))
      case MapType(keyType, valueType) =>
        new TDataType(DataKind.MAP).setParams(toParams("key" -> keyType, "value" -> valueType))
      case DateType      => new TDataType(DataKind.DATE)
      case TimestampType => new TDataType(DataKind.TIMESTAMP)
      case DecimalType(precision, scale) => {
        val params = List(
          new DataField().setName(precision.toString).setDataType(new TDataType(DataKind.INT)),
          new DataField().setName(scale.toString).setDataType(new TDataType(DataKind.INT))
        ).toJava
        new TDataType(DataKind.DECIMAL).setParams(params)
      }
      case StructType(name, fields) =>
        new TDataType(DataKind.STRUCT).setName(name).setParams(toParams(fields.map(f => f.name -> f.fieldType): _*))
      case UnknownType(_) => throw new RuntimeException("Cannot convert unknown type")
    }
  }

  private def toJLong(l: Long): java.lang.Long = java.lang.Long.valueOf(l)
  private def toJDouble(d: Double): java.lang.Double = java.lang.Double.valueOf(d)
  private def toJInt(i: Int): java.lang.Integer = java.lang.Integer.valueOf(i)

  def castToLong(value: AnyRef): AnyRef =
    value match {
      case i: java.lang.Long    => i
      case i: java.lang.Integer => toJLong(i.longValue())
      case i: java.lang.Short   => toJLong(i.longValue())
      case i: java.lang.Byte    => toJLong(i.longValue())
      case i: java.lang.Double  => toJLong(i.longValue())
      case i: java.lang.Float   => toJLong(i.longValue())
      case i: java.lang.String  => toJLong(java.lang.Long.parseLong(i))
      case _                    => value
    }

  def castToInt(value: AnyRef): AnyRef =
    value match {
      case i: java.lang.Integer => i
      case i: java.lang.Long    => toJInt(i.intValue())
      case i: java.lang.Short   => toJInt(i.intValue())
      case i: java.lang.Byte    => toJInt(i.intValue())
      case i: java.lang.Double  => toJInt(i.intValue())
      case i: java.lang.Float   => toJInt(i.intValue())
      case i: java.lang.String  => toJInt(java.lang.Integer.parseInt(i))
      case _                    => value
    }

  def castToDouble(value: AnyRef): AnyRef =
    value match {
      case i: java.lang.Double  => i
      case i: java.lang.Integer => toJDouble(i.doubleValue())
      case i: java.lang.Short   => toJDouble(i.doubleValue())
      case i: java.lang.Byte    => toJDouble(i.doubleValue())
      case i: java.lang.Float   => toJDouble(i.doubleValue())
      case i: java.lang.Long    => toJDouble(i.doubleValue())
      case i: java.lang.String  => toJDouble(java.lang.Double.parseDouble(i))
      case _                    => value
    }

  def castToDecimal(value: AnyRef): AnyRef =
    value match {
      case bd: java.math.BigDecimal => bd
      case d: java.lang.Double      => java.math.BigDecimal.valueOf(d)
      case f: java.lang.Float       => java.math.BigDecimal.valueOf(f.doubleValue())
      case i: java.lang.Integer     => new java.math.BigDecimal(i)
      case s: java.lang.Short       => new java.math.BigDecimal(s.intValue())
      case b: java.lang.Byte        => new java.math.BigDecimal(b.intValue())
      case l: java.lang.Long        => new java.math.BigDecimal(l)
      case bi: java.math.BigInteger => new java.math.BigDecimal(bi)
      case s: java.lang.String      => new java.math.BigDecimal(s)
      case _                        => value
    }

  def castTo(value: AnyRef, typ: DataType): AnyRef = {
    if (value == null) {
      return null
    }

    typ match {
      case LongType                      => castToLong(value)
      case DoubleType                    => castToDouble(value)
      case IntType                       => castToInt(value)
      case StringType                    => if (value.isInstanceOf[String]) value else value.toString
      case DecimalType(precision, scale) => castToDecimal(value)
      case _                             => value
    }

  }
}

case object IntType extends DataType

case object LongType extends DataType

case object DoubleType extends DataType

case object FloatType extends DataType

case object ShortType extends DataType

case object BooleanType extends DataType

case object ByteType extends DataType

case object StringType extends DataType

// maps to Array[Byte]
case object BinaryType extends DataType

// maps to java.util.ArrayList[ElementType]
case class ListType(elementType: DataType) extends DataType

// maps to java.util.Map[KeyType, ValueType]
case class MapType(keyType: DataType, valueType: DataType) extends DataType

case class StructField(name: String, fieldType: DataType)

// maps to java.sql.Date
// maps to java.time.LocalDate if DATETIME_JAVA8API_ENABLED is true
case object DateType extends DataType

// maps to java.sql.Timestamp
// maps to java.time.Instant if DATETIME_JAVA8API_ENABLED is true for java8. See spark doc:
// ```
// If the configuration property is set to true, java.time.Instant and java.time.LocalDate classes of Java
// 8 API are used as external types for Catalyst's TimestampType and DateType. If it is set to false,
// java.sql.Timestamp and java.sql.Date are used for the same purpose.
// ```
case object TimestampType extends DataType

// maps to java.math.BigDecimal
case class DecimalType(precision: Int, scale: Int) extends DataType

// maps to Array[Any]
case class StructType(name: String, fields: Array[StructField]) extends DataType with Seq[StructField] {
  def unpack: Seq[(String, DataType)] = fields.map { field => field.name -> field.fieldType }

  override def apply(idx: Int): StructField = fields(idx)
  override def length: Int = fields.length

  override def iterator: Iterator[StructField] = fields.iterator
  override def stringPrefix: String = this.getClass.getSimpleName
  def typeOf(name: String): Option[DataType] = fields.find(_.name == name).map(_.fieldType)

  def castArr(valueMap: Map[String, AnyRef]): Array[AnyRef] = {
    fields.map { case StructField(name, typ) =>
      val elem = valueMap.getOrElse(name, null)
      // handle cases where a join contains keys of the same name but different types
      // e.g. `listing` is a long in one groupby, but a string in another groupby
      DataType.castTo(elem, typ)
    }
  }

  def cast(valueMap: Map[String, AnyRef]): Map[String, AnyRef] = {
    fields.map { case StructField(name, typ) =>
      val elem = valueMap.getOrElse(name, null)
      // handle cases where a join contains keys of the same name but different types
      // e.g. `listing` is a long in one groupby, but a string in another groupby
      name -> DataType.castTo(elem, typ)
    }.toMap
  }
}

object StructType {
  def from(name: String, fields: Array[(String, DataType)]): StructType = {
    StructType(name, fields.map { case (fieldName, dataType) => StructField(fieldName, dataType) })
  }
}

// mechanism to accept unknown types into the ai.chronon.aggregator.row
// while retaining the original type object for reconstructing the source type information
case class UnknownType(any: Any) extends DataType

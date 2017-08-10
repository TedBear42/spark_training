import com.google.common.base.CaseFormat
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}

import scala.collection.mutable

object CaseClassGenerator {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val globalFieldCounter = new mutable.HashMap[String, Int]()

  def main(args:Array[String]): Unit = {
    val isLocal = args(0).equalsIgnoreCase("l")
    val tableToGenerateFrom = args(1)

    val sparkSession = if (isLocal) {
      SparkSession.builder
        .master("local")
        .appName("my-spark-app")
        .config("spark.some.config.option", "config-value")
        .config("spark.driver.host","127.0.0.1")
        .config("spark.sql.parquet.compression.codec", "gzip")
        .enableHiveSupport()
        .getOrCreate()
    } else {
      SparkSession.builder
        .appName("my-spark-app")
        .config("spark.some.config.option", "config-value")
        .enableHiveSupport()
        .getOrCreate()
    }

    val emptyDf = sparkSession.sql("select * from " + tableToGenerateFrom + " limit 0")

    val rootSchema = emptyDf.schema

    createCaseClass(tableToGenerateFrom, rootSchema)

    sparkSession.close()
  }

  def createCaseClass(className:String,
                      structType: StructType): Unit = {
    val strBuilder = new StringBuilder("case class " + className + "(")
    var isFirst = true

    structType.fields.foreach(field => {

      if (isFirst) {
        isFirst = false
      } else {
        strBuilder.append(",\n\t")
      }

      val dataType = field.dataType

      if (dataType.isInstanceOf[ArrayType]) {

        var arrayType = field.dataType.asInstanceOf[ArrayType]
        var arrayStartString = "Array["
        var arrayEndString = "]"

        while (arrayType.elementType.isInstanceOf[ArrayType]) {
          arrayType = arrayType.elementType.asInstanceOf[ArrayType]
          arrayStartString += arrayStartString
          arrayEndString += arrayEndString
        }

        if (arrayType.elementType.isInstanceOf[StructType]) {
          val nestedStructType = arrayType.elementType.asInstanceOf[StructType]

          var nestedClassName: String = generateFieldName(field)


          createCaseClass(nestedClassName, nestedStructType)
          strBuilder.append(field.name + ":" + arrayStartString + nestedClassName + arrayEndString)
        } else {
          var fieldType: String = getJavaFieldName(arrayType.elementType)
          strBuilder.append(field.name + ":" + arrayStartString + fieldType + arrayEndString)
        }
      } else if (dataType.isInstanceOf[StructType]) {
        val nestedStructType = dataType.asInstanceOf[StructType]

        var nestedClassName: String = generateFieldName(field)

        createCaseClass(nestedClassName, nestedStructType)
        strBuilder.append(field.name + ":" + nestedClassName )
      } else {
        var fieldType: String = getJavaFieldName(field.dataType)
        strBuilder.append(field.name + ":" + fieldType)
      }
    })
    strBuilder.append(")\n")

    println(strBuilder.toString())
  }

  def generateFieldName(field: StructField): String = {
    var nestedClassName = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, field.name)
    val fieldCount = globalFieldCounter.getOrElse(nestedClassName, 0)
    globalFieldCounter.put(nestedClassName, fieldCount + 1)
    if (fieldCount == 0) {
      nestedClassName
    } else {
      nestedClassName + "_" + fieldCount
    }
  }

  def getJavaFieldName(dataType:DataType): String = {
    val fieldClassName = dataType.getClass.getSimpleName.replace("$", "")

    try {
      var fieldType: String = null
      fieldClassName match {
        case "StringType" => fieldType = "String"
        case "IntegerType" => fieldType = "Integer"
        case "LongType" => fieldType = "Long"
        case "DoubleType" => fieldType = "Double"
        case "FloatType" => fieldType = "Float"
        case "BooleanType" => fieldType = "Boolean"
        case "DecimalType" => fieldType = "Decimal"
        case "TimestampType" => fieldType = "Timestamp"
      }
      fieldType
    } catch {
      case e: Exception => {
        println(dataType)
        throw e
      }
    }
  }
}

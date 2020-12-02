package scratch

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructField, StructType}
import org.apache.spark.sql.{SparkSession, functions}

object StackRunner {
  def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
          .appName("Spark SQL")
          .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
          //.master("local[4]")
          .getOrCreate()

        import spark.implicits._

    //spark.sparkContext.textFile("s3a://adam-king-848/example/stack.json").saveAsTextFile("s3a://adam-king-848/example/stack-copy.json")

    val df = spark.read.option("multiline", true).json("s3://adam-king-848/example/stack.json")

   // val df = spark.read.option("multiline", true).json("stack.json")

    df.show(10)
    val rotatedf = df.select(functions.expr(s"stack(${df.columns.length}, ${df.columns.mkString(",")})"))

    //rotatedf.show()

    df.columns.map(df(_))

    val cols = explodeNestedFieldNames(rotatedf.schema)

    //rotatedf.select(cols.head, cols.tail : _*).show()

    println(explodeNestedFieldNames(rotatedf.schema))

    //rotatedf.select(cols.head, cols.tail :_*).write.parquet("stackout.parquet")
    //rotatedf.select(cols.head, cols.tail :_*).write.parquet("s3a://AKIA4OK5FKIYZHUPBRV5:zdZL7hDA7l2N+babooWsD4F2zx1/KECFWr8SDB3u@adam-king-848/example/out/stackout.parquet")

  }

  /*
  This function comes from DataBricks and is under their copyright + under Apache 2.0 license
  The code was taken from
  https://github.com/delta-io/delta/blob/f72bb4147c3555b9a0f571b35ac4d9a41590f90f/src/main/scala/org/apache/spark/sql/delta/schema/SchemaUtils.scala#L123
   */
  def explodeNestedFieldNames(schema: StructType): Seq[String] = {
    def explode(schema: StructType): Seq[Seq[String]] = {
      def recurseIntoComplexTypes(complexType: DataType): Seq[Seq[String]] = {
        complexType match {
          case s: StructType => explode(s)
          case a: ArrayType => recurseIntoComplexTypes(a.elementType)
          case m: MapType =>
            recurseIntoComplexTypes(m.keyType).map(Seq("key") ++ _) ++
              recurseIntoComplexTypes(m.valueType).map(Seq("value") ++ _)
          case _ => Nil
        }
      }

      schema.flatMap {
        case StructField(name, s: StructType, _, _) =>
          Seq(Seq(name)) ++ explode(s).map(nested => Seq(name) ++ nested)
        case StructField(name, a: ArrayType, _, _) =>
          Seq(Seq(name)) ++ recurseIntoComplexTypes(a).map(nested => Seq(name) ++ nested)
        case StructField(name, m: MapType, _, _) =>
          Seq(Seq(name)) ++ recurseIntoComplexTypes(m).map(nested => Seq(name) ++ nested)
        case f => Seq(f.name) :: Nil
      }
    }

    explode(schema).map(UnresolvedAttribute.apply(_).name)
  }
}

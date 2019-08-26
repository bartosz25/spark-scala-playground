package org.apache.spark.sql.types

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.Countries.Countries
import org.apache.spark.unsafe.types.UTF8String

object Countries extends Enumeration {
  type Countries = Value
  val France, England, Poland = Value
}

@SQLUserDefinedType(udt = classOf[CityUDT])
case class City(name: String, country: Countries) {
  def isFrench: Boolean = country == Countries.France
}

class CityUDT extends UserDefinedType[City] {
  override def sqlType: DataType = StructType(Seq(StructField("name", StringType),
    StructField("region", StringType)))

  override def serialize(city: City): Any = {
    val row = new GenericInternalRow(2)
    row.update(CityUDT.NameIndex, UTF8String.fromString(city.name))
    row.update(CityUDT.RegionIndex, UTF8String.fromString(city.country.toString))
    row
  }

  override def deserialize(datum: Any): City = {
    val datumRow = datum.asInstanceOf[InternalRow]
    City(datumRow.getUTF8String(CityUDT.NameIndex).toString,
      Countries.withName(datumRow.getUTF8String(CityUDT.RegionIndex).toString))
  }

  override def userClass: Class[City] = classOf[City]
}

object CityUDT {
  private val NameIndex = 0
  private val RegionIndex = 1
}

case class CityNotUDT(name: String, region: Countries) {
  def isFrench: Boolean = region == Countries.France
}

object TestUDT {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("UDT test").master("local[*]")
      .getOrCreate()
    /*UDTRegistration.register("org.apache.spark.sql.types.City",
      "org.apache.spark.sql.types.CityUDT")*/

    import sparkSession.implicits._
    val cities = Seq(City("Paris", Countries.France), City("London", Countries.England)).toDF("city")

    // Doesn't work
    // Exception in thread "main" org.apache.spark.sql.AnalysisException: cannot resolve '`isFrench`' given input columns: [value]; line 1 pos 0;
    // 'Filter ('isFrench = true)
    // +- LocalRelation [value#1]
    // cities.where("isFrench = true").show()
    cities.printSchema()
    cities.show()
    cities.select("city").show()
    cities.select("city").printSchema()
    //cities.filter(city => city.isFrench).show()
    println("isFrench?")
    cities.map(row => row.getAs[City]("city").isFrench).show()
    println("Which region?")
    cities.map(row => row.getAs[City]("city").country.toString).show()
    // Doesn't work
    // Exception in thread "main" org.apache.spark.sql.AnalysisException:
    // Can't extract value from city#3: need struct type but got struct<name:string,region:string>; line 1 pos 0
    cities.where("city.name == 'Paris'").show()

    // This also is not good:
    // cities.select($"city".cast("city")).where("city.name == 'Paris'").show()
    // Because of: DataType city is not supported.(line 1, pos 0)



    println("Dataset")
    val citiesDs = Seq(City("Paris", Countries.France), City("London", Countries.England)).toDS()
    println("isFrench?")
    citiesDs.map(row => row.isFrench).show()
    println("Which region?")
    citiesDs.map(row => row.country.toString).show()

    println("DataFrame for CityNotUDT")
    val citiesNotUdt = Seq(CityNotUDT("Paris", Countries.France), CityNotUDT("London", Countries.England))
      .toDF("city")
    citiesNotUdt.map(row => row.getAs[City]("city").isFrench).show()
  }
}
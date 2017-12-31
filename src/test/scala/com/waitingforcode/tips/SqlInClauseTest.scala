package com.waitingforcode.tips

import java.sql.PreparedStatement

import com.waitingforcode.util.InMemoryDatabase
import com.waitingforcode.util.sql.data.DataOperation
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}


class SqlInClauseTest extends FlatSpec with Matchers with BeforeAndAfterAll  {

  private val sparkSession: SparkSession = SparkSession.builder()
    .appName("Spark SQL IN tip").master("local[*]").getOrCreate()

  override def beforeAll {
    case class CountryOperation(isoCode: String, name: String) extends DataOperation {
      override def populatePreparedStatement(preparedStatement: PreparedStatement): Unit = {
        preparedStatement.setString(1, isoCode)
        preparedStatement.setString(2, name)
      }
    }
    val countriesToInsert = Seq(CountryOperation("FR", "France"), CountryOperation("PL", "Poland"),
      CountryOperation("GB", "The United Kingdom"), CountryOperation("HT", "Haiti"), CountryOperation("JM", "Jamaica")
    )

    InMemoryDatabase.createTable("CREATE TABLE countries(iso VARCHAR(20) NOT NULL, countryName VARCHAR(20) NOT NULL)")
    InMemoryDatabase.populateTable("INSERT INTO countries (iso, countryName) VALUES (?, ?)", countriesToInsert)
  }

  override def afterAll {
    sparkSession.stop()
    InMemoryDatabase.cleanDatabase()
  }

  "SQL IN clause" should "be used to filter some rows" in {
    val countriesReader = sparkSession.read.format("jdbc")
      .option("url", InMemoryDatabase.DbConnection)
      .option("driver", InMemoryDatabase.DbDriver)
      .option("dbtable", "countries")
      .option("user", InMemoryDatabase.DbUser)
      .option("password", InMemoryDatabase.DbPassword)
      .load()

    import sparkSession.implicits._
    val europeanCountries: Array[String] = countriesReader.select("iso", "countryName")
      .where($"iso".isin("FR", "PL", "GB"))
      .map(row => row.getString(1))
      .collect()

    europeanCountries should have length 3
    europeanCountries should contain allOf("France", "Poland", "The United Kingdom")
  }

  "SQL IN clause" should "be applied for in-memory DataFrame" in {
    import sparkSession.implicits._
    val countriesDataFrame = Seq(
      ("FR", "France"), ("DE", "Germany"), ("CA", "Canada"), ("BR", "Brazil"), ("AR", "Argentina")
    ).toDF("iso", "countryName")

    val europeanCountries: Array[String] = countriesDataFrame.select("iso", "countryName")
      .where($"iso".isin("FR", "DE"))
      .map(row => row.getString(1))
      .collect()

    europeanCountries should have length 2
    europeanCountries should contain allOf("France", "Germany")

  }

}

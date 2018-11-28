package com.waitingforcode.sql

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class CorrelatedSubqueryTest extends FlatSpec with BeforeAndAfterAll with Matchers {

  private val SparkLocalSession = SparkSession.builder().appName("Correlated subquery test")
    .master("local[*]")
    .getOrCreate()

  import SparkLocalSession.implicits._

  private val SportId = 1
  private val GlobalNewsId = 2
  private val ArticlesDataFrame = Seq(
    Article(1, SportId, "News about football", 30), Article(2, SportId, "News about tennis", 10),
    Article(3, GlobalNewsId, "Global news 1", 20), Article(4, GlobalNewsId, "Global news 2", 40)
  ).toDF
  private val CategoriesDataFrame = Seq(
    Category(SportId, 2), Category(GlobalNewsId, 2)
  ).toDF
  ArticlesDataFrame.createOrReplaceTempView("articles")
  CategoriesDataFrame.createOrReplaceTempView("categories")

  behavior of "correlated subquery"

  it should "be used in the projection" in {
    val sqlResult = SparkLocalSession.sql(
      """
        |SELECT a.title, a.categoryId, (
        | SELECT FIRST(c.allArticles) FROM categories c WHERE c.categoryId = a.categoryId
        |) AS allArticlesInCategory FROM articles a
      """.stripMargin)

    val mappedResults = sqlResult.collect().map(row => s"${row.getString(0)};${row.getInt(1)};${row.getInt(2)}")

    mappedResults should have size 4
    mappedResults should contain allOf("News about football;1;2", "News about tennis;1;2",
      "Global news 1;2;2", "Global news 2;2;2")
  }

  it should "should execute aggregation" in {
    val sqlResult = SparkLocalSession.sql(
      """
        |SELECT title, views FROM articles a WHERE a.views > (
        | SELECT AVG(views) AS avg_views_category FROM articles WHERE categoryId = a.categoryId
        | GROUP BY categoryId
        |)
      """.stripMargin)

    val mappedResults = sqlResult.collect().map(row => s"${row.getString(0)};${row.getInt(1)}")

    mappedResults should have size 2
    mappedResults should contain allOf("News about football;30", "Global news 2;40")
  }

}

case class Article(articleId: Int, categoryId: Int, title: String, views: Int)
case class Category(categoryId: Int, allArticles: Int)
package com.waitingforcode.sql

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{FlatSpec, Matchers}
import org.apache.spark.sql.functions._

class HigherOrderFunctionsTest extends FlatSpec with Matchers {

  private val TestedSparkSession: SparkSession = SparkSession.builder()
    .appName("Higher-order test").master("local[*]").getOrCreate()
  import TestedSparkSession.implicits._
  private val testedDataset = Seq((1L, Array("a", "b", "d", "e", "c"))).toDF("id", "letters")

  private def stringifyResult(result: DataFrame, arrayColName: String): Seq[String] = {
    result.collect().map(row => row.getAs[List[String]](arrayColName).mkString(", "))
  }

  "array_sort" should "sort the letters in ascending order" in {
    val testedDataset = Seq((Array("a", "b", "d", "e", "c"))).toDF("letters")

    val sortedLetters = testedDataset.select(sort_array($"letters", asc = true).as("sorted_letters"))

    val functionResult = stringifyResult(sortedLetters, "sorted_letters")
    functionResult should have size 1
    functionResult(0) shouldEqual "a, b, c, d, e"
    sortedLetters.explain(true)
  }

  "array_max" should "get the max letter of the array" in {
    val testedDataset = Seq((Array("a", "b", "d", "e", "c"))).toDF("letters")

    val maxLetters = testedDataset.select(array_max($"letters").as("max_letter"))

    val functionResult = maxLetters.collect().map(row => row.getAs[String]("max_letter"))
    functionResult should have size 1
    functionResult(0) shouldEqual "e"
    maxLetters.explain(true)
  }

  "array_min" should "get the min letter of the array" in {
    val testedDataset = Seq((Array("a", "b", "d", "e", "c"))).toDF("letters")

    val minLetters = testedDataset.select(array_min($"letters").as("min_letter"))

    val functionResult = minLetters.collect().map(row => row.getAs[String]("min_letter"))
    functionResult should have size 1
    functionResult(0) shouldEqual "a"
    minLetters.explain(true)
  }

  "array_position" should "get return the position of existent and missing letters" in {
    val testedDataset = Seq((Array("a", "b", "d", "e", "c"))).toDF("letters")

    val positions = testedDataset.select(array_position($"letters", "b").as("b_position"),
      array_position($"letters", "z").as("z_position"))

    val functionResult = positions.collect().map(row => (row.getAs[Long]("b_position"), row.getAs[Long]("z_position")))
    functionResult should have size 1
    // As you can see, the position is not 0-based, 0 is used to mark the missing index
    functionResult(0) shouldEqual (2, 0)
    positions.explain(true)
  }

  "array_remove" should "remove the 'a' letter" in {
    val testedDataset = Seq((Array("a", "b", "c", "d", "a"))).toDF("letters")

    val lettersWithoutA = testedDataset.select(array_remove($"letters", "a").as("letters_without_a"))

    val functionResult = stringifyResult(lettersWithoutA, "letters_without_a")
    functionResult should have size 1
    functionResult(0) shouldEqual "b, c, d"
    lettersWithoutA.explain(true)
  }

  "element_at" should "return the letters at specific positions" in {
    val testedDataset = Seq((Array("a", "b", "c", "d"))).toDF("letters")

    val lettersFromPositions = testedDataset.select(element_at($"letters", 1).as("letter2"),
      element_at($"letters", 5).as("letter6"))

    val functionResult = lettersFromPositions.collect().map(row => (row.getAs[String]("letter2"), row.getAs[String]("letter6")))
    functionResult should have size 1
    functionResult(0) shouldEqual ("a", null)
    lettersFromPositions.explain(true)
  }

  "element_at" should "return the value from the map" in {
    val testedDataset = Seq((Map("a" -> 3)), (Map("b" -> 4))).toDF("letters")

    val lettersFromPositions = testedDataset.select(element_at($"letters", "a").as("value_of_a"))

    val functionResult = lettersFromPositions.collect().map(row => {
      if (row.isNullAt(row.fieldIndex("value_of_a"))) {
        null
      } else {
        row.getAs[Int]("value_of_a")
      }
    })
    functionResult should have size 2
    functionResult should contain allOf(3, null)
    lettersFromPositions.explain(true)
  }

  "flatten" should "transform nested Arrays to one-level Array" in {
    val testedDatasetWithArrayOfArrays = Seq((1L, Array(Array("a", "b"), Array("c", "d", "e")))).toDF("id", "letters")

    val flattenedLetters = testedDatasetWithArrayOfArrays.select(flatten($"letters").as("flattened_letters"))

    val functionResult = stringifyResult(flattenedLetters, "flattened_letters")
    functionResult should have size 1
    functionResult(0) shouldEqual "a, b, c, d, e"
    flattenedLetters.explain(true)
  }

  "reverse" should "return the array from the end" in {
    val testedDataset = Seq((Array("a", "b", "c", "d"))).toDF("letters")

    val reversedArrayLetters = testedDataset.select(reverse($"letters").as("reversed_letters"))

    val functionResult = stringifyResult(reversedArrayLetters, "reversed_letters")
    functionResult should have size 1
    functionResult(0) shouldEqual "d, c, b, a"
    reversedArrayLetters.explain(true)
  }

  "sequence" should "create an array of numbers from a range with a step" in {
    val testedDatasetWithArrayOfArrays = Seq((2L, 10L)).toDF("nr1", "nr2")
    val sequenceNumbers = testedDatasetWithArrayOfArrays.select(sequence($"nr1", $"nr2", $"nr1").as("numbers"))

    val functionResult = sequenceNumbers.collect().map(row => row.getAs[List[Int]]("numbers").mkString(", "))
    functionResult should have size 1
    functionResult(0) shouldEqual "2, 4, 6, 8, 10"
    sequenceNumbers.explain(true)
  }

  "shuffle" should "return a random permutation of the array" in {
    val testedDataset = Seq((Array("a", "b", "c", "d"))).toDF("letters")

    val shuffledLetters1 = testedDataset.select(shuffle($"letters").as("shuffled_letters"))
    val shuffledLetters2 = testedDataset.select(shuffle($"letters").as("shuffled_letters"))

    val functionResult1 = stringifyResult(shuffledLetters1, "shuffled_letters")
    val functionResult2 = stringifyResult(shuffledLetters2, "shuffled_letters")
    functionResult1 should have size 1
    functionResult2 should have size 1
    functionResult1(0) shouldNot equal(functionResult2(0))
    shuffledLetters1.explain(true)
  }

  "slice" should "return the specified part of the array" in {
    val testedDataset = Seq((Array("a", "b", "c", "d"))).toDF("letters")

    val slicedLetters = testedDataset.select(slice($"letters", 2, 3).as("sliced_letters"))

    val functionResult = stringifyResult(slicedLetters, "sliced_letters")
    functionResult should have size 1
    functionResult(0) shouldEqual "b, c, d"
    slicedLetters.explain(true)
  }

  "cardinality" should "return the size of the arrays" in {
    val testedDataset = Seq((Array("a", "b", "c", "d")), (Array("a", "a"))).toDF("letters")

    val lettersWithCardinality = testedDataset.selectExpr("cardinality(letters) AS letters_number")

    val functionResult = lettersWithCardinality.collect().map(row => row.getAs[Int]("letters_number"))
    functionResult should have size 2
    functionResult should contain allOf(4, 2)
    lettersWithCardinality.explain(true)
  }

  "array_repeat" should "create arrays with letter repeated twice" in {
    val lettersDataset = Seq(("a"), ("b")).toDF("letter")
    val repeatedLetters = lettersDataset.select(array_repeat($"letter", 2).as("repeated_letters"))

    val functionResult = stringifyResult(repeatedLetters, "repeated_letters")
    functionResult should have size 2
    functionResult should contain allOf("a, a", "b, b")
    repeatedLetters.explain(true)
  }

  "array_distinct" should "create an array without duplicates" in {
    val lettersDataset = Seq((Array("a", "a", "b", "c", "b", "a"))).toDF("duplicated_letters")
    val distinctLetters = lettersDataset.select(array_distinct($"duplicated_letters").as("distinct_letters"))

    val functionResult = stringifyResult(distinctLetters, "distinct_letters")
    functionResult should have size 1
    functionResult(0) shouldEqual "a, b, c"
    distinctLetters.explain(true)
  }

  "array_intersect" should "return distinct common letters of 2 arrays" in {
    // a = 4 times, a = 3 times
    val lettersDataset = Seq((Array("a", "a", "a", "a", "b", "c", "b"), Array("a", "a", "a", "b", "d", "e")))
      .toDF("letters1", "letters2")
    val commonLetters = lettersDataset.select(array_intersect($"letters1", $"letters2").as("common_letters"))

    val functionResult = stringifyResult(commonLetters, "common_letters")
    functionResult should have size 1
    functionResult(0) shouldEqual "a, b"
    commonLetters.explain(true)
  }

  "array_union" should "concatenate 2 arrays" in {
    val lettersDataset = Seq((Array("a", "b"), Array("c", "d"))).toDF("letters1", "letters2")
    val concatenatedArrays = lettersDataset.select(array_union($"letters1", $"letters2").as("concatenated_arrays"))

    val functionResult = stringifyResult(concatenatedArrays, "concatenated_arrays")
    functionResult should have size 1
    functionResult(0) shouldEqual "a, b, c, d"
    concatenatedArrays.explain(true)
  }

  "array_except" should "return an array with the elements of the 1st array missing in the 2nd array" in {
    val lettersDataset = Seq((Array("a", "b", "c"), Array("c", "d"))).toDF("letters1", "letters2")
    val differentLetters = lettersDataset.select(array_except($"letters1", $"letters2").as("different_letters"))

    val functionResult = stringifyResult(differentLetters, "different_letters")
    functionResult should have size 1
    functionResult(0) shouldEqual "a, b"
    differentLetters.explain(true)
  }

  "array_join" should "concatenate array's content" in {
    val lettersDataset = Seq((Array("a", "b", "c"))).toDF("letters")
    val concatenatedLetters = lettersDataset.select(array_join($"letters", ",").as("concatenated_letters"))

    val functionResult = concatenatedLetters.collect().map(row => row.getAs[String]("concatenated_letters"))
    functionResult should have size 1
    functionResult(0) shouldEqual "a,b,c"
    concatenatedLetters.explain(true)
  }

  "arrays_overlap" should "remove the a letter" in {
    val lettersDataset = Seq((Array("a", "b", "c"), Array("c", "d")),
      (Array("a", "b"), Array("c", "d"))).toDF("letters1", "letters2")
    val oneCommonEntry = lettersDataset.select(arrays_overlap($"letters1", $"letters2").as("one_common_entry_flag"))

    val functionResult = oneCommonEntry.collect().map(row => row.getAs[Boolean]("one_common_entry_flag"))
    functionResult should have size 2
    functionResult should contain allOf(true, false)
    oneCommonEntry.explain(true)
  }

  "arrays_zip" should "create arrays by mixing input arrays at the same position" in {
    // It won't work for (Array("e", "b"), Array(), Array("g", "h")) because of :
    // Array[_ <: java.lang.String] (of class scala.reflect.internal.Types$ExistentialType)
    val lettersDataset = Seq((Array("a", "b"), Array("c", "d"), Array("e", "f")), (Array("e", "b"), Array("X"), Array("g", "h")) )
      .toDF("letters1", "letters2", "letters3")
    val zippedLetters = lettersDataset.select(arrays_zip($"letters1", $"letters2", $"letters3").as("zipped_letters"))

    val functionResult = stringifyResult(zippedLetters, "zipped_letters")
    functionResult should have size 2
    functionResult should contain allOf ("[a,c,e], [b,d,f]", "[e,X,g], [b,null,h]")
    zippedLetters.explain(true)
  }

  "concat" should "concatenate 3 arrays into a single one" in {
    val lettersDataset = Seq((Array("a", "b"), Array("c", "d"), Array("e", "f"))).toDF("letters1", "letters2", "letters3")
    val concatenatedArrays = lettersDataset.select(concat($"letters1", $"letters2", $"letters3").as("concatenated_arrays"))

    val functionResult = stringifyResult(concatenatedArrays, "concatenated_arrays")
    functionResult should have size 1
    functionResult(0) shouldEqual "a, b, c, d, e, f"
    concatenatedArrays.explain(true)
  }

  "transform" should "concatenate letters with indexes" in {
      val lettersDataset = Seq((Array("a", "b", "c"))).toDF("letters")
      val transformedLetters = lettersDataset.selectExpr("transform(letters, (letter, i) -> concat(\"index \", i, \" value = \", letter))" +
        " AS transformed_letters")

      val functionResult = stringifyResult(transformedLetters, "transformed_letters")
      functionResult should have size 1
      functionResult(0) shouldEqual "index 0 value = a, index 1 value = b, index 2 value = c"
      transformedLetters.explain(true)
    }

  "filter" should "remove all letters except a" in {
    val lettersDataset = Seq((Array("a", "b", "c", "a"))).toDF("letters")
    val filteredLetters = lettersDataset.selectExpr("filter(letters, letter -> letter == 'a')" +
      " AS filtered_letters")

    val functionResult = stringifyResult(filteredLetters, "filtered_letters")
    functionResult should have size 1
    functionResult(0) shouldEqual "a, a"
    filteredLetters.explain(true)
  }

  "aggregate" should "remove the a letter" in {
    val lettersDataset = Seq((Array("a", "b", "c"))).toDF("letters")
    val aggregatedLetters = lettersDataset.selectExpr("aggregate(letters, 'letters', (lettersPrefix, letter) -> concat(lettersPrefix, ' -> ', letter))" +
      " AS aggregated_letters")

    val functionResult = aggregatedLetters.collect().map(row => row.getAs[String]("aggregated_letters"))
    functionResult should have size 1
    functionResult(0) shouldEqual "letters -> a -> b -> c"
    aggregatedLetters.explain(true)
  }

  "map_from_entries" should "create a map from an array" in {
    val lettersDataset = Seq((Array(("a", 1), ("b", 2), ("c", 3), ("d", 4)))).toDF("letters")
    val mappedArraysFromEntries = lettersDataset.select(map_from_entries($"letters").as("mapped_arrays_from_entries"))

    val functionResult = stringifyResult(mappedArraysFromEntries, "mapped_arrays_from_entries")
    functionResult should have size 1
    functionResult(0) shouldEqual "a -> 1, b -> 2, c -> 3, d -> 4"
    mappedArraysFromEntries.explain(true)
  }

  "map_from_arrays" should "create a map from 2 arrays" in {
    // It fails when 2 arrays haven't the same length with:
    // java.lang.RuntimeException: The given two arrays should have the same length
    val lettersDataset = Seq((Array("a", "b", "c"), Array("d", "e", "f"))).toDF("letters1", "letters2")
    val mappedArrays = lettersDataset.select(map_from_arrays($"letters1", $"letters2").as("mapped_arrays"))

    val functionResult = stringifyResult(mappedArrays, "mapped_arrays")
    functionResult should have size 1
    functionResult(0) shouldEqual "a -> d, b -> e, c -> f"
    mappedArrays.explain(true)
  }

  "exists" should "check whether arrays contain searched letter" in {
    val testedDataset = Seq((Array("a", "b", "c", "d")), (Array("e", "f", "g"))).toDF("letters")
    val existenceFlag = testedDataset.selectExpr("exists(letters, letter -> letter = 'a') AS existence_flag")

    val functionResult = existenceFlag.collect().map(row => row.getAs[Boolean]("existence_flag"))
    functionResult should have size 2
    functionResult should contain allOf(true, false)
    existenceFlag.explain(true)
  }

  "zip_with" should "merge 2 arrays of the same length" in {
    val lettersDataset = Seq((Array("a", "b", "c"), Array("d", "e", "f"))).toDF("letters1", "letters2")
    val zippedLetters = lettersDataset.selectExpr("zip_with(letters1, letters2,  (letter1, letter2) -> concat(letter1, ' -> ', letter2))" +
      " AS zipped_letters")

    println(zippedLetters.collect().map(ro => ro.mkString(",")).mkString("\n"))
    val functionResult = stringifyResult(zippedLetters, "zipped_letters")
    functionResult should have size 1
    functionResult(0) shouldEqual "a -> d, b -> e, c -> f"
    zippedLetters.explain(true)
  }



}

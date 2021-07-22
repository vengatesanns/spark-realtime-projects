package com.hackprotech

import com.hackprotech.PopularMoviesByRatingsRDD.{findPopularRatingMovies, loadMoviesMasterDetails}
import org.apache.spark.SparkContext
import org.scalatest.{BeforeAndAfterAll, Ignore}
import org.scalatest.funsuite.AnyFunSuite

/**
 * @author Vengatesan Nagarajan
 */
@Ignore
class PopularMoviesByRatingsRDDTest extends AnyFunSuite with BeforeAndAfterAll {

  @transient var sparkContext: SparkContext = _

  override protected def beforeAll(): Unit = {
    sparkContext = new SparkContext("local[2]", "PopularMoviesByRatingsRDDTest")
  }

  override protected def afterAll(): Unit = {
    sparkContext.stop()
  }

  test("FindPopularRatingMovies") {
    val popularMoviesRDD = findPopularRatingMovies(sparkContext, "/home/vengat/Projects/DataSets/ml-10M100K/ratings.dat")
    assert(popularMoviesRDD.filter(_._2 == 4.5).count() == 0, "popularMoviesRDD should have only popular movies which has highest ratings")
  }

  test("LoadMoivesMasterDetails") {
    val moviesMasterDetails = loadMoviesMasterDetails(sparkContext, "/home/vengat/Projects/DataSets/ml-10M100K/movies.dat")
    assert(moviesMasterDetails.count() != 0, "MoviesMasterDetails Should not be Zero")
  }

}

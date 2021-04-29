package com.hackprotech

import org.apache.log4j.Logger
import org.apache.spark.SparkContext

object PopularMoviesByRatingsRDD extends App {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName())

  // To Parse the ratings line and return tuple (MovieId, ratings)
  def parseRatings(line: String) = {
    val fieldsArr = line.split("::")
    (fieldsArr(1).toInt, fieldsArr(2).toDouble)
  }

  // To Parse the movies line and return tuple
  def parseMovies(line: String) = {
    val fieldsArr = line.split("::")
    (fieldsArr(0).toInt, fieldsArr(1))
  }

  // To Join Movie Id with Lookup Data to get movie names
  //  def mapMovieLookup(ratingLine: (Int, Double)) = {
  //    (ratingLine._1, moviesLookUp.get(ratingLine._1), ratingLine._2)
  //  }

  logger.info("Application Started....")

  val sparkContext = new SparkContext("local[2]", "PopularMoviesByRatings")

  // To read the ratings details
  val ratingsRDD = sparkContext.textFile("/home/vengat/Projects/DataSets/ml-10M100K/ratings.dat")
  val parsedRatingsRDD = ratingsRDD.map(parseRatings)
  val popularRatingsRDD = parsedRatingsRDD.filter(rating => rating._2 == 5)
    .map(movieId => (movieId._1, 1)).reduceByKey(_ + _).sortBy(_._2, false)

  // To read the movie details
  val movieRDD = sparkContext.textFile("/home/vengat/Projects/DataSets/ml-10M100K/movies.dat")
  val parsedMoviesRDD = movieRDD.map(parseMovies)

  // Movie Lookup
  val joinedRDD = popularRatingsRDD.join(parsedMoviesRDD)

  // Printing the results
  //popularRatingsRDD.take(10).foreach(println)
  logger.info("Total Ratings by user against movies: " + ratingsRDD.count())
  logger.info(joinedRDD.take(5).map(movie => (s"Movie ID: ${movie._1}, Movie Name: ${movie._2._2}")).mkString(" \n "))
  logger.info("Application Stopped.")

  // stop the sparkContext
  sparkContext.stop
}

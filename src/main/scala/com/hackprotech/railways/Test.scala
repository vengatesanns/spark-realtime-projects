package com.hackprotech.railways

object Test extends App {

  val str = "Vengat"


  implicit class StrTest(str: String) {
    def lastCharacter() = {
      str.substring(str.length - 1, str.length)
    }

    def firstCharacter() = {
      str.substring(0, 1)
    }
  }

  println(str.lastCharacter)
  println(str.firstCharacter)

}

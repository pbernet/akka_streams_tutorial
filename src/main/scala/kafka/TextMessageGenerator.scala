package kafka

import java.util.concurrent.ThreadLocalRandom

import scala.collection.mutable.ListBuffer

/**
  * Inspired by:
  * https://github.com/oel/akka-streams-text-mining/blob/master/src/main/scala/ngrams/TextMessage.scala
  *
  * Shorter implementation:
  * https://alvinalexander.com/scala/creating-random-strings-in-scala
  *
  */
object TextMessageGenerator {
  val alphabetSet: Set[Char] = ('a' to 'z').toSet
  val alphabets = alphabetSet.toList
  val vowelSet: Set[Char] = Set('a', 'e', 'i', 'o', 'u')
  val vowels = vowelSet.toList
  val consonantSet: Set[Char] = alphabetSet -- vowelSet
  val consonants = consonantSet.toList

  // Subset of Punct character class """!"#$%&'()*+,-./:;<=>?@[\]^_`{|}~"""
  val puncts: String = """.,;?!"""

  def random = ThreadLocalRandom.current

  def randomChar: Char = alphabets(random.nextInt(0, alphabets.length))

  def mostlyVowelChar: Char = {
    // 4/5th chance of vowel
    val isVowel: Boolean = if (random.nextInt(0, 5) > 0) true else false
    if (isVowel) vowels(random.nextInt(0, vowels.length)) else consonants(random.nextInt(0, consonants.length))
  }

  def maybeUpperChar: Char = {
    // 1/5th chance of uppercase
    val isUppercase: Boolean = if (random.nextInt(0, 5) == 0) true else false
    if (isUppercase) Character.toUpperCase(randomChar) else randomChar
  }

  // Generate a word within a range of lengths
  def genRandWord(minLen: Int, maxLen: Int): String = {
    var word = new ListBuffer[Char]()

    val wordLen: Int = random.nextInt(minLen, maxLen + 1)

    for (i <- 1 to wordLen) {
      val char = if (i == 1) maybeUpperChar else if (i % 2 == 0) mostlyVowelChar else randomChar
      word += char
    }

    word.mkString
  }

  def genRandTextWithKeyword(minWordsInText: Int, maxWordsInText: Int,
                  minWordLen: Int = 2, maxWordLen: Int = 8,
                  minWordsInClause: Int = 1, maxWordsInClause: Int = 10, keyword: String
                 ): String = {

    val randomLevel: Double = 0.05
    var text = new ListBuffer[String]()

    val numWordsInText: Int = random.nextInt(minWordsInText, maxWordsInText + 1)

    var wordCount: Int = 0
    var textLen: Int = 0

    while (wordCount < numWordsInText) {
      val numWords = random.nextInt(minWordsInClause, maxWordsInClause + 1)

      val numWordsInClause = if (numWordsInText - wordCount < numWords) numWordsInText - wordCount else
        numWords

      var clauseLen: Int = 0

      // Generate a clause
      for (i <- 1 to numWordsInClause) {
        val word: String = genRandWord(minWordLen, maxWordLen)
        text += word

        if (math.random < randomLevel) text += " " + keyword

        clauseLen += word.length
        wordCount += 1

        if (i < numWordsInClause) {
          text += " "
          clauseLen += 1
        }
      }

      // Add a punctuation
      text += puncts.charAt(random.nextInt(0, puncts.length)).toString
      clauseLen += 1

      if (wordCount < numWordsInText) {
        text += " "
        clauseLen += 1
      }

      textLen += clauseLen
    }

    // println(s"textLen (in chars): is $textLen")
    text.mkString
  }
}


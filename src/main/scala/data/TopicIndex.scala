package main.scala.data

/**
 * Object to get the topic index for the word occurrence map
 * */
object TopicIndex {
  /** Category index map */
  private val topicMap = Map[String, Int] (
    "Animals" -> 0,
    "Compliment" -> 1,
    "Education" -> 2,
    "Health" -> 3,
    "Heavy Emotion" -> 4,
    "Joke" -> 5,
    "Love" -> 6,
    "Politics" -> 7,
    "Religion" -> 8,
    "Science" -> 9,
    "Self" -> 10
  )

  /** Number of topics */
  val topicsNumber: Int = topicMap.size

  /**
   * Get all existent category
   *
   * @return Sequence of categories names
   */
  def getTopicSeq: Seq[String] = topicMap.keySet.toSeq

  /**
   * Get category index
   *
   * @param key Name of category
   * @return Index of category
   */
  def getIndex(key: String): Int = topicMap(key)
}

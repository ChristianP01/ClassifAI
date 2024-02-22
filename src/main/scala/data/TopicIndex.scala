package data

/** Object to get the topic index for the word occurrence map */
object TopicIndex {
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

  /** Returns a sequence of String with all topic names */
  def getTopicSeq: Seq[String] = topicMap.keySet.toSeq

  /** Get a topic index */
  def getIndex(key: String): Int = topicMap(key)
}

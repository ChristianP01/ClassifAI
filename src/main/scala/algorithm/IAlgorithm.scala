package algorithm

trait IAlgorithm {
  /**
   * Computes single attribute's entropy
   *
   * @param catCount Number of attribute's word occurrences in specific category
   * @param totalCount Total number of attribute's word occurrences
   * @param attr Word-as-column we're analyzing
   * @param category Tree's category
   * @return Entropy of an attribute
   * */
  def calcEntropy(catCount: Double, totalCount: Double, attr: String, category: String): Double = {
    this.entropyFormula(catCount / totalCount) +
      this.entropyFormula(totalCount - catCount / totalCount)
  }

  /**
   *  Computes entropy
   *
   * @param ratio Class probability
   * @return Entropy
   */
  private def entropyFormula(ratio: Double): Double = {
    - (ratio * this.log2(ratio))
  }

  /**
   * Computes attribute's gain ratio
   *
   * @param entropyA Attribute's entropy
   * @param subDFCount DataFrame count after filtered one possible value of attr
   * @param attr Attribute
   * @param totalCount DataFrame total count
   * @return Gain ratio of attribute attr
   */
  def splitAttribute(entropyA: Double, subDFCount: Double, attr: String, totalCount: Double): Double = {
    var infoA: Double = 0.0
    var splitInfoA: Double = 0.0

    // Computes info and split for every values of attribute attr (0 or > 0)
    Seq(subDFCount, totalCount - subDFCount).foreach { subCount =>
      infoA += (subCount / totalCount) * (
        this.entropyFormula(subCount / totalCount) +
          this.entropyFormula(totalCount - subCount / totalCount)
        )

      splitInfoA += this.entropyFormula(subCount / totalCount)
    }

    // Gain ratio
    (entropyA - infoA) / splitInfoA
  }

  /** Returns the majority classes among dataset */
  private def getMajorityClass(occurMap: Map[String, Seq[Double]], category: String): String = {
    var countCategories: Seq[Double] = Seq(0,0)

    occurMap.foreach( elem => {
      countCategories = (countCategories, elem._2).zipped.map(_ + _)
    })

    if (countCategories.head > countCategories(1)) category else "Other"
  }

  private def log2(num: Double): Double = {
    if (num == 0) 0 else math.log(num) / math.log(2)
  }
}
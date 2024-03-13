package algorithm

object AlgorithmUtils extends Serializable {
  def calcEntropy(catCount: Double, totalCount: Double): Double = {
    this.entropyFormula(catCount / totalCount) +
    this.entropyFormula((totalCount - catCount) / totalCount)
  }

  private def log2(num: Double): Double = {
    if (num == 0) 0 else math.log(num) / math.log(2)
  }

  def entropyFormula(ratio: Double): Double = {
    - (ratio * this.log2(ratio))
  }
}

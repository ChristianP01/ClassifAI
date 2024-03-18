package model

abstract class Node {
  def toString(prefix: String): String
}

case class LeafNode(label: String) extends Node {
  def getLabel: String = label

  override def toString(prefix: String = ""): String = {
    prefix + label + "\n"
  }
}

case class DecisionNode(attribute: String, var leftChild: Node = null, var rightChild: Node = null) extends Node {
  def getAttribute: String = attribute

  def getLeft: Node = leftChild

  def getRight: Node = rightChild

  override def toString(prefix: String = ""): String = {
    val self = prefix + attribute + "\n"
    val left = leftChild.toString(prefix + "| ")
    val right = rightChild.toString(prefix + "| ")

    self + left + right.mkString
  }
}

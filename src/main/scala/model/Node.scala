package model

abstract class Node
case class LeafNode(label: String) extends Node
case class DecisionNode(word: String, children: List[Node]) extends Node {
  def addNode(node: Node): Unit = {
    this.children :+ node
  }
}
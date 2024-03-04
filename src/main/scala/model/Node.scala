package model

abstract class Node {
  def toString(prefix: String): String
}

case class LeafNode(label: String) extends Node {
  override def toString(prefix: String = ""): String = {
    label
  }
}

case class DecisionNode(label: String, var children: Seq[Node]) extends Node {
  def addNode(node: Node): Unit = {
    this.children = this.children :+ node
  }

  // TODO: stampare più leggibile
  override def toString(prefix: String = ""): String = {
    val self = prefix + label + "\n"
    val childStrings = children.map(child => child.toString(prefix + "| "))
    self + childStrings.mkString
  }
}
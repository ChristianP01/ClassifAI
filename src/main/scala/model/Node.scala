package model

abstract class Node
case class LeafNode(label: String) extends Node
case class DecisionNode(label: String, var children: Seq[Node]) extends Node {
  def addNode(node: Node): Unit = {
    this.children = this.children :+ node
  }

  // TODO: stampare più leggibile
  override def toString: String = {
    this.label + " -> [" + this.children.toString() + "]\n"
  }
}
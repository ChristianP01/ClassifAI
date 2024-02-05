package model

abstract class Node
case class LeafNode(label: String) extends Node
case class DecisionNode(label: String, threshold: Option[Double], children: List[Node]) extends Node

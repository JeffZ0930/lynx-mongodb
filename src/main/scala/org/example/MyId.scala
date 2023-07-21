package org.example

import org.grapheco.lynx.types.property.LynxInteger
import org.grapheco.lynx.types.structural.LynxId

case class MyId(value: Long) extends LynxId {
  override def toLynxInteger: LynxInteger = LynxInteger(value)

  override def toString: String = value.toString
}

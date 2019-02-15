package com.github.tashoyan.telecom.util

import com.github.tashoyan.telecom.util.Collections.RichIterable
import org.scalatest.FunSuite

class CollectionsTest extends FunSuite {

  test("empty iterable") {
    val it: Iterable[Int] = Seq.empty
    val result = it.minOption()
    assert(result === None)
  }

  test("iterable with single element") {
    val it = Seq(5)
    val result = it.minOption()
    assert(result === Some(5))
  }

  test("iterable with multiple elements") {
    val it = Seq(5, 2, -10, 18, 7)
    val result = it.minOption()
    assert(result === Some(-10))
  }

}

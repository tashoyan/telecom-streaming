package com.github.tashoyan.telecom.util

object Collections {

  implicit class RichIterable[T](val iterable: Iterable[T]) extends AnyVal {

    @inline def minOption[C >: T]()(implicit ord: Ordering[C]): Option[T] =
      iterable.foldLeft[Option[T]](None) { (acc, elem) =>
        acc
          .map(min => if (ord.lt(elem, min)) elem else min)
          .orElse(Some(elem))
      }

  }

}

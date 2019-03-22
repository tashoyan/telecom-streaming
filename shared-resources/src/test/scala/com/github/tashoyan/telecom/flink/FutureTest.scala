package com.github.tashoyan.telecom.flink

import java.util.concurrent.{Executors, TimeUnit}

import org.scalatest.FunSuite

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, blocking}
import scala.util.Random

//TODO Remove this playground
class FutureTest extends FunSuite {

  private def longJob: String = {
    println(s"[$currentThreadId] longJob: start")
    Thread.sleep(10000L)
    println(s"[$currentThreadId] longJob: end")
    "RESULT"
  }

  private def currentThreadId: Long =
    Thread.currentThread().getId

  private def log(msg: String): Unit = {
    println(s"[$currentThreadId] $msg")
  }

  private def getQuote(currency: String): Double = {
    log(s"getQuote($currency): start")
    Thread.sleep(5000L)
    val result = Random.nextDouble()
    if (result > 0.75)
      //Simulate service failure
      throw new RuntimeException(s"Failed to get quote for $currency; failure: $result")
    log(s"getQuote($currency): end: $result")
    result
  }

  private def makeExchange(curSell: String, amountSell: Double, curBuy: String, amountBuy: Double): String = {
    log("makeExchange: start")
    Thread.sleep(5000L)
    val result = s"Sold $amountSell $curSell, bought $amountBuy $curBuy"
    log("makeExchange: end")
    result
  }

  test("future - for-comprehension") {
    import ExecutionContext.Implicits.global
    val usdRate = Future[Double] {
      getQuote("USD")
    }
    val chfRate = Future[Double] {
      getQuote("CHF")
    }
    val exchange: Future[String] = for {
      usd <- usdRate
      chf <- chfRate
      // Compilation fails: warning: parameter value chf in value $anonfun is never used
      //      if isProfitable(usd)
      //      if isProfitable(chf)
    } yield {
      makeExchange("USD", usd * 10, "CHF", chf * 10)
    }
    val transaction = exchange recover {
      case e: RuntimeException => s"Failed to make exchange: ${e.getMessage}"
    }

    log("Awaiting for transaction result")
    val transactionResult = Await.result(transaction, Duration(15000L, TimeUnit.MILLISECONDS))
    log(s"Transaction result: $transactionResult")
  }

  ignore("future - blocking job") {
    import ExecutionContext.Implicits.global
    val future: Future[String] = Future {
      blocking {
        longJob
      }
    }
    future.foreach { result =>
      println(s"[$currentThreadId] foreach: $result")
    }

    println(s"[$currentThreadId] Staring long job")
    Await.result(future, Duration(15L, TimeUnit.SECONDS))
    println(s"[$currentThreadId] Current value: ${future.value}; done.")
    Thread.sleep(1000L)
  }

  ignore("future - dedicated executor") {
    implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())
    val future: Future[String] = Future { longJob }
    future.andThen {
      case result => println(s"[$currentThreadId] andThen: $result")
    }
    future.foreach { result =>
      println(s"[$currentThreadId] foreach: $result")
    }

    println(s"[$currentThreadId] Staring long job")
    Await.result(future, Duration(15L, TimeUnit.SECONDS))
    println(s"[$currentThreadId] Current value: ${future.value}; done.")
    Thread.sleep(1000L)
  }

}

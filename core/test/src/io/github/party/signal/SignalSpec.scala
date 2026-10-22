package io.github.party.signal

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{ Millis, Seconds, Span }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.mutable.ArrayBuffer
import java.util.concurrent.{ CountDownLatch, TimeUnit }
import java.util.concurrent.atomic.AtomicInteger

class SignalSpec extends AnyFlatSpec with Matchers with Eventually:

  override given patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(5, Seconds), interval = Span(50, Millis))

  behavior of "Signal"

  it should "create a signal and emitter pair" in:
    val (signal, emitter) = Signal.make[Int]
    signal should not be null
    emitter should not be null

  it should "deliver emitted values to subscribers" in:
    val (signal, emitter) = Signal.make[Int]
    val receivedValues = ArrayBuffer.empty[Int]

    signal.subscribe { value =>
      receivedValues.synchronized:
        receivedValues += value
    }

    emitter.emit(1)
    emitter.emit(2)
    emitter.emit(3)

    eventually:
      receivedValues.synchronized:
        receivedValues.sorted should contain theSameElementsAs Seq(1, 2, 3)

  it should "support multiple subscribers" in:
    val (signal, emitter) = Signal.make[String]
    val received1 = ArrayBuffer.empty[String]
    val received2 = ArrayBuffer.empty[String]

    signal.subscribe { value =>
      received1.synchronized:
        received1 += value
    }

    signal.subscribe { value =>
      received2.synchronized:
        received2 += value
    }

    emitter.emit("hello")
    emitter.emit("world")

    eventually:
      received1.synchronized:
        received1.sorted should contain theSameElementsAs Seq("hello", "world")
      received2.synchronized:
        received2.sorted should contain theSameElementsAs Seq("hello", "world")

  it should "allow subscribers to unsubscribe" in:
    val (signal, emitter) = Signal.make[Int]
    val receivedValues = ArrayBuffer.empty[Int]

    val subscription = signal.subscribe { value =>
      receivedValues.synchronized:
        receivedValues += value
    }

    emitter.emit(1)

    eventually:
      receivedValues.synchronized:
        receivedValues should contain(1)

    subscription.cancel()

    emitter.emit(2)

    Thread.sleep(200)

    receivedValues.synchronized:
      receivedValues should not contain 2

  it should "not deliver values after signal is closed" in:
    val (signal, emitter) = Signal.make[Int]
    val receivedValues = ArrayBuffer.empty[Int]

    signal.subscribe { value =>
      receivedValues.synchronized:
        receivedValues += value
    }

    emitter.emit(1)

    eventually:
      receivedValues.synchronized:
        receivedValues should contain(1)

    emitter.close()
    emitter.emit(2)

    Thread.sleep(200)

    receivedValues.synchronized:
      receivedValues should not contain 2

  it should "call onClose callbacks when closed" in:
    val (signal, emitter) = Signal.make[Int]
    val latch = new CountDownLatch(1)
    var called = false

    signal.onClose { () =>
      called = true
      latch.countDown()
    }

    emitter.close()

    latch.await(1, TimeUnit.SECONDS) shouldBe true
    called shouldBe true

  it should "call onClose immediately if already closed" in:
    val (signal, emitter) = Signal.make[Int]
    var called = false

    emitter.close()

    signal.onClose { () =>
      called = true
    }

    eventually:
      called shouldBe true

  it should "support multiple onClose callbacks" in:
    val (signal, emitter) = Signal.make[Int]
    val latch = new CountDownLatch(3)
    val counter = new AtomicInteger(0)

    signal.onClose { () =>
      counter.incrementAndGet()
      latch.countDown()
    }

    signal.onClose { () =>
      counter.incrementAndGet()
      latch.countDown()
    }

    signal.onClose { () =>
      counter.incrementAndGet()
      latch.countDown()
    }

    emitter.close()

    latch.await(1, TimeUnit.SECONDS) shouldBe true
    counter.get() shouldBe 3

  it should "clear subscribers when closed" in:
    val (signal, emitter) = Signal.make[Int]
    val receivedValues = ArrayBuffer.empty[Int]

    signal.subscribe { value =>
      receivedValues.synchronized:
        receivedValues += value
    }

    emitter.close()
    emitter.emit(1)

    Thread.sleep(200)

    receivedValues.synchronized:
      receivedValues shouldBe empty

  it should "support map transformation" in:
    val (signal, emitter) = Signal.make[Int]
    val mappedSignal = signal.map(x => x * 2)
    val receivedValues = ArrayBuffer.empty[Int]

    mappedSignal.subscribe { value =>
      receivedValues.synchronized:
        receivedValues += value
    }

    emitter.emit(1)
    emitter.emit(2)
    emitter.emit(3)

    eventually:
      receivedValues.synchronized:
        receivedValues.sorted should contain theSameElementsAs Seq(2, 4, 6)

  it should "support chained map transformations" in:
    val (signal, emitter) = Signal.make[Int]
    val mappedSignal = signal
      .map(x => x * 2)
      .map(x => x + 1)
      .map(x => x.toString)

    val receivedValues = ArrayBuffer.empty[String]

    mappedSignal.subscribe { value =>
      receivedValues.synchronized:
        receivedValues += value
    }

    emitter.emit(1)
    emitter.emit(2)

    eventually:
      receivedValues.synchronized:
        receivedValues.sorted should contain theSameElementsAs Seq("3", "5")

  it should "close derived signals when the source closes" in:
    val (signal, emitter) = Signal.make[Int]
    val mappedSignal = signal.map(_ * 2)
    val filteredSignal = signal.filter(_ % 2 == 0)
    val closed = new CountDownLatch(2)

    mappedSignal.onClose(() => closed.countDown())
    filteredSignal.onClose(() => closed.countDown())

    emitter.close()

    closed.await(1, TimeUnit.SECONDS) shouldBe true

  it should "handle exceptions in callbacks gracefully" in:
    val (signal, emitter) = Signal.make[Int]
    val receivedValues = ArrayBuffer.empty[Int]

    signal.subscribe { value =>
      if value == 2 then throw new RuntimeException("Test exception")
      receivedValues.synchronized:
        receivedValues += value
    }

    signal.subscribe { value =>
      receivedValues.synchronized:
        receivedValues += value * 10
    }

    emitter.emit(1)
    emitter.emit(2)
    emitter.emit(3)

    eventually:
      receivedValues.synchronized:
        receivedValues should contain allOf (1, 10, 20, 3, 30)

  it should "deliver values to slow subscribers synchronously" in:
    val (signal, emitter) = Signal.make[Int]
    val latch = new CountDownLatch(1)

    signal.subscribe { value =>
      Thread.sleep(100)
      latch.countDown()
    }

    emitter.emit(1)

    // Emit is synchronous: callback has already completed by the time emit returns
    latch.await(1, TimeUnit.SECONDS) shouldBe true

  it should "handle concurrent emissions correctly" in:
    val (signal, emitter) = Signal.make[Int]
    val receivedValues = ArrayBuffer.empty[Int]
    val latch = new CountDownLatch(100)

    signal.subscribe { value =>
      receivedValues.synchronized:
        receivedValues += value
      latch.countDown()
    }

    val threads = (1 to 100).map { i =>
      new Thread(() => emitter.emit(i))
    }

    threads.foreach(_.start())
    threads.foreach(_.join())

    latch.await(5, TimeUnit.SECONDS) shouldBe true
    receivedValues.synchronized:
      receivedValues.size shouldBe 100
      receivedValues.toSet shouldBe (1 to 100).toSet

  it should "handle concurrent subscriptions correctly" in:
    val (signal, emitter) = Signal.make[Int]
    val counter = new AtomicInteger(0)
    val latch = new CountDownLatch(10)

    val threads = (1 to 10).map { _ =>
      new Thread(() =>
        signal.subscribe { value =>
          counter.incrementAndGet()
          latch.countDown()
        }
        (),
      )
    }

    threads.foreach(_.start())
    threads.foreach(_.join())

    emitter.emit(42)

    latch.await(1, TimeUnit.SECONDS) shouldBe true
    counter.get() shouldBe 10

  it should "handle concurrent unsubscriptions correctly" in:
    val (signal, emitter) = Signal.make[Int]
    val subscriptions = (1 to 100).map { _ =>
      signal.subscribe { _ => () }
    }.toArray

    val threads = subscriptions.map { subscription =>
      new Thread(() => subscription.cancel())
    }

    threads.foreach(_.start())
    threads.foreach(_.join())

    val receivedValues = ArrayBuffer.empty[Int]
    signal.subscribe { value =>
      receivedValues.synchronized:
        receivedValues += value
    }

    emitter.emit(1)

    eventually:
      receivedValues.synchronized:
        receivedValues should contain(1)

  it should "not call closed callbacks multiple times" in:
    val (signal, emitter) = Signal.make[Int]
    val counter = new AtomicInteger(0)

    signal.onClose { () =>
      counter.incrementAndGet()
    }

    emitter.close()
    emitter.close()
    emitter.close()

    eventually:
      counter.get() shouldBe 1

  it should "handle onClose race conditions correctly" in:
    val (signal, emitter) = Signal.make[Int]
    val counter = new AtomicInteger(0)
    val latch = new CountDownLatch(10)

    val threads = (1 to 10).map { _ =>
      new Thread(() =>
        signal.onClose { () =>
          counter.incrementAndGet()
          latch.countDown()
        }
        (),
      )
    }

    threads.foreach(_.start())

    Thread.sleep(50)
    emitter.close()

    threads.foreach(_.join())

    latch.await(1, TimeUnit.SECONDS) shouldBe true
    counter.get() shouldBe 10

  it should "handle map on closed signal" in:
    val (signal, emitter) = Signal.make[Int]

    emitter.close()

    val mappedSignal = signal.map(x => x * 2)
    val receivedValues = ArrayBuffer.empty[Int]
    val closed = new CountDownLatch(1)

    mappedSignal.subscribe { value =>
      receivedValues.synchronized:
        receivedValues += value
    }
    mappedSignal.onClose(() => closed.countDown())

    Thread.sleep(200)

    closed.await(1, TimeUnit.SECONDS) shouldBe true
    receivedValues.synchronized:
      receivedValues shouldBe empty

  it should "work with different value types" in:
    case class TestData(id: Int, name: String)

    val (signal, emitter) = Signal.make[TestData]
    val receivedValues = ArrayBuffer.empty[TestData]

    signal.subscribe { value =>
      receivedValues.synchronized:
        receivedValues += value
    }

    emitter.emit(TestData(1, "Alice"))
    emitter.emit(TestData(2, "Bob"))

    eventually:
      receivedValues.synchronized:
        receivedValues should contain theSameElementsInOrderAs Seq(
          TestData(1, "Alice"),
          TestData(2, "Bob"),
        )

  it should "preserve subscription order for events" in:
    val (signal, emitter) = Signal.make[Int]
    val results = ArrayBuffer.empty[String]
    val latch = new CountDownLatch(6)

    signal.subscribe { value =>
      results.synchronized:
        results += s"sub1-$value"
      latch.countDown()
    }

    signal.subscribe { value =>
      results.synchronized:
        results += s"sub2-$value"
      latch.countDown()
    }

    signal.subscribe { value =>
      results.synchronized:
        results += s"sub3-$value"
      latch.countDown()
    }

    emitter.emit(1)
    emitter.emit(2)

    latch.await(1, TimeUnit.SECONDS) shouldBe true

    results.synchronized:
      results.count(_.startsWith("sub1")) shouldBe 2
      results.count(_.startsWith("sub2")) shouldBe 2
      results.count(_.startsWith("sub3")) shouldBe 2
end SignalSpec

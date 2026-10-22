package io.github.party.signal

import io.github.party.network.Identifier
import scala.concurrent.ExecutionContext
import scala.collection.mutable
import java.util.concurrent.CountDownLatch
import scala.concurrent.Await
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import scala.collection.concurrent.TrieMap
import io.github.party.network.Network
import scala.caps.CapSet
import scala.caps.unsafe.unsafeAssumePure
import scala.concurrent.Promise
import scala.concurrent.duration.Duration

trait Emitter[V]:
  def emit(value: V): Unit
  def close(): Unit

trait Subscription:
  def cancel(): Unit

trait Signal[V]:
  def subscribe(callback: V => Unit): Subscription^{this}
  def onClose(callback: () => Unit): Unit
  def map[U](f: V => U): Signal[U]^{this, f}
  def fold[U](initial: U)(f: (U, V) -> U): U
  def filter(p: V => Boolean): Signal[V]^{this, p}
  def first: V

object Signal:
  def make[V](using ec: ExecutionContext): (Signal[V], Emitter[V]) =
    val signal = new SignallingImpl[V]
    (signal, signal)

  def signalBuilder[V](body: Emitter[V] => Unit)(using ec: ExecutionContext): Signal[V] =
    val (signal, emitter) = make[V]
    ec.execute: () =>
      body(emitter)
      emitter.close()
    signal

  def merge[V](signals: Seq[Signal[V]])(using ec: ExecutionContext): Signal[V] =
    val mergedSignal = new SignallingImpl[V]
    val subscriptions = signals.map(_.subscribe(mergedSignal.emit))
    mergedSignal.onClose(() => subscriptions.foreach(_.cancel()))
    mergedSignal

  def empty[V](using ec: ExecutionContext): Signal[V] =
    val signal = new SignallingImpl[V]
    signal.close()
    signal

  class SignallingImpl[V](using ec: ExecutionContext) extends Signal[V], Emitter[V]:

    private val subscribers = TrieMap[Long, V -> Unit]()
    private val onCloseCallbacks = TrieMap.empty[Long, () -> Unit]
    private val counter = AtomicLong(0)
    private val callbackCounter = AtomicLong(0)
    private val closed = AtomicBoolean(false)
    private val bufferLock = new AnyRef
    private val pendingValues: mutable.ArrayBuffer[V] = mutable.ArrayBuffer.empty[V] // Buffer for values emitted when no subscribers are registered

    override def first: V =
      val promise = Promise[V]()
      subscribe(promise.success(_))
      val result = Await.result(promise.future, Duration.Inf)
      close()
      result

    override def onClose(callback: () => Unit): Unit =
      if closed.get() then callback()
      else
        val id = callbackCounter.incrementAndGet()
        onCloseCallbacks += (id -> unsafeAssumePure(callback))
        // Double-check in case close() happened between the first check and adding the callback
        if closed.get() then onCloseCallbacks.remove(id).foreach(_.apply())

    override def map[U](f: V => U): Signal[U]^{this, f} =
      val mappedSignal = new SignallingImpl[U]
      val upstream = subscribe: v =>
        val u = f(v)
        mappedSignal.emit(u)
      mappedSignal.onClose(() => upstream.cancel())
      onClose(() => mappedSignal.close())
      mappedSignal

    override def filter(p: V => Boolean): Signal[V]^{this, p} =
      val filteredSignal = new SignallingImpl[V]
      val upstream = subscribe: v =>
        if p(v) then filteredSignal.emit(v)
      filteredSignal.onClose(() => upstream.cancel())
      onClose(() => filteredSignal.close())
      filteredSignal

    override def fold[U](initial: U)(f: (U, V) -> U): U =
      val promise = Promise[U]()
      var accumulator = initial
      subscribe: v =>
        accumulator = f(accumulator, v)
      onClose(() => promise.success(accumulator))
      Await.result(promise.future, Duration.Inf)

    override def close(): Unit = if closed.compareAndSet(false, true) then
      bufferLock.synchronized(pendingValues.clear())
      subscribers.clear()
      val callbacks = onCloseCallbacks.values.toList
      onCloseCallbacks.clear()
      callbacks.foreach(_.apply())

    override def emit(value: V): Unit = if !closed.get() then
      val candidates = bufferLock.synchronized:
        val subs = subscribers.values.toList
        if subs.isEmpty then
          pendingValues += value
          Nil
        else
          subs
      candidates.foreach: cb =>
        try cb(value)
        catch case _: Throwable => ()

    override def subscribe(callback: V => Unit): Subscription^{this} =
      val id = counter.incrementAndGet()
      val buffered = bufferLock.synchronized:
        subscribers += (id -> unsafeAssumePure(callback))
        val buf = pendingValues.toList
        pendingValues.clear()
        buf
      // Replay buffered values to the new subscriber
      buffered.foreach: v =>
        try callback(v)
        catch case _: Throwable => ()
      new Subscription:
        def cancel(): Unit = subscribers -= id

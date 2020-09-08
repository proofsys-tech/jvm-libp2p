package io.libp2p.simulate

import java.util.Collections
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger

interface SimPeer {

    val name: String
    val connections: List<SimConnection>

    fun start() = CompletableFuture.completedFuture(Unit)

    fun connect(other: SimPeer): CompletableFuture<SimConnection>

    fun setThroughput(througput: RandomValue)

    fun stop(): CompletableFuture<Unit> = CompletableFuture.completedFuture(Unit)
}

abstract class AbstractSimPeer : SimPeer {

    override val name = counter.getAndIncrement().toString()

    override val connections: MutableList<SimConnection> = Collections.synchronizedList(ArrayList())

    override fun connect(other: SimPeer): CompletableFuture<SimConnection> {
        return connectImpl(other).thenApply { conn ->
            val otherAbs = other as? AbstractSimPeer
            connections += conn
            otherAbs?.connections?.add(conn)
            conn.closed.thenAccept {
                connections -= conn
                otherAbs?.connections?.remove(conn)
            }
            conn
        }
    }

    override fun setThroughput(througput: RandomValue): Unit = TODO()

    abstract fun connectImpl(other: SimPeer): CompletableFuture<SimConnection>

    companion object {
        val counter = AtomicInteger()
    }
}
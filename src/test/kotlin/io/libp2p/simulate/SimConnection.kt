package io.libp2p.simulate

import io.libp2p.simulate.stats.StatsFactory
import io.libp2p.simulate.stats.WritableStats
import java.util.concurrent.CompletableFuture

interface SimConnection {

    val dialer: SimPeer
    val listener: SimPeer
    val closed: CompletableFuture<Unit>
    val dialerStat: ConnectionStat
    val listenerStat: ConnectionStat

    fun close()

    fun isClosed() = closed.isDone

    fun setLatency(latency: RandomValue): Unit = TODO()
}

data class ConnectionStat(
    val msgSize: WritableStats = StatsFactory.DUMMY,
    val msgLatency: WritableStats = StatsFactory.DUMMY
)

package io.libp2p.simulate

interface Network {

    val peers: List<SimPeer>

    val activeConnections: List<SimConnection>
        get() = peers.flatMap { it.connections }.distinct()

    val networkStats: NetworkStats
        get() = NetworkStats(
                activeConnections.map { it.dialerStat.msgSize.getCount() + it.listenerStat.msgSize.getCount() }.sum(),
                activeConnections.map { it.dialerStat.msgSize.getSum() + it.listenerStat.msgSize.getSum() }.sum().toLong()
            )

    fun resetStats()
}

data class NetworkStats(
    val msgCount: Long,
    val traffic: Long
) {
    operator fun minus(other: NetworkStats) =
        NetworkStats(msgCount - other.msgCount, traffic - other.traffic)
    operator fun plus(other: NetworkStats) =
        NetworkStats(msgCount + other.msgCount, traffic + other.traffic)
}

class ImmutableNetworkImpl(
    override val activeConnections: List<SimConnection>
) : Network {
    override val peers = activeConnections.map { it.dialer }.distinct()

    override fun resetStats() {
        activeConnections.flatMap {
            listOf(it.dialerStat, it.listenerStat)
        }.forEach {
            it.msgSize.reset()
            it.msgLatency.reset()
        }
    }
}
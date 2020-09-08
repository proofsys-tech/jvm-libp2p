package io.libp2p.simulate.topology

import io.libp2p.simulate.ImmutableNetworkImpl
import io.libp2p.simulate.Network
import io.libp2p.simulate.SimPeer
import io.libp2p.simulate.Topology
import org.jgrapht.Graph

abstract class AbstractGraphTopology : Topology {

    abstract fun <T> buildGraph(peers: List<T>): Graph<T, Any>

    override fun connect(peers: List<SimPeer>): Network {
        val graph = buildGraph(peers)
        val conns = peers
            .flatMap { graph.incomingEdgesOf(it) }
            .distinct()
            .map { graph.getEdgeSource(it).connect(graph.getEdgeTarget(it)) }
            .map { it.get() }
        return ImmutableNetworkImpl(conns)
    }
}
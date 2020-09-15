package io.libp2p.simulate.main

import io.libp2p.core.pubsub.Topic
import io.libp2p.etc.types.seconds
import io.libp2p.pubsub.gossip.GossipRouter
import io.libp2p.pubsub.gossip.builders.GossipScoreParamsBuilder
import io.libp2p.pubsub.gossip.builders.GossipTopicScoreParamsBuilder
import io.libp2p.simulate.RandomDistribution
import io.libp2p.simulate.gossip.BlocksTopic
import io.libp2p.simulate.gossip.Eth2DefaultBlockTopicParams
import io.libp2p.simulate.gossip.Eth2DefaultGossipParams
import io.libp2p.simulate.gossip.Eth2DefaultPeerScoreParams
import io.libp2p.simulate.gossip.Eth2DefaultScoreParams
import io.libp2p.simulate.gossip.Eth2DefaultTopicsParams
import io.libp2p.simulate.gossip.GossipSimPeer
import io.libp2p.simulate.gossip.SimConfig
import io.libp2p.simulate.gossip.SimNetwork
import io.libp2p.simulate.gossip.Simulation
import io.libp2p.simulate.stats.StatsFactory
import io.libp2p.simulate.topology.RandomNPeers
import io.libp2p.tools.millis
import io.libp2p.tools.propertiesAsMap
import org.junit.jupiter.api.Test
import java.time.Duration

class Simulation2 {

    fun iterateMeshMessageDeliveryWindow(values: List<Duration>) =
        values.map {
            GossipTopicScoreParamsBuilder(Eth2DefaultBlockTopicParams)
                .meshMessageDeliveryWindow(it)
                .build()
        }.map {
            Eth2DefaultTopicsParams.withTopic(BlocksTopic, it)
        }.map {
            GossipScoreParamsBuilder(Eth2DefaultScoreParams)
                .peerScoreParams(Eth2DefaultPeerScoreParams)
                .topicsScoreParams(it)
                .build()
        }

    @Test
    fun sim() {

        val simConfig = SimConfig(
            totalPeers = 1000,
            topic = Topic(BlocksTopic),
            topology = RandomNPeers(30),
            latency = RandomDistribution.uniform(5.0, 100.0),
            gossipValidationDelay = 50.millis
        )

        val gossipParams = Eth2DefaultGossipParams
        val gossipScoreParams = Eth2DefaultScoreParams
        val gossipRouterCtor = { _: Int ->
            GossipRouter(gossipParams, gossipScoreParams)
        }

        val simNetwork = SimNetwork(simConfig, gossipRouterCtor)
        println("Creating peers...")
        simNetwork.createAllPeers()
        println("Connecting peers...")
        simNetwork.connectAllPeers()

        println("Creating simulation...")
        val simulation = Simulation(simConfig, simNetwork)
        println("Publishing message simulation...")
        simulation.publishMessage(1)
        println("Wrapping up...")
        simulation.forwardTime(10.seconds)

        println("Gathering results...")
        val results = simulation.gatherMessageResults()

        val msgDelayStats = StatsFactory.DEFAULT.createStats("msgDelay").also {
            it += results.entries.flatMap { e ->
                e.value.map { it.receivedTime - e.key.sentTime }
            }
        }
        val msgDeliveryStats = StatsFactory.DEFAULT.createStats("msgDelay").also {
            it += results.entries.map { it.value.size.toDouble() / (simConfig.totalPeers - 1) }
        }
        val gossipScoreStats = StatsFactory.DEFAULT.createStats("gossipScore").also {
            it += simNetwork.peers.values
                .map { it.router as GossipRouter }
                .flatMap { gossip ->
                    gossip.peers.map { gossip.score.score(it) }
                }
        }

        val connections = simNetwork.peers.values.flatMap { peer ->
            peer.getConnectedPeers().map { peer to it as GossipSimPeer }
        }
        val scoredConnections = connections.map { (from, to) ->
            val gossipFrom = from.router as GossipRouter
            val toPeerHandler = gossipFrom.peers.filter { it.peerId == to.peerId }.first()
            Triple(gossipFrom.score.score(toPeerHandler), from, to)
        }.sortedBy { it.first }

        println(msgDelayStats)
    }

    @Test
    fun a() {
        val param =
            iterateMeshMessageDeliveryWindow(listOf(20, 50, 100, 200, 500, 1000).map { it.millis })
        val map = param[0].propertiesAsMap()
        println(map)
    }
}
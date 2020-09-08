package io.libp2p.simulate

import java.util.Random

interface Topology {

    var random: Random
    fun connect(peers: List<SimPeer>): Network
}
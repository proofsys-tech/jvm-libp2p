package io.libp2p.simulate.stats

interface StatsFactory {

    fun createStats(name: String = ""): WritableStats

    companion object {
        val DUMMY = object : WritableStats {
            override fun addValue(value: Double) {}
            override fun reset() {}
            override fun plus(other: Stats) = TODO()
            override fun toString() = "<dummy>"
        }

        var DEFAULT: StatsFactory = object : StatsFactory {
            override fun createStats(name: String) = DescriptiveStatsImpl()
        }
    }
}
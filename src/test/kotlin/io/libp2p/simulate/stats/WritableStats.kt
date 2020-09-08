package io.libp2p.simulate.stats

interface WritableStats : Stats {

    fun addValue(value: Double)

    fun reset()

    fun addValue(value: Int) = addValue(value.toDouble())
    fun addValue(value: Long) = addValue(value.toDouble())
}
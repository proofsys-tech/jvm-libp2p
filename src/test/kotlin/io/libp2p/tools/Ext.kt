package io.libp2p.tools

import java.time.Duration
import kotlin.math.max
import kotlin.math.roundToLong

fun msec(info: String = "Starting...", f: () -> Unit) {
    val s = System.nanoTime()
    try {
        print("$info ")
        System.out.flush()
        f()
    } finally {
        val d = System.nanoTime() - s

        val durS = when (d) {
            in 0..10 * 1_000L -> "" + d + " ns"
            in 10 * 1000..10 * 1_000_000L -> "" + (d / 1_000) + " us"
            in 10 * 1_000_000L..1 * 1_000_000_000L -> "" + (d / 1_000_000) + " ms"
            else -> "%.3f".format((d / 1_000_000) / 1000.0) + " s"
        }
        println("done in $durS")
    }
}

operator fun <T> List<T>.get(subIndexes: IntRange) = subList(subIndexes.first, subIndexes.last + 1)

val Int.millis: Duration
    get() = Duration.ofMillis(this.toLong())
val Int.seconds: Duration
    get() = Duration.ofSeconds(this.toLong())
val Int.minutes: Duration
    get() = Duration.ofMinutes(this.toLong())

fun <K, V> Map<K, V>.setKeys(f: (K) -> K): Map<K, V> = asSequence().map { f(it.key) to it.value }.toMap()

operator fun <K, V> Map<K, V>.plus(other: Map<K, V>): Map<K, V> =
    (asSequence() + other.asSequence()).map { it.key to it.value }.toMap()

fun <K, V> List<Map<K, V>>.regroup(): Map<K, List<V>> = flatMap { it.asIterable() }.groupBy({ it.key }, { it.value })

fun Int.pow(n: Int): Long {
    var t = 1L
    for (i in 0 until n) t *= this
    return t
}

fun Double.smartRound(meaningCount: Int = 3): Double {
    var cnt = 0
    var n = this
    val t = 10.pow(meaningCount)

    if (n < t) {
        while (n < t) {
            n *= 10
            cnt++
        }
        return n.roundToLong().toDouble() / 10.pow(cnt)
    } else {
        while (n > t * 10) {
            n /= 10
            cnt++
        }
        return n.roundToLong().toDouble() * 10.pow(cnt)
    }
}

fun String.align(width: Int, alignLeft: Boolean = true, fillChar: Char = ' '): String {
    val n = max(1, width - length)
    return if (alignLeft) this + fillChar.toString().repeat(n) else fillChar.toString().repeat(n) + this
}

fun String.formatTable(firstLineHeaders: Boolean = true, separator: String = "\t", alignLeft: Boolean = true): String {
    val list = this.split("\n").map { it.split(separator) }
    require(list.map { it.size }.min() == list.map { it.size }.max()) { "Different number of columns" }
    val colSizes = list[0].indices.map { col -> list.map { it[col].length + 1 }.max() }
    val strings = list.map { raw ->
        raw.indices.map { raw[it].align(colSizes[it]!!, alignLeft) }
            .joinToString("")
    }.toMutableList()

    if (firstLineHeaders) {
        strings.add(1, colSizes.map { "-".repeat(it!! - 1) + " " }.joinToString(""))
    }
    return strings.joinToString("\n")
}
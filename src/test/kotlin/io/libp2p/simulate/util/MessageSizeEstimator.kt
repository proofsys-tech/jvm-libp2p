package io.libp2p.simulate.util

import io.netty.buffer.ByteBuf

typealias MsgSizeEstimator = (Any) -> Int

val GeneralSizeEstimator: MsgSizeEstimator = { msg ->
    when (msg) {
        is ByteBuf -> msg.readableBytes()
        else -> 0
    }
}
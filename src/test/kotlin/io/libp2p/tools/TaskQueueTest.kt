package io.libp2p.tools

import io.libp2p.tools.schedulers.TaskQueue
import io.libp2p.tools.schedulers.TimeController
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

class TaskQueueTest {

    val pool = Executors.newFixedThreadPool(256)
    val taskQueue = TaskQueue()
    val id = AtomicInteger()
    val cnt = AtomicInteger()

    inner class MyTask : TimeController.Task {
        val num: Int = id.getAndIncrement()

        override fun getTime() = 0L
        override fun execute(): CompletableFuture<Void> {
            val runnable = Runnable {
                Thread.sleep(1)
                if (num < 10000) {
                    for (i in 0..3) {
                        val myTask = MyTask()
                        taskQueue.add(myTask)
                        Thread.sleep(1)
                    }
                }
                cnt.incrementAndGet()
            }
            return CompletableFuture.runAsync(runnable, pool)
        }
    }

    @Test
    fun test1() {

        taskQueue.add(MyTask())
        taskQueue.add(MyTask())
        taskQueue.executeEarliest()
        Assertions.assertEquals(id.get(), cnt.get())
    }
}
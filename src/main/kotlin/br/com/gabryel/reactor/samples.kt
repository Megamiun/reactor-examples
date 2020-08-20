package br.com.gabryel.reactor

import org.reactivestreams.Publisher
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.ParallelFlux
import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers
import java.time.Duration
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit.*
import java.time.temporal.TemporalUnit
import java.util.function.BiFunction
import kotlin.random.Random.Default.nextLong
import kotlin.system.measureTimeMillis

private val start = LocalDateTime.now()

private val elasticScheduler = Schedulers.elastic()

private val singleScheduler = Schedulers.single()

fun main() {
    "Single Call" { call("Single") }

    "Mix Two Calls - Sequential" {
        val a = call("A", 500, MILLIS)
        val b = call("B", 1, SECONDS)

        a.zipWith(b)
    }

    "Mix Two Calls - Elastic" {
        val a = call("A", 500, scheduler = elasticScheduler)
        val b = call("B", 200, scheduler = elasticScheduler)

        a.zipWith(b)
    }

    "Join Two Calls" {
        val a = call("A", 500)
        val b = call("B", 200)

        a.zipWith(b) { first, second -> "$first + $second" }
    }

    "Compose Calls" {
        call("Generic Movie", 100, MILLIS)
            .flatMap { previousResult -> call("$previousResult 2.0 - The Vengeance", 200, MILLIS) }
    }

    "Timeout" {
        call("A", 1, DAYS).timeout(Duration.ofMillis(500))
    }

    "Logging Error Without Stopping Them" {
        call("A", 1, DAYS).timeout(Duration.ofMillis(500))
            .doOnError { err -> printTimed("Error: ${err.javaClass.simpleName}") }
    }

    "Recovering from Error" {
        call("A", 1, DAYS).timeout(Duration.ofMillis(500))
            .onErrorResume { err -> Mono.just("I like ${err.javaClass.simpleName}s!!!") }
    }

    "Calling Multiple Times And Concatenating to List" {
        listOf("A", "B", "C", "D").map { call(it, 200, MILLIS) }
            .fold(Mono.just(emptyList<String>())) { aMono, bMono -> aMono.flatMap { a -> bMono.map { b -> a + b } } }
    }

    "Calling Multiple Times And Concatenating to List - Elastic" {
        Flux.fromIterable(listOf("A", "B", "C", "D"))
            .flatMap { call(it, 200, MILLIS, elasticScheduler) }
    }

    "Calling 5 Items with Random Time - Sequential" {
        Flux.fromIterable((1..5).asIterable())
            .flatMap { call(it, nextLong(20, 60), MILLIS) }
    }

    "Calling 5 Items with Random Time - Elastic" {
        Flux.fromIterable((1..5).asIterable())
            .parallel().flatMap { call(it, nextLong(20, 60), MILLIS, elasticScheduler) }
    }

    "Calling 5 Items with Random Time - Parallel/Elastic" {
        Flux.fromIterable((1..5).asIterable())
            .parallel().flatMap { call(it, nextLong(20, 60), MILLIS, elasticScheduler) }
    }

    "Calling 30 Items with Random Time - Sequential"(showArrivals = false) {
        Flux.fromIterable((1..30).asIterable())
            .flatMap { call(it, nextLong(20, 60), MILLIS, verbose = false) }
    }

    "Calling 30 Items with Random Time - Elastic"(showArrivals = false) {
        Flux.fromIterable((1..30).asIterable())
            .parallel().flatMap { call(it, nextLong(20, 60), MILLIS, elasticScheduler, verbose = false) }
    }

    "Calling 30 Items with Random Time - Parallel/Elastic"(showArrivals = false) {
        Flux.fromIterable((1..30).asIterable())
            .parallel().flatMap { call(it, nextLong(20, 60), MILLIS, elasticScheduler, verbose = false) }
    }

    "Calling 1000 Items - Parallel/Elastic"(showResults = false, showArrivals = false) {
        Flux.fromIterable((1..1000).asIterable())
            .parallel().flatMap { call(it, 40, MILLIS, elasticScheduler, verbose = false) }
    }

    "Calling 5000 Items - Elastic"(showResults = false, showArrivals = false) {
        Flux.fromIterable((1..5000).asIterable())
            .flatMap { call(it, 40, MILLIS, elasticScheduler, verbose = false) }
    }

    "Calling 5000 Items - Parallel/Elastic"(showResults = false, showArrivals = false) {
        Flux.fromIterable((1..5000).asIterable())
            .parallel().flatMap { call(it, 40, MILLIS, elasticScheduler, verbose = false) }
    }

    "Calling With Multiple Results" {
        callFlux(listOf("A", "B", "C", "D"), 200, MILLIS)
    }

    "Mixing Two Set of Calls With Multiple Results" {
        callFlux(listOf("A", "B", "C", "D"), 200, MILLIS)
            .zipWith(callFlux(listOf("1", "2", "3", "4"), 500, MILLIS), BiFunction(String::plus))
    }

    "Mixing Two Set of Calls With Different Sizes" {
        callFlux(listOf("A", "B"), 200, MILLIS)
            .zipWith(callFlux(listOf("1"), 500, MILLIS), BiFunction(String::plus))
    }
}

private fun <T> call(
    obj: T,
    delay: Long = 0,
    timeUnit: TemporalUnit = MILLIS,
    scheduler: Scheduler = singleScheduler,
    verbose: Boolean = true
): Mono<T> {
    return Mono.just(obj)
        .publishOn(scheduler)
        .doOnSubscribe { if (verbose) printTimed("Call started: '$obj'") }
        .doOnNext { Thread.sleep(Duration.of(delay, timeUnit).toMillis()) }
        .doOnSuccess { if (verbose) printTimed("Call ended: '$obj'") }
}

private fun <T> callFlux(
    items: List<T>,
    delay: Long = 0,
    timeUnit: TemporalUnit = MILLIS,
    scheduler: Scheduler = singleScheduler
): Flux<T> {
    return Flux.fromIterable(items)
        .publishOn(scheduler)
        .doOnSubscribe { printTimed("Call started: $items") }
        .doOnSubscribe { Thread.sleep(Duration.of(delay, timeUnit).toMillis()) }
        .doOnNext { printTimed("Current Element: '$it'") }
        .doOnComplete { printTimed("Call ended: $items") }
}

private operator fun String.invoke(
    showArrivals: Boolean = true,
    showResults: Boolean = true,
    invoke: () -> Publisher<*>
) {
    println("---------------$this---------------")

    var result: Any? = null
    val delta = measureTimeMillis {
        result = try {
            when (val pub = invoke()) {
                is Mono -> pub
                is Flux -> {
                    pub.doOnNext {
                        if (showArrivals) printTimed("Arrived: '$it'")
                    }.collectList()
                }
                is ParallelFlux ->  {
                    pub.doOnNext {
                        if (showArrivals) printTimed("Arrived: '$it'")
                    }.sequential().collectList()
                }
                else -> throw IllegalStateException("Not expected type: ${pub.javaClass.simpleName}")
            }.block()
        } catch (err: Throwable) {
            err
        }
    }

    if (showResults) {
        printTimed(
            """
                
                Duration: ${delta}ms
                Result: '$result'
                
            """.trimIndent()
        )
    } else {
        printTimed(
            """
                
                Duration: ${delta}ms
                
            """.trimIndent()
        )
    }
}

private fun printTimed(message: String) {
    val time = start.until(LocalDateTime.now(), MILLIS).toString().padStart(6, '-')
    val lines = message.lines().map {
        if (it.isBlank()) it
        else "[$time] $it"
    }

    println(lines.joinToString("\n"))
}
package com.yolt.accountsandtransactions;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class MonoTest {

    public static void main(String[] args) {

        var a = Schedulers.newSingle("a");
        var b = Schedulers.newSingle("b");
        var c = Schedulers.newSingle("c");

        Mono.fromCallable(() -> "Root callable " + Thread.currentThread().getName())
                .log()
                .doOnNext(System.out::println)
                .flatMap(s -> Mono.fromCallable(() -> "Child 1 " + Thread.currentThread().getName())
                        .log()
                        .doOnNext(ignored -> System.out.println(ignored + " -> " + Thread.currentThread().getName()))
                        .subscribeOn(b)
                        .publishOn(Schedulers.parallel())
                        .doOnNext(ignored -> System.out.println("child publish " + Thread.currentThread().getName())))
                .doOnNext(ignored -> System.out.println("root publish " + Thread.currentThread().getName()))
                .flatMap(s -> Mono.fromCallable(() -> "Child 2 " + Thread.currentThread().getName())
                        .log()
                        .subscribeOn(c)
                )
                .doOnNext(System.out::println)
                .subscribeOn(a)
                .block();

        a.dispose();
        b.dispose();
        c.dispose();
    }
}

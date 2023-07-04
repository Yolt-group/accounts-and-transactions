package com.yolt.accountsandtransactions.concurrency;

import com.google.common.util.concurrent.FutureCallback;
import lombok.NonNull;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.concurrent.CompletableFuture;

import static com.google.common.util.concurrent.Futures.addCallback;

public class Futures {

    public static <T> CompletableFuture<T> from(final ListenableFuture<T> listenableFuture) {
        CompletableFuture<T> completable = new CompletableFuture<>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                boolean result = listenableFuture.cancel(mayInterruptIfRunning);
                super.cancel(mayInterruptIfRunning);
                return result;
            }
        };

        listenableFuture.addCallback(new ListenableFutureCallback<T>() {
            @Override
            public void onSuccess(T result) {
                completable.complete(result);
            }

            @Override
            public void onFailure(@NonNull Throwable t) {
                completable.completeExceptionally(t);
            }
        });
        return completable;
    }

    public static <T> CompletableFuture<T> from(com.google.common.util.concurrent.ListenableFuture<T> listenableFuture) {

        final CompletableFuture<T> completableFuture = new CompletableFuture<>();

        //noinspection UnstableApiUsage
        addCallback(listenableFuture, new FutureCallback<T>() {
            @Override
            public void onSuccess(T result) {
                completableFuture.complete(result);
            }

            @Override
            public void onFailure(Throwable t) {
                completableFuture.completeExceptionally(t);
            }
        });

        return completableFuture;
    }
}

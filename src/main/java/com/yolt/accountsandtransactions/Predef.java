package com.yolt.accountsandtransactions;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.lang.Nullable;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Collections.emptyList;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.Collectors.toList;

/**
 * Convenience methods
 */
public class Predef {

    public static <T> Optional<T> maybe(@Nullable T val) {
        return Optional.ofNullable(val);
    }

    public static <T> Optional<T> some(@NonNull T val) {
        return Optional.of(val);
    }

    public static <T> Optional<T> none() {
        return Optional.empty();
    }

    public static <T> List<T> append(List<? extends T> list, T element) {
        return Stream.concat(list.stream(), Stream.of(element)).collect(toList());
    }

    /**
     * @return the head of the list if any
     */
    public static <T> Optional<T> head(List<? extends T> list) {
        return list.size() == 0
                ? Optional.empty()
                : Optional.of(list.get(0));
    }

    /**
     * @return the last element of the list if any
     */
    public static <T> Optional<T> last(List<? extends T> list) {
        return list.size() == 0
                ? Optional.empty()
                : Optional.of(list.get(list.size() - 1));
    }

    public static <T> List<T> concat(List<? extends T> l1, List<? extends T> l2) {
        return Stream.concat(l1.stream(), l2.stream()).collect(toList());
    }

    @SafeVarargs
    public static <T> List<T> concatAll(List<T>... lists) {
        return Arrays.stream(lists).reduce(emptyList(), Predef::concat);
    }

    public static <T, A> Map<A, T> mapValuesAsHeadOfList(final Map<A, List<T>> mapWithNonEmptyList) {
        return mapWithNonEmptyList.entrySet().stream()
                .map(e -> {
                    if (e.getValue().size() != 1) {
                        throw new IllegalStateException("mapWithNonEmptyList should contain exactly 1 element.");
                    }
                    return Pair.of(e.getKey(), e.getValue().get(0));
                }).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    }

    public static class Streams {

        /**
         * PartitionBy collector which wraps the result Map into a {@link Partitioned} struct.
         */
        public static <T> Collector<T, ?, Partitioned<List<T>>> partitioned(final Predicate<? super T> predicate) {
            return partitioned(predicate, identity());
        }

        /**
         * PartitionBy collector which wraps the result Map into a {@link Partitioned} struct.
         */
        public static <T, O> Collector<T, ?, Partitioned<List<O>>> partitioned(final Predicate<? super T> predicate, Function<T, O> mapping) {
            return Collectors.collectingAndThen(partitioningBy(predicate, toList()),
                    result -> Partitioned.<List<O>>builder()
                            .excluded(result.getOrDefault(false, Collections.emptyList()).stream().map(mapping).collect(toList()))
                            .included(result.getOrDefault(true, Collections.emptyList()).stream().map(mapping).collect(toList()))
                            .build());
        }

        public static <A, B> Collector<A, ?, B> foldLeft(final B init, final BiFunction<? super B, ? super A, ? extends B> f) {
            return Collectors.collectingAndThen(
                    Collectors.reducing(Function.<B>identity(), a -> b -> f.apply(b, a), Function::andThen),
                    end -> end.apply(init)
            );
        }

        public static <T, V> Stream<Pair<T, V>> zip(Stream<T> first, Stream<V> second) {
            Iterable<Pair<T, V>> iterable = () -> new ZippedIterator<>(first.iterator(), second.iterator());
            return StreamSupport.stream(iterable.spliterator(), false);
        }

        private static class ZippedIterator<K, V> implements Iterator<Pair<K, V>> {
            public final Iterator<K> first;
            public final Iterator<V> second;

            public ZippedIterator(Iterator<K> first, Iterator<V> second) {
                this.first = first;
                this.second = second;
            }

            @Override
            public Pair<K, V> next() {
                return Pair.of(first.next(), second.next());
            }

            @Override
            public boolean hasNext() {
                return first.hasNext() && second.hasNext();
            }

        }
    }

    @Builder
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Partitioned<T> {
        @NonNull
        public final T excluded;
        @NonNull
        public final T included;
    }
}

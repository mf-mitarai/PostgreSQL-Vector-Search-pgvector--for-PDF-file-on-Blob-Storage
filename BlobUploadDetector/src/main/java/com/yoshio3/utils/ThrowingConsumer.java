package com.yoshio3.utils;

import java.util.function.Consumer;

@FunctionalInterface
public interface ThrowingConsumer<T> extends Consumer<T> {

    @Override
    default void accept(final T e) {
        try {
            accept0(e);
        } catch (Throwable ex) {
            Throwing.sneakyThrow(ex);
        }
    }

    void accept0(T e) throws Throwable;
}

package com.yoshio3.utils;

import java.util.function.Consumer;

public final class Throwing {

    private Throwing() {}

    public static <T> Consumer<T> rethrow(final ThrowingConsumer<T> consumer) {
        return consumer;
    }

    @SuppressWarnings("unchecked")
    public static <E extends Throwable> void sneakyThrow(Throwable ex) throws E {
        throw (E) ex;
    }
}

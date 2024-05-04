package com.alibaba.ttl;

import com.alibaba.ttl.spi.TtlAttachments;
import com.alibaba.ttl.spi.TtlAttachmentsDelegate;
import com.alibaba.ttl.spi.TtlEnhanced;
import com.alibaba.ttl.spi.TtlWrapper;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jetbrains.annotations.Contract;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

import static com.alibaba.ttl.TransmittableThreadLocal.Transmitter.*;

/**
 * Callable包装类
 *
 * @author Jerry Lee (oldratlee at gmail dot com)
 */
public final class TtlCallable<V> implements Callable<V>, TtlWrapper<Callable<V>>, TtlEnhanced, TtlAttachments {

    private final AtomicReference<Object> capturedRef;
    private final Callable<V> callable;
    private final boolean releaseTtlValueReferenceAfterCall;

    @Override
    @SuppressFBWarnings("THROWS_METHOD_THROWS_CLAUSE_BASIC_EXCEPTION")
    public V call() throws Exception {
        final Object captured = capturedRef.get();
        if (captured == null || releaseTtlValueReferenceAfterCall && !capturedRef.compareAndSet(captured, null)) {
            throw new IllegalStateException("TTL value reference is released after call!");
        }
        final Object backup = replay(captured);
        try {
            return callable.call();
        } finally {
            restore(backup);
        }
    }


    @Nullable
    @Contract(value = "null -> null; !null -> !null", pure = true)
    public static <T> TtlCallable<T> get(@Nullable Callable<T> callable) {
        return get(callable, false, false);
    }
    @Nullable
    @Contract(value = "null, _ -> null; !null, _ -> !null", pure = true)
    public static <T> TtlCallable<T> get(@Nullable Callable<T> callable, boolean releaseTtlValueReferenceAfterCall) {
        return get(callable, releaseTtlValueReferenceAfterCall, false);
    }
    @Nullable
    @Contract(value = "null, _, _ -> null; !null, _, _ -> !null", pure = true)
    public static <T> TtlCallable<T> get(@Nullable Callable<T> callable, boolean releaseTtlValueReferenceAfterCall, boolean idempotent) {
        if (callable == null) return null;
        if (callable instanceof TtlEnhanced) {
            // avoid redundant decoration, and ensure idempotency
            if (idempotent) return (TtlCallable<T>) callable;
            else throw new IllegalStateException("Already TtlCallable!");
        }
        return new TtlCallable<>(callable, releaseTtlValueReferenceAfterCall);
    }
    private TtlCallable(@NonNull Callable<V> callable, boolean releaseTtlValueReferenceAfterCall) {
        this.capturedRef = new AtomicReference<>(capture());
        this.callable = callable;
        this.releaseTtlValueReferenceAfterCall = releaseTtlValueReferenceAfterCall;
    }

    @NonNull
    public static <T> List<TtlCallable<T>> gets(@Nullable Collection<? extends Callable<T>> tasks) {
        return gets(tasks, false, false);
    }
    @NonNull
    public static <T> List<TtlCallable<T>> gets(@Nullable Collection<? extends Callable<T>> tasks, boolean releaseTtlValueReferenceAfterCall) {
        return gets(tasks, releaseTtlValueReferenceAfterCall, false);
    }
    @NonNull
    public static <T> List<TtlCallable<T>> gets(@Nullable Collection<? extends Callable<T>> tasks, boolean releaseTtlValueReferenceAfterCall, boolean idempotent) {
        if (tasks == null) return Collections.emptyList();

        List<TtlCallable<T>> copy = new ArrayList<>();
        for (Callable<T> task : tasks) {
            copy.add(TtlCallable.get(task, releaseTtlValueReferenceAfterCall, idempotent));
        }
        return copy;
    }

    @Nullable
    @Contract(value = "null -> null; !null -> !null", pure = true)
    public static <T> Callable<T> unwrap(@Nullable Callable<T> callable) {
        if (!(callable instanceof TtlCallable)) return callable;
        else return ((TtlCallable<T>) callable).getCallable();
    }

    @NonNull
    public static <T> List<Callable<T>> unwraps(@Nullable Collection<? extends Callable<T>> tasks) {
        if (tasks == null) return Collections.emptyList();
        List<Callable<T>> copy = new ArrayList<>();
        for (Callable<T> task : tasks) {
            if (!(task instanceof TtlCallable)) copy.add(task);
            else copy.add(((TtlCallable<T>) task).getCallable());
        }
        return copy;
    }

    private final TtlAttachmentsDelegate ttlAttachment = new TtlAttachmentsDelegate();
    @Override
    public void setTtlAttachment(@NonNull String key, Object value) {
        ttlAttachment.setTtlAttachment(key, value);
    }
    @Override
    public <T> T getTtlAttachment(@NonNull String key) {
        return ttlAttachment.getTtlAttachment(key);
    }

    @NonNull
    public Callable<V> getCallable() {
        return unwrap();
    }

    @NonNull
    @Override
    public Callable<V> unwrap() {
        return callable;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TtlCallable<?> that = (TtlCallable<?>) o;
        return callable.equals(that.callable);
    }

    @Override
    public int hashCode() {
        return callable.hashCode();
    }

    @Override
    public String toString() {
        return this.getClass().getName() + " - " + callable.toString();
    }

}

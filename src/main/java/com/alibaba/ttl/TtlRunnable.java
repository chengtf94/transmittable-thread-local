package com.alibaba.ttl;

import com.alibaba.ttl.spi.TtlAttachments;
import com.alibaba.ttl.spi.TtlAttachmentsDelegate;
import com.alibaba.ttl.spi.TtlEnhanced;
import com.alibaba.ttl.spi.TtlWrapper;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import org.jetbrains.annotations.Contract;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static com.alibaba.ttl.TransmittableThreadLocal.Transmitter.*;

/**
 * Runnable包装类
 *
 * @author Jerry Lee (oldratlee at gmail dot com)
 */
public final class TtlRunnable implements Runnable, TtlWrapper<Runnable>, TtlEnhanced, TtlAttachments {

    private final AtomicReference<Object> capturedRef;
    private final Runnable runnable;
    private final boolean releaseTtlValueReferenceAfterRun;

    @Override
    public void run() {
        final Object captured = capturedRef.get();
        if (captured == null || releaseTtlValueReferenceAfterRun && !capturedRef.compareAndSet(captured, null)) {
            throw new IllegalStateException("TTL value reference is released after run!");
        }
        final Object backup = replay(captured);
        try {
            runnable.run();
        } finally {
            restore(backup);
        }
    }

    @Nullable
    @Contract(value = "null -> null; !null -> !null", pure = true)
    public static TtlRunnable get(@Nullable Runnable runnable) {
        return get(runnable, false, false);
    }
    @Nullable
    @Contract(value = "null, _ -> null; !null, _ -> !null", pure = true)
    public static TtlRunnable get(@Nullable Runnable runnable, boolean releaseTtlValueReferenceAfterRun) {
        return get(runnable, releaseTtlValueReferenceAfterRun, false);
    }
    @Nullable
    @Contract(value = "null, _, _ -> null; !null, _, _ -> !null", pure = true)
    public static TtlRunnable get(@Nullable Runnable runnable, boolean releaseTtlValueReferenceAfterRun, boolean idempotent) {
        if (runnable == null) return null;
        if (runnable instanceof TtlEnhanced) {
            if (idempotent) return (TtlRunnable) runnable;
            else throw new IllegalStateException("Already TtlRunnable!");
        }
        return new TtlRunnable(runnable, releaseTtlValueReferenceAfterRun);
    }
    private TtlRunnable(@NonNull Runnable runnable, boolean releaseTtlValueReferenceAfterRun) {
        this.capturedRef = new AtomicReference<>(capture());
        this.runnable = runnable;
        this.releaseTtlValueReferenceAfterRun = releaseTtlValueReferenceAfterRun;
    }

    @NonNull
    public static List<TtlRunnable> gets(@Nullable Collection<? extends Runnable> tasks) {
        return gets(tasks, false, false);
    }
    @NonNull
    public static List<TtlRunnable> gets(@Nullable Collection<? extends Runnable> tasks, boolean releaseTtlValueReferenceAfterRun) {
        return gets(tasks, releaseTtlValueReferenceAfterRun, false);
    }
    @NonNull
    public static List<TtlRunnable> gets(@Nullable Collection<? extends Runnable> tasks, boolean releaseTtlValueReferenceAfterRun, boolean idempotent) {
        if (tasks == null) return Collections.emptyList();
        List<TtlRunnable> copy = new ArrayList<>();
        for (Runnable task : tasks) {
            copy.add(TtlRunnable.get(task, releaseTtlValueReferenceAfterRun, idempotent));
        }
        return copy;
    }

    @Nullable
    @Contract(value = "null -> null; !null -> !null", pure = true)
    public static Runnable unwrap(@Nullable Runnable runnable) {
        if (!(runnable instanceof TtlRunnable)) return runnable;
        else return ((TtlRunnable) runnable).getRunnable();
    }

    @NonNull
    public static List<Runnable> unwraps(@Nullable Collection<? extends Runnable> tasks) {
        if (tasks == null) return Collections.emptyList();
        List<Runnable> copy = new ArrayList<>();
        for (Runnable task : tasks) {
            if (!(task instanceof TtlRunnable)) copy.add(task);
            else copy.add(((TtlRunnable) task).getRunnable());
        }
        return copy;
    }

    @NonNull
    public Runnable getRunnable() {
        return unwrap();
    }

    @NonNull
    @Override
    public Runnable unwrap() {
        return runnable;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TtlRunnable that = (TtlRunnable) o;
        return runnable.equals(that.runnable);
    }

    @Override
    public int hashCode() {
        return runnable.hashCode();
    }

    @Override
    public String toString() {
        return this.getClass().getName() + " - " + runnable.toString();
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

}

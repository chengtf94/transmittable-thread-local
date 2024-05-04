package com.alibaba.ttl.threadpool;

import com.alibaba.ttl.TtlRunnable;
import com.alibaba.ttl.spi.TtlEnhanced;
import com.alibaba.ttl.spi.TtlWrapper;
import edu.umd.cs.findbugs.annotations.NonNull;

import java.util.concurrent.Executor;

/**
 * Executor包装类
 *
 * @author Jerry Lee (oldratlee at gmail dot com)
 */
class ExecutorTtlWrapper implements Executor, TtlWrapper<Executor>, TtlEnhanced {

    private final Executor executor;
    protected final boolean idempotent;

    ExecutorTtlWrapper(@NonNull Executor executor, boolean idempotent) {
        this.executor = executor;
        this.idempotent = idempotent;
    }

    @Override
    public void execute(@NonNull Runnable command) {
        executor.execute(TtlRunnable.get(command, false, idempotent));
    }

    @NonNull
    @Override
    public Executor unwrap() {
        return executor;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExecutorTtlWrapper that = (ExecutorTtlWrapper) o;
        return executor.equals(that.executor);
    }

    @Override
    public int hashCode() {
        return executor.hashCode();
    }

    @Override
    public String toString() {
        return this.getClass().getName() + " - " + executor;
    }

}

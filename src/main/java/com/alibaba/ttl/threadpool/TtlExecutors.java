package com.alibaba.ttl.threadpool;

import com.alibaba.ttl.TransmittableThreadLocal;
import com.alibaba.ttl.spi.TtlEnhanced;
import com.alibaba.ttl.spi.TtlWrapper;
import com.alibaba.ttl.threadpool.agent.TtlAgent;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import org.jetbrains.annotations.Contract;

import java.util.Comparator;
import java.util.concurrent.*;

/**
 * Util methods for TTL wrapper of jdk executors.
 *
 * @author Jerry Lee (oldratlee at gmail dot com)
 */
public final class TtlExecutors {

    @Nullable
    @Contract(value = "null -> null; !null -> !null", pure = true)
    public static Executor getTtlExecutor(@Nullable Executor executor) {
        if (TtlAgent.isTtlAgentLoaded() || executor == null || executor instanceof TtlEnhanced) {
            return executor;
        }
        return new ExecutorTtlWrapper(executor, true);
    }

    @Nullable
    @Contract(value = "null -> null; !null -> !null", pure = true)
    public static ExecutorService getTtlExecutorService(@Nullable ExecutorService executorService) {
        if (TtlAgent.isTtlAgentLoaded() || executorService == null || executorService instanceof TtlEnhanced) {
            return executorService;
        }
        return new ExecutorServiceTtlWrapper(executorService, true);
    }

    @Nullable
    @Contract(value = "null -> null; !null -> !null", pure = true)
    public static ScheduledExecutorService getTtlScheduledExecutorService(@Nullable ScheduledExecutorService scheduledExecutorService) {
        if (TtlAgent.isTtlAgentLoaded() || scheduledExecutorService == null || scheduledExecutorService instanceof TtlEnhanced) {
            return scheduledExecutorService;
        }
        return new ScheduledExecutorServiceTtlWrapper(scheduledExecutorService, true);
    }

    public static <T extends Executor> boolean isTtlWrapper(@Nullable T executor) {
        return executor instanceof TtlWrapper;
    }

    @Nullable
    @Contract(value = "null -> null; !null -> !null", pure = true)
    @SuppressWarnings("unchecked")
    public static <T extends Executor> T unwrap(@Nullable T executor) {
        if (!isTtlWrapper(executor)) return executor;
        return (T) ((ExecutorTtlWrapper) executor).unwrap();
    }

    @Nullable
    @Contract(value = "null -> null; !null -> !null", pure = true)
    public static ThreadFactory getDisableInheritableThreadFactory(@Nullable ThreadFactory threadFactory) {
        if (threadFactory == null || isDisableInheritableThreadFactory(threadFactory)) return threadFactory;
        return new DisableInheritableThreadFactoryWrapper(threadFactory);
    }

    @NonNull
    public static ThreadFactory getDefaultDisableInheritableThreadFactory() {
        return getDisableInheritableThreadFactory(Executors.defaultThreadFactory());
    }

    public static boolean isDisableInheritableThreadFactory(@Nullable ThreadFactory threadFactory) {
        return threadFactory instanceof DisableInheritableThreadFactory;
    }

    @Nullable
    @Contract(value = "null -> null; !null -> !null", pure = true)
    public static ThreadFactory unwrap(@Nullable ThreadFactory threadFactory) {
        if (!isDisableInheritableThreadFactory(threadFactory)) return threadFactory;
        return ((DisableInheritableThreadFactory) threadFactory).unwrap();
    }

    @Nullable
    @Contract(value = "null -> null; !null -> !null", pure = true)
    public static Comparator<Runnable> getTtlRunnableUnwrapComparator(@Nullable Comparator<Runnable> comparator) {
        if (comparator == null || isTtlRunnableUnwrapComparator(comparator)) return comparator;
        return new TtlUnwrapComparator<>(comparator);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static final Comparator INSTANCE = new TtlUnwrapComparator(ComparableComparator.INSTANCE);


    @NonNull
    @SuppressWarnings("unchecked")
    public static Comparator<Runnable> getTtlRunnableUnwrapComparatorForComparableRunnable() {
        return (Comparator<Runnable>) INSTANCE;
    }

    public static boolean isTtlRunnableUnwrapComparator(@Nullable Comparator<Runnable> comparator) {
        return comparator instanceof TtlUnwrapComparator;
    }

    @Nullable
    @Contract(value = "null -> null; !null -> !null", pure = true)
    public static Comparator<Runnable> unwrap(@Nullable Comparator<Runnable> comparator) {
        if (!isTtlRunnableUnwrapComparator(comparator)) return comparator;
        return ((TtlUnwrapComparator<Runnable>) comparator).unwrap();
    }

    private TtlExecutors() {
        throw new InstantiationError("Must not instantiate this class");
    }

}

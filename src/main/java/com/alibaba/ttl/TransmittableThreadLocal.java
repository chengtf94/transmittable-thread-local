package com.alibaba.ttl;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jetbrains.annotations.TestOnly;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * TransmittableThreadLocal
 *
 * @author Jerry Lee (oldratlee at gmail dot com)
 * @author Yang Fang (snoop dot fy at gmail dot com)
 */
public class TransmittableThreadLocal<T> extends InheritableThreadLocal<T> implements TtlCopier<T> {
    private static final Logger logger = Logger.getLogger(TransmittableThreadLocal.class.getName());

    /**
     * 构造方法
     */
    private final boolean disableIgnoreNullValueSemantics;
    public TransmittableThreadLocal() {
        this(false);
    }
    public TransmittableThreadLocal(boolean disableIgnoreNullValueSemantics) {
        this.disableIgnoreNullValueSemantics = disableIgnoreNullValueSemantics;
    }
    @NonNull
    @SuppressWarnings("ConstantConditions")
    public static <S> TransmittableThreadLocal<S> withInitial(@NonNull Supplier<? extends S> supplier) {
        if (supplier == null) throw new NullPointerException("supplier is null");
        return new SuppliedTransmittableThreadLocal<>(supplier, null, null);
    }
    @NonNull
    @ParametersAreNonnullByDefault
    @SuppressWarnings("ConstantConditions")
    public static <S> TransmittableThreadLocal<S> withInitialAndCopier(Supplier<? extends S> supplier,
                                                                       TtlCopier<S> copierForChildValueAndCopy) {
        if (supplier == null) throw new NullPointerException("supplier is null");
        if (copierForChildValueAndCopy == null) throw new NullPointerException("ttl copier is null");

        return new SuppliedTransmittableThreadLocal<>(supplier, copierForChildValueAndCopy, copierForChildValueAndCopy);
    }
    @NonNull
    @ParametersAreNonnullByDefault
    @SuppressWarnings("ConstantConditions")
    public static <S> TransmittableThreadLocal<S> withInitialAndCopier(Supplier<? extends S> supplier,
                                                                       TtlCopier<S> copierForChildValue,
                                                                       TtlCopier<S> copierForCopy) {
        if (supplier == null) throw new NullPointerException("supplier is null");
        if (copierForChildValue == null) throw new NullPointerException("ttl copier for child value is null");
        if (copierForCopy == null) throw new NullPointerException("ttl copier for copy value is null");
        return new SuppliedTransmittableThreadLocal<>(supplier, copierForChildValue, copierForCopy);
    }
    private static final class SuppliedTransmittableThreadLocal<T> extends TransmittableThreadLocal<T> {
        private final Supplier<? extends T> supplier;
        private final TtlCopier<T> copierForChildValue;
        private final TtlCopier<T> copierForCopy;
        SuppliedTransmittableThreadLocal(Supplier<? extends T> supplier,
                                         TtlCopier<T> copierForChildValue,
                                         TtlCopier<T> copierForCopy) {
            if (supplier == null) throw new NullPointerException("supplier is null");
            this.supplier = supplier;
            this.copierForChildValue = copierForChildValue;
            this.copierForCopy = copierForCopy;
        }
        @Override
        protected T initialValue() {
            return supplier.get();
        }
        @Override
        protected T childValue(T parentValue) {
            if (copierForChildValue != null) return copierForChildValue.copy(parentValue);
            else return super.childValue(parentValue);
        }
        @Override
        public T copy(T parentValue) {
            if (copierForCopy != null) return copierForCopy.copy(parentValue);
            else return super.copy(parentValue);
        }
    }

    public T copy(T parentValue) {
        return parentValue;
    }

    protected void beforeExecute() {
    }

    protected void afterExecute() {
    }

    private static final InheritableThreadLocal<WeakHashMap<TransmittableThreadLocal<Object>, ?>> holder =
            new InheritableThreadLocal<WeakHashMap<TransmittableThreadLocal<Object>, ?>>() {
                @Override
                protected WeakHashMap<TransmittableThreadLocal<Object>, ?> initialValue() {
                    return new WeakHashMap<>();
                }
                @Override
                protected WeakHashMap<TransmittableThreadLocal<Object>, ?> childValue(WeakHashMap<TransmittableThreadLocal<Object>, ?> parentValue) {
                    return new WeakHashMap<>(parentValue);
                }
            };

    @Override
    public final void set(T value) {
        if (!disableIgnoreNullValueSemantics && value == null) {
            // may set null to remove value
            remove();
        } else {
            super.set(value);
            addThisToHolder();
        }
    }
    @SuppressWarnings("unchecked")
    private void addThisToHolder() {
        if (!holder.get().containsKey(this)) {
            holder.get().put((TransmittableThreadLocal<Object>) this, null); // WeakHashMap supports null value.
        }
    }

    @Override
    public final T get() {
        T value = super.get();
        if (disableIgnoreNullValueSemantics || value != null) addThisToHolder();
        return value;
    }

    @Override
    public final void remove() {
        removeThisFromHolder();
        super.remove();
    }
    private void removeThisFromHolder() {
        holder.get().remove(this);
    }

    private void superRemove() {
        super.remove();
    }

    private T copyValue() {
        return copy(get());
    }

    private static void doExecuteCallback(boolean isBefore) {
        WeakHashMap<TransmittableThreadLocal<Object>, ?> ttlInstances = new WeakHashMap<TransmittableThreadLocal<Object>, Object>(holder.get());
        for (TransmittableThreadLocal<Object> threadLocal : ttlInstances.keySet()) {
            try {
                if (isBefore) threadLocal.beforeExecute();
                else threadLocal.afterExecute();
            } catch (Throwable t) {
                if (logger.isLoggable(Level.WARNING)) {
                    logger.log(Level.WARNING, "TTL exception when " + (isBefore ? "beforeExecute" : "afterExecute") + ", cause: " + t, t);
                }
            }
        }
    }

    /**
     * Transmittee
     */
    public interface Transmittee<C, B> {
        @NonNull
        C capture();
        @NonNull
        B replay(@NonNull C captured);
        @NonNull
        B clear();
        void restore(@NonNull B backup);
    }

    /**
     * Transmitter
     */
    public static class Transmitter {

        private static final Transmittee<
                HashMap<TransmittableThreadLocal<Object>, Object>,
                HashMap<TransmittableThreadLocal<Object>, Object>> ttlTransmittee =
                new Transmittee<HashMap<TransmittableThreadLocal<Object>, Object>, HashMap<TransmittableThreadLocal<Object>, Object>>() {
                    @NonNull
                    @Override
                    public HashMap<TransmittableThreadLocal<Object>, Object> capture() {
                        final HashMap<TransmittableThreadLocal<Object>, Object> ttl2Value = newHashMap(holder.get().size());
                        for (TransmittableThreadLocal<Object> threadLocal : holder.get().keySet()) {
                            ttl2Value.put(threadLocal, threadLocal.copyValue());
                        }
                        return ttl2Value;
                    }

                    @NonNull
                    @Override
                    public HashMap<TransmittableThreadLocal<Object>, Object> replay(@NonNull HashMap<TransmittableThreadLocal<Object>, Object> captured) {
                        final HashMap<TransmittableThreadLocal<Object>, Object> backup = newHashMap(holder.get().size());
                        for (final Iterator<TransmittableThreadLocal<Object>> iterator = holder.get().keySet().iterator(); iterator.hasNext(); ) {
                            TransmittableThreadLocal<Object> threadLocal = iterator.next();
                            // backup
                            backup.put(threadLocal, threadLocal.get());
                            // clear the TTL values that is not in captured
                            // avoid the extra TTL values after replay when run task
                            if (!captured.containsKey(threadLocal)) {
                                iterator.remove();
                                threadLocal.superRemove();
                            }
                        }
                        // set TTL values to captured
                        setTtlValuesTo(captured);
                        // call beforeExecute callback
                        doExecuteCallback(true);
                        return backup;
                    }

                    @NonNull
                    @Override
                    public HashMap<TransmittableThreadLocal<Object>, Object> clear() {
                        return replay(newHashMap(0));
                    }

                    @Override
                    public void restore(@NonNull HashMap<TransmittableThreadLocal<Object>, Object> backup) {
                        // call afterExecute callback
                        doExecuteCallback(false);
                        for (final Iterator<TransmittableThreadLocal<Object>> iterator = holder.get().keySet().iterator(); iterator.hasNext(); ) {
                            TransmittableThreadLocal<Object> threadLocal = iterator.next();
                            // clear the TTL values that is not in backup
                            // avoid the extra TTL values after restore
                            if (!backup.containsKey(threadLocal)) {
                                iterator.remove();
                                threadLocal.superRemove();
                            }
                        }
                        // restore TTL values
                        setTtlValuesTo(backup);
                    }
                };
        private static void setTtlValuesTo(@NonNull HashMap<TransmittableThreadLocal<Object>, Object> ttlValues) {
            for (Map.Entry<TransmittableThreadLocal<Object>, Object> entry : ttlValues.entrySet()) {
                TransmittableThreadLocal<Object> threadLocal = entry.getKey();
                threadLocal.set(entry.getValue());
            }
        }

        private static volatile WeakHashMap<ThreadLocal<Object>, TtlCopier<Object>> threadLocalHolder = new WeakHashMap<>();
        private static final Transmittee<
                HashMap<ThreadLocal<Object>, Object>,
                HashMap<ThreadLocal<Object>, Object>> threadLocalTransmittee =
                new Transmittee<HashMap<ThreadLocal<Object>, Object>, HashMap<ThreadLocal<Object>, Object>>() {
                    @NonNull
                    @Override
                    public HashMap<ThreadLocal<Object>, Object> capture() {
                        final HashMap<ThreadLocal<Object>, Object> threadLocal2Value = newHashMap(threadLocalHolder.size());
                        for (Map.Entry<ThreadLocal<Object>, TtlCopier<Object>> entry : threadLocalHolder.entrySet()) {
                            final ThreadLocal<Object> threadLocal = entry.getKey();
                            final TtlCopier<Object> copier = entry.getValue();
                            threadLocal2Value.put(threadLocal, copier.copy(threadLocal.get()));
                        }
                        return threadLocal2Value;
                    }

                    @NonNull
                    @Override
                    public HashMap<ThreadLocal<Object>, Object> replay(@NonNull HashMap<ThreadLocal<Object>, Object> captured) {
                        final HashMap<ThreadLocal<Object>, Object> backup = newHashMap(captured.size());
                        for (Map.Entry<ThreadLocal<Object>, Object> entry : captured.entrySet()) {
                            final ThreadLocal<Object> threadLocal = entry.getKey();
                            backup.put(threadLocal, threadLocal.get());

                            final Object value = entry.getValue();
                            if (value == threadLocalClearMark) threadLocal.remove();
                            else threadLocal.set(value);
                        }
                        return backup;
                    }

                    @NonNull
                    @Override
                    public HashMap<ThreadLocal<Object>, Object> clear() {
                        final HashMap<ThreadLocal<Object>, Object> threadLocal2Value = newHashMap(threadLocalHolder.size());

                        for (Map.Entry<ThreadLocal<Object>, TtlCopier<Object>> entry : threadLocalHolder.entrySet()) {
                            final ThreadLocal<Object> threadLocal = entry.getKey();
                            threadLocal2Value.put(threadLocal, threadLocalClearMark);
                        }

                        return replay(threadLocal2Value);
                    }

                    @Override
                    public void restore(@NonNull HashMap<ThreadLocal<Object>, Object> backup) {
                        for (Map.Entry<ThreadLocal<Object>, Object> entry : backup.entrySet()) {
                            final ThreadLocal<Object> threadLocal = entry.getKey();
                            threadLocal.set(entry.getValue());
                        }
                    }
                };

        private static final Set<Transmittee<Object, Object>> transmitteeSet = new CopyOnWriteArraySet<>();
        static {
            registerTransmittee(ttlTransmittee);
            registerTransmittee(threadLocalTransmittee);
        }
        @SuppressWarnings("unchecked")
        public static <C, B> boolean registerTransmittee(@NonNull Transmittee<C, B> transmittee) {
            return transmitteeSet.add((Transmittee<Object, Object>) transmittee);
        }

        @NonNull
        public static Object capture() {
            final HashMap<Transmittee<Object, Object>, Object> transmittee2Value = newHashMap(transmitteeSet.size());
            for (Transmittee<Object, Object> transmittee : transmitteeSet) {
                try {
                    transmittee2Value.put(transmittee, transmittee.capture());
                } catch (Throwable t) {
                    if (logger.isLoggable(Level.WARNING)) {
                        logger.log(Level.WARNING, "exception when Transmitter.capture for transmittee " + transmittee +
                                "(class " + transmittee.getClass().getName() + "), just ignored; cause: " + t, t);
                    }
                }
            }
            return new Snapshot(transmittee2Value);
        }

        @NonNull
        public static Object replay(@NonNull Object captured) {
            final Snapshot capturedSnapshot = (Snapshot) captured;
            final HashMap<Transmittee<Object, Object>, Object> transmittee2Value = newHashMap(capturedSnapshot.transmittee2Value.size());
            for (Map.Entry<Transmittee<Object, Object>, Object> entry : capturedSnapshot.transmittee2Value.entrySet()) {
                Transmittee<Object, Object> transmittee = entry.getKey();
                try {
                    Object transmitteeCaptured = entry.getValue();
                    transmittee2Value.put(transmittee, transmittee.replay(transmitteeCaptured));
                } catch (Throwable t) {
                    if (logger.isLoggable(Level.WARNING)) {
                        logger.log(Level.WARNING, "exception when Transmitter.replay for transmittee " + transmittee +
                                "(class " + transmittee.getClass().getName() + "), just ignored; cause: " + t, t);
                    }
                }
            }
            return new Snapshot(transmittee2Value);
        }

        @NonNull
        public static Object clear() {
            final HashMap<Transmittee<Object, Object>, Object> transmittee2Value = newHashMap(transmitteeSet.size());
            for (Transmittee<Object, Object> transmittee : transmitteeSet) {
                try {
                    transmittee2Value.put(transmittee, transmittee.clear());
                } catch (Throwable t) {
                    if (logger.isLoggable(Level.WARNING)) {
                        logger.log(Level.WARNING, "exception when Transmitter.clear for transmittee " + transmittee +
                                "(class " + transmittee.getClass().getName() + "), just ignored; cause: " + t, t);
                    }
                }
            }
            return new Snapshot(transmittee2Value);
        }

        public static void restore(@NonNull Object backup) {
            for (Map.Entry<Transmittee<Object, Object>, Object> entry : ((Snapshot) backup).transmittee2Value.entrySet()) {
                Transmittee<Object, Object> transmittee = entry.getKey();
                try {
                    Object transmitteeBackup = entry.getValue();
                    transmittee.restore(transmitteeBackup);
                } catch (Throwable t) {
                    if (logger.isLoggable(Level.WARNING)) {
                        logger.log(Level.WARNING, "exception when Transmitter.restore for transmittee " + transmittee +
                                "(class " + transmittee.getClass().getName() + "), just ignored; cause: " + t, t);
                    }
                }
            }
        }

        private static class Snapshot {
            final HashMap<Transmittee<Object, Object>, Object> transmittee2Value;
            public Snapshot(HashMap<Transmittee<Object, Object>, Object> transmittee2Value) {
                this.transmittee2Value = transmittee2Value;
            }
        }

        @SuppressWarnings("unchecked")
        public static <C, B> boolean unregisterTransmittee(@NonNull Transmittee<C, B> transmittee) {
            return transmitteeSet.remove((Transmittee<Object, Object>) transmittee);
        }

        public static <R> R runSupplierWithCaptured(@NonNull Object captured, @NonNull Supplier<R> bizLogic) {
            final Object backup = replay(captured);
            try {
                return bizLogic.get();
            } finally {
                restore(backup);
            }
        }

        public static <R> R runSupplierWithClear(@NonNull Supplier<R> bizLogic) {
            final Object backup = clear();
            try {
                return bizLogic.get();
            } finally {
                restore(backup);
            }
        }

        @SuppressFBWarnings("THROWS_METHOD_THROWS_CLAUSE_BASIC_EXCEPTION")
        public static <R> R runCallableWithCaptured(@NonNull Object captured, @NonNull Callable<R> bizLogic) throws Exception {
            final Object backup = replay(captured);
            try {
                return bizLogic.call();
            } finally {
                restore(backup);
            }
        }

        @SuppressFBWarnings("THROWS_METHOD_THROWS_CLAUSE_BASIC_EXCEPTION")
        public static <R> R runCallableWithClear(@NonNull Callable<R> bizLogic) throws Exception {
            final Object backup = clear();
            try {
                return bizLogic.call();
            } finally {
                restore(backup);
            }
        }

        private static final Object threadLocalHolderUpdateLock = new Object();
        private static final Object threadLocalClearMark = new Object();

        /**
         * Register the {@link ThreadLocal}(including subclass {@link InheritableThreadLocal}) instances
         * to enhance the <b>Transmittable</b> ability for the existed {@link ThreadLocal} instances.
         */
        public static <T> boolean registerThreadLocal(@NonNull ThreadLocal<T> threadLocal, @NonNull TtlCopier<T> copier) {
            return registerThreadLocal(threadLocal, copier, false);
        }

        /**
         * Register the {@link ThreadLocal}(including subclass {@link InheritableThreadLocal}) instances
         * to enhance the <b>Transmittable</b> ability for the existed {@link ThreadLocal} instances.
         */
        @SuppressWarnings("unchecked")
        public static <T> boolean registerThreadLocalWithShadowCopier(@NonNull ThreadLocal<T> threadLocal) {
            return registerThreadLocal(threadLocal, (TtlCopier<T>) shadowCopier, false);
        }
        private static final TtlCopier<Object> shadowCopier = parentValue -> parentValue;

        /**
         * Register the {@link ThreadLocal}(including subclass {@link InheritableThreadLocal}) instances
         * to enhance the <b>Transmittable</b> ability for the existed {@link ThreadLocal} instances.
         */
        @SuppressWarnings("unchecked")
        public static <T> boolean registerThreadLocal(@NonNull ThreadLocal<T> threadLocal, @NonNull TtlCopier<T> copier, boolean force) {
            if (threadLocal instanceof TransmittableThreadLocal) {
                logger.warning("register a TransmittableThreadLocal instance, this is unnecessary!");
                return true;
            }

            synchronized (threadLocalHolderUpdateLock) {
                if (!force && threadLocalHolder.containsKey(threadLocal)) return false;

                WeakHashMap<ThreadLocal<Object>, TtlCopier<Object>> newHolder = new WeakHashMap<>(threadLocalHolder);
                newHolder.put((ThreadLocal<Object>) threadLocal, (TtlCopier<Object>) copier);
                threadLocalHolder = newHolder;
                return true;
            }
        }

        @SuppressWarnings("unchecked")
        public static <T> boolean registerThreadLocalWithShadowCopier(@NonNull ThreadLocal<T> threadLocal, boolean force) {
            return registerThreadLocal(threadLocal, (TtlCopier<T>) shadowCopier, force);
        }

        public static <T> boolean unregisterThreadLocal(@NonNull ThreadLocal<T> threadLocal) {
            if (threadLocal instanceof TransmittableThreadLocal) {
                logger.warning("unregister a TransmittableThreadLocal instance, this is unnecessary!");
                return true;
            }
            synchronized (threadLocalHolderUpdateLock) {
                if (!threadLocalHolder.containsKey(threadLocal)) return false;
                WeakHashMap<ThreadLocal<Object>, TtlCopier<Object>> newHolder = new WeakHashMap<>(threadLocalHolder);
                newHolder.remove(threadLocal);
                threadLocalHolder = newHolder;
                return true;
            }
        }


        private Transmitter() {
            throw new InstantiationError("Must not instantiate this class");
        }

    }

    private static <K, V> HashMap<K, V> newHashMap(int expectedSize) {
        final float DEFAULT_LOAD_FACTOR = 0.75f;
        final int initialCapacity = (int) Math.ceil(expectedSize / (double) DEFAULT_LOAD_FACTOR);
        return new HashMap<>(initialCapacity, DEFAULT_LOAD_FACTOR);
    }

    @TestOnly
    static void dump(@Nullable String title) {
        if (title != null && title.length() > 0) {
            System.out.printf("Start TransmittableThreadLocal[%s] Dump...%n", title);
        } else {
            System.out.println("Start TransmittableThreadLocal Dump...");
        }
        for (TransmittableThreadLocal<Object> threadLocal : holder.get().keySet()) {
            System.out.println(threadLocal.get());
        }
        System.out.println("TransmittableThreadLocal Dump end!");
    }

    @TestOnly
    static void dump() {
        dump(null);
    }

}

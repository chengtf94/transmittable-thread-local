package com.alibaba.ttl.threadpool.agent;

import com.alibaba.ttl.threadpool.agent.internal.logging.Logger;
import com.alibaba.ttl.threadpool.agent.internal.transformlet.JavassistTransformlet;
import com.alibaba.ttl.threadpool.agent.internal.transformlet.impl.TtlExecutorTransformlet;
import com.alibaba.ttl.threadpool.agent.internal.transformlet.impl.TtlForkJoinTransformlet;
import com.alibaba.ttl.threadpool.agent.internal.transformlet.impl.TtlPriorityBlockingQueueTransformlet;
import com.alibaba.ttl.threadpool.agent.internal.transformlet.impl.TtlTimerTaskTransformlet;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.Instrumentation;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

/**
 * TTL Java Agent.
 *
 * @author Jerry Lee (oldratlee at gmail dot com)
 */
public final class TtlAgent {

    public static void premain(final String agentArgs, @NonNull final Instrumentation inst) {
        kvs = splitCommaColonStringToKV(agentArgs);

        Logger.setLoggerImplType(getLogImplTypeFromAgentArgs(kvs));
        final Logger logger = Logger.getLogger(TtlAgent.class);

        try {
            logger.info("[TtlAgent.premain] begin, agentArgs: " + agentArgs + ", Instrumentation: " + inst);
            final boolean disableInheritableForThreadPool = isDisableInheritableForThreadPool();

            final List<JavassistTransformlet> transformletList = new ArrayList<>();

            transformletList.add(new TtlExecutorTransformlet(disableInheritableForThreadPool));
            transformletList.add(new TtlPriorityBlockingQueueTransformlet());

            transformletList.add(new TtlForkJoinTransformlet(disableInheritableForThreadPool));

            if (isEnableTimerTask()) transformletList.add(new TtlTimerTaskTransformlet());

            final ClassFileTransformer transformer = new TtlTransformer(transformletList);
            inst.addTransformer(transformer, true);
            logger.info("[TtlAgent.premain] addTransformer " + transformer.getClass() + " success");

            logger.info("[TtlAgent.premain] end");

            ttlAgentLoaded = true;
        } catch (Exception e) {
            String msg = "Fail to load TtlAgent , cause: " + e;
            logger.log(Level.SEVERE, msg, e);
            throw new IllegalStateException(msg, e);
        }
    }

    private static String getLogImplTypeFromAgentArgs(@NonNull final Map<String, String> kvs) {
        return kvs.get(Logger.TTL_AGENT_LOGGER_KEY);
    }

    private static volatile Map<String, String> kvs;

    private static volatile boolean ttlAgentLoaded = false;

    public static boolean isTtlAgentLoaded() {
        return ttlAgentLoaded;
    }

    private static final String TTL_AGENT_ENABLE_TIMER_TASK_KEY = "ttl.agent.enable.timer.task";

    private static final String TTL_AGENT_DISABLE_INHERITABLE_FOR_THREAD_POOL = "ttl.agent.disable.inheritable.for.thread.pool";

    public static boolean isDisableInheritableForThreadPool() {
        return isBooleanOptionSet(kvs, TTL_AGENT_DISABLE_INHERITABLE_FOR_THREAD_POOL, false);
    }

    public static boolean isEnableTimerTask() {
        return isBooleanOptionSet(kvs, TTL_AGENT_ENABLE_TIMER_TASK_KEY, true);
    }

    private static boolean isBooleanOptionSet(@Nullable final Map<String, String> kvs, @NonNull String key, boolean defaultValue) {
        if (kvs == null) return defaultValue;

        final boolean containsKey = kvs.containsKey(key);
        if (!containsKey) return defaultValue;

        return !"false".equalsIgnoreCase(kvs.get(key));
    }

    @NonNull
    static Map<String, String> splitCommaColonStringToKV(@Nullable final String commaColonString) {
        final Map<String, String> ret = new HashMap<>();
        if (commaColonString == null || commaColonString.trim().length() == 0) return ret;

        final String[] splitKvArray = commaColonString.trim().split("\\s*,\\s*");
        for (String kvString : splitKvArray) {
            final String[] kv = kvString.trim().split("\\s*:\\s*");
            if (kv.length == 0) continue;

            if (kv.length == 1) ret.put(kv[0], "");
            else ret.put(kv[0], kv[1]);
        }

        return ret;
    }

    private TtlAgent() {
        throw new InstantiationError("Must not instantiate this class");
    }

}

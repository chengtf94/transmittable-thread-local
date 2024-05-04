package com.alibaba.ttl.threadpool;

import com.alibaba.ttl.spi.TtlWrapper;
import edu.umd.cs.findbugs.annotations.NonNull;

import java.util.concurrent.ThreadFactory;

/**
 * ThreadFactory包装类
 *
 * @author Jerry Lee (oldratlee at gmail dot com)
 */
public interface DisableInheritableThreadFactory extends ThreadFactory, TtlWrapper<ThreadFactory> {
    @NonNull
    @Override
    ThreadFactory unwrap();
}

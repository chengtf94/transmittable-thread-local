package com.alibaba.ttl.spi;

import com.alibaba.ttl.TtlUnwrap;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Ttl Wrapper interface.
 *
 * @author Jerry Lee (oldratlee at gmail dot com)
 */
public interface TtlWrapper<T> extends TtlEnhanced {
    @NonNull
    T unwrap();
}

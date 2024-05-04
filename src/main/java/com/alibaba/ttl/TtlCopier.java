package com.alibaba.ttl;

/**
 * TtlCopier
 *
 * @since 2.11.0
 */
@FunctionalInterface
public interface TtlCopier<T> {
    T copy(T parentValue);
}

package com.hyw.gdata.utils;

import java.io.Serializable;
import java.util.function.Function;

/**
 * 支持序列化的 Function
 *
 * @author hyw
 * @since 2021-06-25
 */
@FunctionalInterface
public interface QFunction<T, R> extends Function<T, R>, Serializable {
}

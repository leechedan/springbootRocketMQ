package com.maihaoche.starter.mq.base;

import java.util.Map;

/**
 * Create Date 2018-07-31
 *
 * @author <a href="mailto:lijianchao@gototw.com.cn">李建超</a>
 */
public interface ConsumerTagDeal<T> {

    boolean process(T message, Map<String, Object> extMap);

    String getTag();
}

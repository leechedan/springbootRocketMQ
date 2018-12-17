package com.maihaoche.starter.mq.base;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.remoting.exception.RemotingException;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by yipin on 2017/6/27.
 * RocketMQ的消费者(Push模式)处理消息的接口
 */
@Slf4j
public abstract class AbstractMQPushConsumer extends AbstractMQConsumer {

    @Getter
    @Setter
    private DefaultMQPushConsumer consumer;

    public AbstractMQPushConsumer() {
    }

    /***
     * 默认每个consumer只能消费一个tag
     */
    @Setter
    Map<String, ConsumerTagDeal> consumers = new HashMap<>();

    Map<String, Class<?>> mapClass = new HashMap<>();

    public boolean ignoreExt() {
        return false;
    }
    /**
     * 原生dealMessage方法，可以重写此方法自定义序列化和返回消费成功的相关逻辑
     *
     * @param list 消息列表
     * @param context 上下文
     * @return 消费状态
     */
    public <T> ConsumeConcurrentlyStatus dealMessage(List<MessageExt> list, ConsumeConcurrentlyContext context) {

        list.parallelStream().forEach(messageExt->{
            ConsumerTagDeal consumerTagDeal = consumers.get(messageExt.getTags());
            if (consumerTagDeal == null) {
                consumerTagDeal = consumers.get(messageExt.getTags().toLowerCase());
                if (consumerTagDeal == null) {
                    log.warn("consume fail , ask for re-consume , msgId: {} tags: {}", messageExt.getMsgId(), messageExt.getTags());
                    return ;
                }
            }
            Class<T> clz = null;
            if (mapClass.containsKey(messageExt.getTags())) {
                clz = (Class<T>)mapClass.get(messageExt.getTags());
            } else {
                Type[] types = consumerTagDeal.getClass().getGenericInterfaces();
                ParameterizedType parameterized = (ParameterizedType) types[0];

                clz = (Class<T>) parameterized.getActualTypeArguments()[0];
                mapClass.put(messageExt.getTags(), clz);
            }
            // parse message body
            T t = parseMessage(messageExt, clz);
            // parse ext properties
            Map<String, Object> ext = ignoreExt() ? null: parseExtParam(messageExt);
            if( null != t) {
                try {
                    consumerTagDeal.process(t, ext);
                } catch (Exception e) {
                    log.warn("send message back error ", e);
                    try {
                        log.warn("consume fail , ask for re-consume , msgId: {}", messageExt.getMsgId());
                        consumer.sendMessageBack(messageExt, 0, context.getMessageQueue().getBrokerName());
                    } catch (RemotingException e1) {
                        e1.printStackTrace();
                    } catch (MQBrokerException e1) {
                        e1.printStackTrace();
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    } catch (MQClientException e1) {
                        e1.printStackTrace();
                    }
                }
                return ;
            }
        });
        return  ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }

    /**
     * 原生dealMessage方法，可以重写此方法自定义序列化和返回消费成功的相关逻辑
     *
     * @param list 消息列表
     * @param consumeOrderlyContext 上下文
     * @return 处理结果
     */
    public  <T> ConsumeOrderlyStatus dealMessage(List<MessageExt> list, ConsumeOrderlyContext consumeOrderlyContext) {
        for(MessageExt messageExt : list) {
            log.info("receive msgId: {}, tags : {}" , messageExt.getMsgId(), messageExt.getTags());
            ConsumerTagDeal consumerTagDeal = consumers.get(messageExt.getTags());
            if (consumerTagDeal == null) {
                consumerTagDeal = consumers.get(messageExt.getTags().toLowerCase());
                if (consumerTagDeal == null) {
                    log.warn("consume fail , ask for re-consume , msgId: {} tags: {}", messageExt.getMsgId(), messageExt.getTags());
                    return  ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                }
            }
            Type[] types = consumerTagDeal.getClass().getGenericInterfaces();
            ParameterizedType parameterized = (ParameterizedType) types[0];

            Class<T> clz = (Class<T>) parameterized.getActualTypeArguments()[0];

            // parse message body
            T t = parseMessage(messageExt, clz);
            // parse ext properties
            Map<String, Object> ext = parseExtParam(messageExt);
            if( null != t && !consumerTagDeal.process(t, ext)) {
                log.warn("consume fail , ask for re-consume , msgId: {}", messageExt.getMsgId());
                return  ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
            }
        }
        return  ConsumeOrderlyStatus.SUCCESS;
    }
}

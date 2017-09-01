import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @author: zhulili1
 * @date: 2017/9/1
 * @description:
 */
public class Consumer {
    public static void main(String[] args) throws InterruptedException,
            MQClientException {

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(
                "producerGroupName2");
        consumer.setNamesrvAddr("127.0.0.1:9876");

        consumer.subscribe("testTopic", "tagA || tagB");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.registerMessageListener(new MessageListenerConcurrently() {

            public ConsumeConcurrentlyStatus consumeMessage(
                    List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                System.out.println(Thread.currentThread().getName()
                        + " Receive New Messages: " + msgs);
                MessageExt msg = msgs.get(0);
                if (msg.getTopic().equals("testTopic")) {
                    if (msg.getTags() != null && msg.getTags().equals("tagA")) {
                        // 获取消息体
                        String message = new String(msg.getBody());
                        System.out.println("receive tagA message:" + message);
                    } else if (msg.getTags() != null
                            && msg.getTags().equals("tagB")) {
                        // 获取消息体
                        String message = new String(msg.getBody());
                        System.out.println("receive tagB message:" + message);
                    }

                }
                // 成功
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.out.println("Consumer Started.");
    }

}

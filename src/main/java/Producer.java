import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;

/**
 * @author: zhulili1
 * @date: 2017/9/1
 * @description:
 */
public class Producer {
    public static void main(String[] args) throws MQClientException {

        DefaultMQProducer producer = new DefaultMQProducer("producerGroupName2");
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.start();
        try {
            {
                //实例化一个消息
                Message msg = new Message("testTopic","tagA","keyA",("Hello World tagA!").getBytes("UTF-8"));
                for(int i=50;i>0;i--){
                    if(i%2==0){
                        SendResult sendResult = producer.send(msg);
                        Thread.sleep(1000);
                        System.out.println("tagA send result:"+sendResult);
                    }else{
                        msg = new Message("testTopic","tagB","keyB",("Hello World tagB!").getBytes("UTF-8"));
                        SendResult sendResult = producer.send(msg);
                        Thread.sleep(1000);
                        System.out.println("tagB send result:"+sendResult);
                    }
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> producer.shutdown()));
        System.exit(0);
    }
}

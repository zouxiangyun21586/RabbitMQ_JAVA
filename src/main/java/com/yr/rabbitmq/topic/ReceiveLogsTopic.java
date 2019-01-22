package com.yr.rabbitmq.topic;

import com.rabbitmq.client.*;

import java.io.IOException;

/**
 * 主题交换机：Topic exchange
 * 
 * 消费者
 * 
 * @author zxy-un
 * 
 * 2018年9月11日 下午7:30:48
 */
public class ReceiveLogsTopic {

    private static final String EXCHANGE_NAME = "topic_logs"; // 交换机名

    public static void main(String[] argv) throws Exception {
    	//创建连接工厂
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("192.168.1.50"); // rabbitmq监听IP
		factory.setPort(5672); // rabbitmq监听默认端口
		factory.setUsername("zxy"); // 设置访问的用户
		factory.setPassword("root1"); // 用户密码
		
		
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        /**
		 * exchangeDeclare: 声明交换机
		 * 	参数一: 交换器名称(路由名)
		 * 	参数二: 交换器类型(direct, fanout, topic, headers)
		 */
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
        /*String queueName = channel.queueDeclare().getQueue();*/

        String queueName = "zxyTopic"; // 队列名对应后, 路由Key不一致也会消费掉,Topic不一致也会消费掉 ---------------
        /**
		 * queueBind: 把队列绑定到路由上
		 * 	参数一: 队列名称
		 * 	参数二: 交换器名称
		 * 	参数三: 路由Key (yr.# : 代表前缀是'yr.'的路由消息都可消费, 也可只接收一个路由消息(eg: yr.aaa))
		 */
        channel.queueDeclare(queueName, false, false, false, null); // 使用默认的队列需将此条注释使用
        channel.queueBind(queueName, EXCHANGE_NAME, "aa.bbb"); // 路由Key 随意写
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println(" [x] Received '" + envelope.getRoutingKey() + "':'" + message + "'");
            }
        };
        
        /**
         * 自动回复队列应答  -- RabbitMQ中的消息确认机制
         * 	参数一: 队列名称
         * 	参数二: true表示服务器等消息一次性交付过来, false表示服务器期望等到消息
         * 	参数三: 消费者对象(与消费者对接的接口)
         * 参考网址: https://blog.csdn.net/u014178917/article/details/80235001
         */
        channel.basicConsume(queueName, true, consumer);
    }
}
package com.yr.rabbitmq;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 消费者
 * @author zxy-un
 * 
 * 2018年9月11日 下午5:56:06
 */
public class Customer {

    private static final String QUEUE_NAME ="queue.test"; // 定义队列名称

    public static void main(String[] args) throws IOException, TimeoutException {
        //创建连接工厂
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("192.168.1.50"); // rabbitmq 连接 IP
        connectionFactory.setPort(5672); // java单节点连接端口 5672
        connectionFactory.setUsername("zxy"); // 用户名
        connectionFactory.setPassword("root1"); // 密码
        
        //创建一个连接
        Connection connection = connectionFactory.newConnection();
        
        //创建一个通道
        Channel channel = connection.createChannel();
        
        /**
         * 声明队列 (queueDeclare):
         * 	第一个参数表示: 队列名称
         * 	第二个参数表示: 是否持久化(true表示是, 队列将在服务器重启时生存), 默认是存放到内存中, rabbitmq重启会丢失
         * 	第三个参数表示: 是否是独占队列 (一般等于true的话用于一个队列只能有一个消费者来消费的场景)
         * 	第四个参数表示: 是否自动删除, 当所有消费者客户端连接断开时是否自动删除队列 (true: 自动删除)
         * 	第五个参数表示: 队列的其他参数
         * 注意: 如果使用同一套参数进行声明了, 就不能再使用其他参数来声明, 要么删除该队列重新创建, 要么给队列重新起一个名字
         * 	例如: 使用  channel.queueDeclare(QUEUE_NAME, false, false, false, null); 创建 ,那么要改变参数需重新创建队列, 或重起队列名
         */
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        System.out.println("Customer Waiting Received messages ( 客户端等待接收消息 )");
        
        /**
         * DefaultConsumer类实现了Consumer接口, 通过传入一个频道, 
         * 告诉服务器我们需要那个频道的消息, 如果频道中有消息, 就会执行回调函数 handleDelivery
         */
        Consumer consumer = new DefaultConsumer(channel){
            /**
             * handleDelivery:
             * 	参数一: 消费标签
             * 	参数二: 主要存放生产者相关信息 (比如交换机, 路由key等)
             * 	参数三: 基本属性
             * 	参数四: 消息实体
             * @throws IOException
             */
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8"); // 接收生产者发送过来的消息
                System.out.println("Customer Received ( 客户端接收 ) '" + message + "'");
            }
        };
        
        /**
         * 自动回复队列应答  -- RabbitMQ中的消息确认机制
         * 	参数一: 队列名称
         * 	参数二: true表示服务器等消息一次性交付过来, false表示服务器期望等到消息
         * 	参数三: 消费者对象(与消费者对接的接口)
         * 参考网址: https://blog.csdn.net/u014178917/article/details/80235001
         */
        channel.basicConsume(QUEUE_NAME, true, consumer);
        
    }
}
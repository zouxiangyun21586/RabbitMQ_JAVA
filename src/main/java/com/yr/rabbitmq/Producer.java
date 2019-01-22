package com.yr.rabbitmq;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * 生产者
 * @author zxy-un
 * 
 * 2018年9月11日 下午5:55:54
 */
public class Producer {
	
    private static final String QUEUE_NAME ="queue.test";

    public static void main(String[] args) throws Exception {
        //创建连接工厂
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("192.168.1.50"); // rabbitmq 连接 IP
        connectionFactory.setPort(5672);
        connectionFactory.setUsername("zxy"); // 用户名
        connectionFactory.setPassword("root1"); // 密码
        
        //创建一个连接
        Connection connection = connectionFactory.newConnection();
        
        //创建一个通道
        Channel channel = connection.createChannel();
        
        /**
         * queueDeclare: 声明队列
         * 	第一个参数表示队列名称
         * 	第二个参数为是否持久化（true表示是，队列将在服务器重启时生存）
         * 	第三个参数为是否是独占队列（创建者可以使用的私有队列，断开后自动删除）
         * 	第四个参数为当所有消费者客户端连接断开时是否自动删除队列
         * 	第五个参数为队列的其他参数	
         */
        channel.queueDeclare(QUEUE_NAME,false,false,false,null); // 这里只是创建队列
        String msg = "Rabbit Written ZXY"; // 要发送的消息

        /**
         * 发送消息到队列 (basicPublish) :
         * 	第一个参数表示: 交换机名称
         * 	第二个参数表示: 队列映射的路由key(路由键)
         * 	第三个参数表示: 消息的其他属性
         * 	第四个参数表示: 发送信息的主体(消息内容)
         */
        channel.basicPublish("",QUEUE_NAME,null,msg.getBytes("UTF-8")); // 这里才是发送消息
        System.out.println("Producer Send (生产者发送消息) +'" + msg + "'");
        
        Thread.sleep(100 * 1000);
        
        channel.close(); // 关闭通道
        connection.close(); // 关闭连接
    }
}
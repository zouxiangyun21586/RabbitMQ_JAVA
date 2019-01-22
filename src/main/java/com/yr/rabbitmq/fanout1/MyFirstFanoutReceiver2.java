package com.yr.rabbitmq.fanout1;

import java.io.IOException;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

/**
 * 扇形交换机：Fanout exchange
 * 
 * 消费者2
 * 
 * @author zxy-un
 * 
 * 2018年9月11日 下午5:56:18
 */
public class MyFirstFanoutReceiver2 {

	public static void main(String[] args) {
		Connection conn = null;
		Channel channel = null;
		try {
			// 初始化连接
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost("192.168.1.232"); // rabbitmq监听IP
			factory.setPort(5672); // rabbitmq监听默认端口
			factory.setUsername("zxy"); // 设置访问的用户
			factory.setPassword("root1"); // 用户密码
			
			// 创建连接
			conn = factory.newConnection();
			// 创建通道
			channel = conn.createChannel();
			// 声明交换机类型
			channel.exchangeDeclare("exchange_fanout", "fanout", true, false, null);

			// 声明默认的队列
			// 使用这种方法时, 需要先开启客户端再启动服务, 因为没有绑定, 队列不一致, 消费不到
			// String queue = channel.queueDeclare().getQueue();
			
			String queue = "bbb";
			// 将队列与交换机绑定，最后一个参数为routingKey,与发送者指定的一样""
			channel.queueDeclare(queue, false, false, false, null);
			channel.queueBind(queue, "exchange_fanout", "", null);
			Channel channel1 = channel;
			
			// 消费者
			Consumer consumer = new DefaultConsumer(channel1) {
				@Override
				public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties,
						byte[] body) throws IOException {
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					System.out.println(new String(body, "utf-8") + " BBBB " + "==============================");
				}
			};
			
			channel.basicConsume(queue, true, consumer); // 自动应答
			System.out.println("i am the first fanout receiver2 !");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

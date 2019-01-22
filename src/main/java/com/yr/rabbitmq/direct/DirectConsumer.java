package com.yr.rabbitmq.direct;
 
import java.io.IOException;
import java.util.concurrent.TimeoutException;
 
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

/**
 * 直连交换机：Direct exchange
 * 
 * 消费者
 * 
 * @author zxy-un
 * 
 * 2018年9月11日 下午5:55:17
 */
public class DirectConsumer {
	
	private static final String EXCHANGE_NAME = "exchange_direct"; // 定义交换器名称
	
	public static void main(String[] argv) throws IOException, TimeoutException  {
		new ExchangeDirect("logs.info");
		//new ExchangeDirect("logs.error");
		//new ExchangeDirect("logs.waring");
		
	}
 
	static class ExchangeDirect{
		public  ExchangeDirect(String routingKey) throws IOException, TimeoutException {
			//创建连接工厂
			ConnectionFactory factory = new ConnectionFactory();
//			factory.setHost("192.168.1.50"); // rabbitmq监听IP
			Address[] addrs = new Address[]{ // 集群连接
					new Address("192.168.1.50"), 
					new Address("192.168.1.52"), 
					new Address("192.168.1.53")};
			factory.setPort(5672); // rabbitmq监听默认端口
			factory.setUsername("zxy"); // 设置访问的用户
			factory.setPassword("root1"); // 用户密码
			
			Connection connection = factory.newConnection(addrs); // 创建连接
			Channel channel = connection.createChannel(); // 创建通道
			
			/**
			 * exchangeDeclare:
			 * 	参数一: 交换器名称(路由名)
			 * 	参数二: 交换器类型(direct, fanout, topic, headers)
			 * 	参数三: 是否持久化(设置为true表示持久化)
			 * 	参数四: 是否自动删除(设置为true则表示是自动删除) ------- 自动删除
			 * 	参数五: 其他参数
			 */
			channel.exchangeDeclare(EXCHANGE_NAME, "direct", true, false, null);
			
			/* 由交换机的Key与自己定义的名字组成的队列名称 */
			String queueName = routingKey + ".queue";
			
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
			channel.queueDeclare(queueName, false, false, false, null);
			
			/**
			 * queueBind: 把队列绑定到路由上
			 * 	参数一: 队列名称
			 * 	参数二: 交换器名称
			 * 	参数三: 路由Key
			 */
			channel.queueBind(queueName, EXCHANGE_NAME, routingKey);
			System.out.println(" [routingKey = "+ routingKey +"] Waiting for msg....");
 
			/**
	         * DefaultConsumer类实现了Consumer接口, 通过传入一个频道, 
	         * 告诉服务器我们需要那个频道的消息, 如果频道中有消息, 就会执行回调函数 handleDelivery
	         */
			Consumer consumer = new DefaultConsumer(channel) {
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
					System.out.println("[routingKey = "+ envelope.getRoutingKey() +"] Received msg is '" + message + "'");
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
 
}

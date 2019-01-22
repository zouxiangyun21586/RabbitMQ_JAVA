package com.yr.rabbitmq.fanout;

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
 * 消费者1
 * 
 * @author zxy-un
 * 
 * 2018年9月11日 下午5:56:41
 */
public class MyFirstFanoutReceiver {

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
			/**
			 * exchangeDeclare: 声明交换机
			 * 	参数一: 交换器名称(路由名)
			 * 	参数二: 交换器类型(direct, fanout, topic, headers)
			 * 	参数三: 是否持久化(设置为true表示持久化)
			 * 	参数四: 是否自动删除(设置为true则表示是自动删除)
			 * 	参数五: 其他参数
			 */
			channel.exchangeDeclare("exchange_fanout", "fanout", true, false, null);

			// 声明默认的队列 (会自己产生临时队列, 不过一旦关闭临时队列则消失)
			// 使用这种方法时, 需要先开启客户端再启动服务, 因为没有绑定, 队列不一致, 消费不到
			// String queue = channel.queueDeclare().getQueue();
			
			String queue = "bbb"; // 队列名
			/**
	         * 声明队列并将队列与交换机绑定 (queueDeclare):
	         * 	第一个参数表示: 队列名称
	         * 	第二个参数表示: 是否持久化(true表示是, 队列将在服务器重启时生存), 默认是存放到内存中, rabbitmq重启会丢失
	         * 	第三个参数表示: 是否是独占队列 (一般等于true的话用于一个队列只能有一个消费者来消费的场景)
	         * 	第四个参数表示: 是否自动删除, 当所有消费者客户端连接断开时是否自动删除队列 (true: 自动删除)
	         * 	第五个参数表示: 队列的其他参数 (最后一个参数为routingKey,与发送者指定的一样"")
	         * 注意: 如果使用同一套参数进行声明了, 就不能再使用其他参数来声明, 要么删除该队列重新创建, 要么给队列重新起一个名字
	         * 	例如: 使用  channel.queueDeclare(QUEUE_NAME, false, false, false, null); 创建 ,那么要改变参数需重新创建队列, 或重起队列名
	         */
			channel.queueDeclare(queue, false, false, false, null); // 使用默认的队列需将此条注释使用
			
			/**
			 * queueBind: 把队列绑定到路由上
			 * 	参数一: 队列名称
			 * 	参数二: 交换器名称
			 * 	参数三: 路由Key
			 * 	参数四: 其他参数
			 */
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
					System.out.println(new String(body, "utf-8") + " AAAA " + "--------------------------");
					channel1.basicAck(envelope.getDeliveryTag(),false); // 每次处理完成一个消息后，手动发送一次应答
					
				}
			};
			
			/**
	         * 自动回复队列应答  -- RabbitMQ中的消息确认机制
	         * 	参数一: 队列名称
	         * 	参数二: true表示服务器等消息一次性交付过来(自动应答机制), false表示服务器期望等到消息(手动应答)
	         * 	参数三: 消费者对象(与消费者对接的接口)
	         * 参考网址: https://blog.csdn.net/u014178917/article/details/80235001
	         * 自动应答:
	         * 手动应答:
	         */
			channel.basicConsume(queue, false, consumer); // 手动应答
			System.out.println("i am the first fanout receiver1 !");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

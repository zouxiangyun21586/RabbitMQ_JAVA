package com.yr.rabbitmq.direct;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * 直连交换机：Direct exchange
 * 
 * 生产者
 * 
 * @author zxy-un
 * 
 * 2018年9月11日 下午5:55:27
 */
public class DirectProducer {

	private static final String EXCHANGE_NAME = "exchange_direct"; // 交换机名

	public static void main(String[] argv) throws Exception {
		for (int i = 0; i < 10; i++) {
			new ExchangeDirect("logs.info", "logs Info test ！！");
		}
		// new ExchangeDirect("logs.error", "logs error test ！！");
		// new ExchangeDirect("logs.warning", "logs warning test ！！");
	}

	static class ExchangeDirect {

		public ExchangeDirect(String routingKey, String message) throws Exception {
			// 创建连接工厂
			ConnectionFactory factory = new ConnectionFactory();
//			factory.setHost("19.168.1.50"); // rabbitmq监听IP
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
			 * 	参数四: 是否自动删除(设置为true则表示是自动删除) 
			 * 	参数五: 其他参数
			 */
			channel.exchangeDeclare(EXCHANGE_NAME, "direct", true, false, null);

			/**
			 * 发送消息到队列 (basicPublish) : 
			 * 	第一个参数表示: 交换机名称 
			 * 	第二个参数表示: 队列映射的路由key(路由键)
			 * 	第三个参数表示: 消息的其他属性 
			 * 	第四个参数表示: 发送信息的主体(消息内容)
			 */
			channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes());
			System.out.println("[routingKey = " + routingKey + "] Sent msg is '" + message + "'");

			/* 根据要求加入不同队列 */ 
			/*int scoure = 50; // 假设scoure的分数为50
			if (scoure < 60) { // 如果小于60,为不及格进入这个队列
				channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes());
			} else if (scoure >= 60 && scoure < 80) { // 及格进入这个队列
				channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes());
			} else if (scoure >= 80 && scoure < 90) { // 良进入这个队列
				channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes());
			} else { // 否则就是优秀,进入这个队列
				channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes());
			}*/

			channel.close(); // 关闭通道
			connection.close(); // 关闭连接

		}

	}

}

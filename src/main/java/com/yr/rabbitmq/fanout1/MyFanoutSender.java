package com.yr.rabbitmq.fanout1;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * 扇形交换机：Fanout exchange
 * 
 * 生产者
 * 
 * @author zxy-un
 * 
 * 2018年9月11日 下午5:56:48
 */
public class MyFanoutSender {

	// 只有先绑定在交换机上的队列才能进行数据消费.
	private static final String MESSAGE = " 生产者发出的消息 ** "; // 消息

	public static void main(String[] args) {
		for (int i = 0; i < 100; i++) {
			Connection conn = null;
			Channel channel = null;
			try {
				// 初始化连接，主机，端口，用户名，密码可以自己定义
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
				 * exchangeDeclare: 
				 * 	参数一: 交换器名称(路由名) 
				 * 	参数二: 交换器类型(direct, fanout, topic, headers) 
				 * 	参数三: 是否持久化(设置为true表示持久化) 
				 * 		      与消费者相对应,例: 消费者是持久化,那么生产者也必须是持久化的,否则会对应不上报错
				 * 	参数四: 是否自动删除(设置为true则表示是自动删除) 
				 * 	参数五: 其他参数
				 */
				channel.exchangeDeclare("exchange_fanout", "fanout", true, false, null); // 定义fanout类型的交换机

				/**
				 * 发送消息到队列 (basicPublish) : 
				 * 	第一个参数表示: 交换机名称 
				 * 	第二个参数表示: routingKey 队列映射的路由key(路由键)
				 * 	第三个参数表示: 消息的其他属性 
				 * 	第四个参数表示: 发送信息的主体(消息内容)
				 */
				channel.basicPublish("exchange_fanout", "", null, MESSAGE.getBytes());
				System.out.println("I send a fanout massage!");
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				try {
					if (channel != null) {
						channel.close();
					}
					if (conn != null) {
						conn.close();
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
}

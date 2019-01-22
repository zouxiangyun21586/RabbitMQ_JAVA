package com.yr.rabbitmq.topic;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;

/**
 * 主题交换机：Topic exchange
 * 
 * 生产者 : Key(频道)要符合规范才能插入
 * 
 * @author zxy-un
 * 
 * 2018年9月11日 下午7:23:37
 */
public class EmitLogTopic {

	private static final String EXCHANGE_NAME = "topic_logs"; // 定义交换机名

	public static void main(String[] argv) {
		System.out.println("参数是:" + argv.toString());
		Connection connection = null;
		Channel channel = null;
		
		try {
			//创建连接工厂
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost("192.168.1.50"); // rabbitmq监听IP
			factory.setPort(5672); // rabbitmq监听默认端口
			factory.setUsername("zxy"); // 设置访问的用户
			factory.setPassword("root1"); // 用户密码

			connection = factory.newConnection();
			channel = connection.createChannel();

			/**
			 * exchangeDeclare: 声明交换机
			 * 	参数一: 交换器名称(路由名)
			 * 	参数二: 交换器类型(direct, fanout, topic, headers)
			 */
			channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);

			String routingKey = getRouting(); // 路由Key
			String message = getMessage(); // 消息

			/**
			 * 发送消息到队列 (basicPublish) : 
			 * 	第一个参数表示: 交换机名称 
			 * 	第二个参数表示: routingKey 队列映射的路由key(路由键)
			 * 	第三个参数表示: 消息的其他属性 
			 * 	第四个参数表示: 发送信息的主体(消息内容)
			 */
			channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes("UTF-8"));
			System.out.println(" [x] Sent '" + routingKey + "':'" + message + "'");

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (connection != null) {
				try {
					connection.close();
				} catch (Exception ignore) {
				}
			}
		}
	}

	// 若没有参数 ,则返回anoymous.info
	// 若有参数 ,则返回参数第一个
	private static String getRouting() { // 获取路由Key(可随意定义)
		return "yr.aaa";
	}

	// 若参数个数小于2,则返回hello world,否则返回joinstring()相应的值
	private static String getMessage() { // 获取需发送消息
		return "Hello RabbitMQ!";
	}

	// 返回输入的数组中从startindex开始的值,这些值以delimeter为分隔符。
	private static String joinStrings(String[] strings, String delimiter, int startIndex) {
		int length = strings.length;
		if (length == 0)
			return "";
		if (length < startIndex)
			return "";
		StringBuilder words = new StringBuilder(strings[startIndex]);
		for (int i = startIndex + 1; i < length; i++) {
			words.append(delimiter).append(strings[i]);
		}
		return words.toString();
	}
}
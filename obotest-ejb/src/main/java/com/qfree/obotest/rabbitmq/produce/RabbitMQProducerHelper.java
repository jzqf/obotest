package com.qfree.obotest.rabbitmq.produce;

import java.io.IOException;

import com.rabbitmq.client.Channel;

public interface RabbitMQProducerHelper {

	public void openConnection() throws IOException;

	public void closeConnection() throws IOException;

	public void openChannel() throws IOException;

	public void closeChannel() throws IOException;

	public Channel getChannel();

	//TODO Rename handlePublish --> ?????????????????????????????????
	public void handlePublish(byte[] messageBytes) throws IOException;

}
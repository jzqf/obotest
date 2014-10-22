package com.qfree.obotest.rabbitmq.consume;

import java.io.IOException;

import com.google.protobuf.InvalidProtocolBufferException;
import com.qfree.obotest.rabbitmq.RabbitMQMsgEnvelope;
import com.rabbitmq.client.Channel;

public interface RabbitMQConsumerHelper {

	public void openConnection() throws IOException;

	public void closeConnection() throws IOException;

	public void openChannel() throws IOException;

	public void closeChannel() throws IOException;

	public Channel getChannel();

	//	public void setAcknowledgementQueue(BlockingQueue<RabbitMQMsgAck> acknowledgementQueue);

	public void configureConsumer() throws IOException;

	public void handleNextDelivery(RabbitMQMsgEnvelope rabbitMQMsgEnvelope) throws
			InterruptedException, IOException, InvalidProtocolBufferException;

	//	public void acknowledgeMsg(RabbitMQMsgAck rabbitMQMsgAck);

}
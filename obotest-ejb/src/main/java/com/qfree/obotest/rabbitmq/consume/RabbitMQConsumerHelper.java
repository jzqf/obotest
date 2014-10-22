package com.qfree.obotest.rabbitmq.consume;

import java.io.IOException;

import javax.enterprise.event.ObserverException;

import com.google.protobuf.InvalidProtocolBufferException;
import com.qfree.obotest.rabbitmq.RabbitMQMsgEnvelope;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.ShutdownSignalException;

public interface RabbitMQConsumerHelper {

	public void openConnection() throws IOException;

	public void closeConnection() throws IOException;

	public void openChannel() throws IOException;

	public void closeChannel() throws IOException;

	public Channel getChannel();

	//	public void setAcknowledgementQueue(BlockingQueue<RabbitMQMsgAck> acknowledgementQueue);

	public void configureConsumer() throws IOException;

	public void handleNextDelivery(RabbitMQMsgEnvelope rabbitMQMsgEnvelope) throws
			InterruptedException, ShutdownSignalException, ConsumerCancelledException,
			ObserverException, IllegalArgumentException,
			InvalidProtocolBufferException;

	//	public void acknowledgeMsg(RabbitMQMsgAck rabbitMQMsgAck);

}
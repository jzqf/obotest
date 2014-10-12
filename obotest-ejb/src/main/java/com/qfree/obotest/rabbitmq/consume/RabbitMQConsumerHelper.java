package com.qfree.obotest.rabbitmq.consume;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import com.google.protobuf.InvalidProtocolBufferException;
import com.qfree.obotest.rabbitmq.RabbitMQMsgAck;

public interface RabbitMQConsumerHelper {

	public void openConnection() throws IOException;

	public void closeConnection() throws IOException;

	public void openChannel() throws IOException;

	public void closeChannel() throws IOException;

	public void setAcknowledgementQueue(BlockingQueue<RabbitMQMsgAck> acknowledgementQueue);

	public void configureConsumer() throws IOException;

	public void handleNextDelivery() throws InterruptedException, IOException, InvalidProtocolBufferException;

	public void acknowledgeMsg(RabbitMQMsgAck rabbitMQMsgAck) throws IOException;

}
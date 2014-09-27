package com.qfree.obotest.rabbitmq.consume;

import java.io.IOException;

import com.google.protobuf.InvalidProtocolBufferException;

public interface RabbitMQConsumerHelper {

	public void openConnection() throws IOException;

	public void closeConnection() throws IOException;

	public void openChannel() throws IOException;

	public void closeChannel() throws IOException;

	public void configureConsumer() throws IOException;

	public void handleDeliveries() throws InterruptedException, IOException, InvalidProtocolBufferException;

}
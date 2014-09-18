package com.qfree.obotest.rabbitmq.consume;

import java.io.IOException;

import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.ShutdownSignalException;

public interface RabbitMQConsumerHelper {

	public void openConnection() throws IOException;

	public void closeConnection() throws IOException;

	public void openChannel() throws IOException;

	public void closeChannel() throws IOException;

	public void configureConsumer() throws IOException;

	public void handleDeliveries() throws ShutdownSignalException,
			ConsumerCancelledException, InterruptedException, IOException;

}
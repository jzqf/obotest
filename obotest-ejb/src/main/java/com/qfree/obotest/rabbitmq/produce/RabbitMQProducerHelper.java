package com.qfree.obotest.rabbitmq.produce;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

//TODO This must be eliminated or updated to something related to producing:
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.ShutdownSignalException;

public interface RabbitMQProducerHelper {

	public void openConnection() throws IOException;

	public void closeConnection() throws IOException;

	public void openChannel() throws IOException;

	public void closeChannel() throws IOException;

	//TODO Is configureProducer() needed at all?
	public void configureProducer(BlockingQueue<byte[]> messageBlockingQueue) throws IOException;

	//TODO Rename handlePublish --> ?????????????????????????????????
	public void handlePublish() throws ShutdownSignalException,
			ConsumerCancelledException, InterruptedException, IOException;

}
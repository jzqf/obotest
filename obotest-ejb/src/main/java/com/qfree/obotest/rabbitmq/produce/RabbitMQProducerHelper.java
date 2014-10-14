package com.qfree.obotest.rabbitmq.produce;

import java.io.IOException;

public interface RabbitMQProducerHelper {

	public void openConnection() throws IOException;

	public void closeConnection() throws IOException;

	public void openChannel() throws IOException;

	public void closeChannel() throws IOException;

	//TODO Eliminate this method (also wherever it appears) if not needed:
	//	public void configureProducer(...) throws ..., e.g., IOException;

	//TODO Rename handlePublish --> ?????????????????????????????????
	public void handlePublish() throws InterruptedException, IOException;

}
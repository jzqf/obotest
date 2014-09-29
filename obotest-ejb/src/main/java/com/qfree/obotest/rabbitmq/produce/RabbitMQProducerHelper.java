package com.qfree.obotest.rabbitmq.produce;

import java.io.IOException;
//TODO This must be eliminated or updated to something related to producing:

public interface RabbitMQProducerHelper {

	public void openConnection() throws IOException;

	public void closeConnection() throws IOException;

	public void openChannel() throws IOException;

	public void closeChannel() throws IOException;

	//TODO Is configureProducer() needed at all?
	//	public void configureProducer(BlockingQueue<byte[]> producerMsgQueue) throws IOException;

	//TODO Rename handlePublish --> ?????????????????????????????????
	public void handlePublish() throws InterruptedException, IOException;

}
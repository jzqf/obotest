package com.qfree.obotest.rabbitmq.produce;

import java.io.IOException;
import java.util.SortedMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qfree.obotest.rabbitmq.RabbitMQMsgAck;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.ReturnListener;

public class RabbitMQProducerReturnListener implements ReturnListener {

	private static final Logger logger = LoggerFactory.getLogger(RabbitMQProducerReturnListener.class);

	private SortedMap<Long, RabbitMQMsgAck> pendingPublisherConfirms;

	public RabbitMQProducerReturnListener(SortedMap<Long, RabbitMQMsgAck> pendingPublisherConfirms) {
		this.pendingPublisherConfirms = pendingPublisherConfirms;
	}

	@Override
	public void handleReturn(int replyCode, String replyText, String exchange, String routingKey, BasicProperties properties, byte[] body)
			throws IOException {

		logger.error("A message has been returned by the broker because either the \"immediate\" flag"
				+ " or the \"mandatory\" flag was set and the broker could not satisfy this constraint."
				+ " Currently, this handler does nothing more than report that this occurred."
				+ " In order to perform a specific action, the details must be coded into this handler."
				+ " If the broker *also* reports this message on the ConfirmListener, then the original"
				+ " consumed message will be acked/nacked appropriately; otherwise, the original message"
				+ " will not be acknowledged at all, but should be requeued when the consumer is restarted."
				+ " However, in this case there will remain entries in the \"pending confirms map\" when"
				+ " this application is shut down, which will cause this application to hang for some"
				+ " period of time before it shuts down. This is because during shutdown, the application"
				+ " waits for this map to empty for terminating, but it will only wait for up to a "
				+ " pre-configured timeout period."
				+ " The details returned from the broker to this handler are the following:"
				+ "\nreplyCode = {}"
				+ "\nreplyText = {}"
				+ "\nexchange = {}"
				+ "\nroutingKey = {}"
				+ "\nproperties = {}"
				+ "\nmessage = {} bytes",
				replyCode,  replyText,  exchange,  routingKey,  properties, body.length);
 
	}

}

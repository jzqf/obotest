package com.qfree.obotest.rabbitmq.produce.passagetest1;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import javax.annotation.PreDestroy;
import javax.enterprise.event.Event;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qfree.obotest.event.PassageTest1Event;
import com.qfree.obotest.eventlistener.PassageQualifier;
import com.qfree.obotest.rabbitmq.RabbitMQMsgAck;
import com.qfree.obotest.rabbitmq.RabbitMQMsgEnvelope;
import com.qfree.obotest.rabbitmq.consume.RabbitMQConsumerController;
import com.qfree.obotest.rabbitmq.consume.RabbitMQConsumerController.AckAlgorithms;
import com.qfree.obotest.rabbitmq.produce.RabbitMQProducerController;
import com.qfree.obotest.rabbitmq.produce.RabbitMQProducerHelper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
//import com.qfree.obotest.eventsender.PassageProtos.Passage;

/*
 * This class is used as a base class for helper singleton EJBs (one for each 
 * producer thread). By using a base class, it is easy to ensure that all such 
 * classes have identical methods. Separate singleton classes are needed because
 * only one singleton object can be instantiated from a singleton EJB class, and
 * we want a different singleton object for each producer thread (to eliminate 
 * resource contention).
 * 
 * One slight drawback of using a common base class is that all logging is
 * associated with this base class, not the particular EJB singleton class that
 * extends it. To get around this, the constructor for this class sets the field
 * "subClassName" to
 * 
 *     this.getClass().getSimpleName()
 * 
 * Then this field can be included in log messages to make it clear which 
 * concrete subclass is logging the message.
 */
public abstract class RabbitMQProducerHelperPassageTest1 implements RabbitMQProducerHelper {

	private static final Logger logger = LoggerFactory.getLogger(RabbitMQProducerHelperPassageTest1.class);

	private static final String PASSAGE_QUEUE_NAME = "passage_queue_test1";
	/*
	 * Since this thread is never interrupted via Thread.interrupt(), we don't 
	 * want to block for any length of time so that this thread can respond to 
	 * state changes in a timely fashion. So this timeout should be "small".
	 */
	private static final long RABBITMQ_PRODUCER_TIMEOUT_MS = 1000;

	/*
	 * This field is used to enable the name of the subclass to be logged if 
	 * this class has been used to create a subclass. This field is set in the
	 * constructor for this class, but it will be set to the name of the 
	 * subclass if an instance of a subclass is constructed.
	 */
	private String subClassName = null;

	private Connection connection = null;
	private Channel channel = null;

    @Inject
	@PassageQualifier
	public Event<PassageTest1Event> passageEvent;

	public RabbitMQProducerHelperPassageTest1() {
		/*
		 * This will be the name of the subclass *if* an a an instance of a 
		 * subclass is constructed. Currently, this class is abstract so an
		 * object of this class will never be instantiate directly, but if this
		 * were done, this field will contain the name of this class, of course.
		 */
		this.subClassName = this.getClass().getSimpleName();
	}

	public void openConnection() throws IOException {

		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");

		// Do *not* end this URI with "/". It will not work.
		//		factory.setUri("amqp://guest:guest@trdds010.q-free.com:5672");

		//		factory.setHost("trdds010.q-free.com");
		//		factory.setPort(5672);
		//		factory.setUsername("guest");
		//		factory.setPassword("guest");
		//		factory.setVirtualHost("/");

		connection = factory.newConnection();
	}

	@Override
	public void closeConnection() throws IOException {
		if (connection != null) {
			connection.close();
		}
	}

	@Override
	public void openChannel() throws IOException {
		channel = connection.createChannel();
		channel.queueDeclare(PASSAGE_QUEUE_NAME, true, false, false, null);
		if (RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_PUBLISHED_TX) {
			/*
			 * Enable TX mode on this channel.
			 */
			channel.txSelect();
		}
	}

	@Override
	public void closeChannel() throws IOException {
		if (channel != null) {
			channel.close();
		}
	}

	//TODO Eliminate this method if not useful.
	//	@Override
	//	public void configureProducer(...) {
	//		...;
	//	}

	@Override
	public void handlePublish() throws InterruptedException, IOException {

		RabbitMQMsgEnvelope rabbitMQMsgEnvelope = RabbitMQProducerController.producerMsgQueue.poll(
				RABBITMQ_PRODUCER_TIMEOUT_MS, TimeUnit.MILLISECONDS);
		if (rabbitMQMsgEnvelope != null) {

			/*
			 * Extract from rabbitMQMsgEnvelope both the outgoing serialized 
			 * protobuf message to be published here as well as the 
			 * RabbitMQMsgAck object that is associated with the original 
			 * consumed RabbitMQ message (containing its delivery tag and other 
			 * details).
			 */
			byte[] passageBytes = rabbitMQMsgEnvelope.getMessage();
			RabbitMQMsgAck rabbitMQMsgAck = rabbitMQMsgEnvelope.getRabbitMQMsgAck();

			logger.debug("Publishing RabbitMQ passage message [{} bytes]...", passageBytes.length);
			//			logger.info("[{}]: Publishing RabbitMQ passage message [{} bytes]...", subClassName,
			//					passageBytes.length);

			//TODO Implement "PUBLISHER CONFIRMS" !!!!!!!!!
			channel.basicPublish("", PASSAGE_QUEUE_NAME, MessageProperties.PERSISTENT_BASIC, passageBytes);

			if (RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_PUBLISHED_TX) {
				/*
				 * This will block here until the RabittMQ broker that just received
				 * the published message has written the message to disk and performed
				 * some sort of fsync(), which takes a significant time to complete.
				 * Therefore, this acknowledgement algorithm, while very safe, will 
				 * probably be too slow in practice.
				 */
				channel.txCommit();
			}

			if (RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_PUBLISHED
					|| RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_PUBLISHED_TX) {
				rabbitMQMsgAck.queueAck();
			}

		} else {
			/*
			 * This just means that there were no messages in the queue to
			 * publish after waiting the timeout period. This in perfectly 
			 * normal. The timeout is implemented so that the calling thread
			 * can check whether there has been a request made for it to 
			 * terminate or whatever, even if this thread is not 
			 * interrupted. 
			 */
			//			logger.trace("q={} - After poll: No message.",
			//					RabbitMQProducerController.producerMsgQueue.remainingCapacity());
		}
	}

	@PreDestroy
	public void terminate() {
		logger.info("[{}]: This bean will now be destroyed by the container...", subClassName);
	}

}

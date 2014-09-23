package com.qfree.obotest.rabbitmq.produce.passagetest1;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.enterprise.event.Event;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qfree.obotest.event.PassageTest1Event;
import com.qfree.obotest.eventlistener.PassageQualifier;
import com.qfree.obotest.rabbitmq.produce.RabbitMQProducerHelper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
//TODO This must be eliminated or updated to something related to producing:
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.MessageProperties;
//TODO This must be eliminated or updated to something related to producing:
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;
//import com.qfree.obotest.eventsender.PassageProtos.Passage;

/*
 * This class is used as a base class for helper singleton EJBs
 * (one for each producer thread). By using a base class, it is easy to ensure 
 * that all such classes have identical methods. Separate singleton  classes are
 * needed because one singleton object can be instantiated from a singleton EJB'
 * class, and we want a different singleton object per producer thread (to 
 * eliminate resource contention).
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
	private static final long RABBITMQ_PRODUCER_TIMEOUT_MS = 5000;

	/*
	 * This field is used to enable the name of the subclass to be logged if 
	 * this class has been used to create a subclass. This field is set in the
	 * constructor for this class, but it will be set to the name of the 
	 * subclass if an instance of a subclass is constructed.
	 */
	String subClassName = null;

	Connection connection = null;
	Channel channel = null;
	//TODO This must be eliminated or updated to something related to producing:
	QueueingConsumer consumer = null;

	BlockingQueue<byte[]> messageBlockingQueue = null;

    @Inject
	@PassageQualifier
	Event<PassageTest1Event> passageEvent;

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
	}

	@Override
	public void closeChannel() throws IOException {
		if (channel != null) {
			channel.close();
		}
	}

	@Override
	//TODO Should we set this.messageBlockingQueue via a setter instead of this method?
	// If so, then we can consider eliminating or renaming this configureProducer() method.
	public void configureProducer(BlockingQueue<byte[]> messageBlockingQueue) {
		logger.debug("[{}]: Setting the blocking queue that will be used by this producer thread: {}", subClassName,
				messageBlockingQueue);
		this.messageBlockingQueue = messageBlockingQueue;
	}

	@Override
	//TODO Update this list of exceptions if necessary after I have written an implementation
	public void handlePublish() throws ShutdownSignalException,
			ConsumerCancelledException, InterruptedException, IOException {

		if (messageBlockingQueue != null) {

			logger.trace("[{}]: messageBlockingQueue.size() = {}", subClassName, messageBlockingQueue.size());
			logger.trace("[{}]: messageBlockingQueue.remainingCapacity() = {}", subClassName,
					messageBlockingQueue.remainingCapacity());

			byte[] passageBytes = messageBlockingQueue.poll(RABBITMQ_PRODUCER_TIMEOUT_MS, TimeUnit.MILLISECONDS);
			if (passageBytes!=null) {

				logger.debug("[{}]: Publishing RabbitMQ passage message [{} bytes]...", subClassName,
						passageBytes.length);
				channel.basicPublish("", PASSAGE_QUEUE_NAME, MessageProperties.PERSISTENT_BASIC, passageBytes);
				logger.debug("[{}]: Published RabbitMQ passage message", subClassName);

				logger.debug("[{}]: Sleeping for 10 ms...", subClassName);
				Thread.sleep(10);

			} else {
				/*
				 * This just means that there were no messages in the queue to
				 * publish after waiting the timeout period. This in perfectly 
				 * normal. The timeout is implemented so that the calling thread
				 * can check whether there has been a request made for it to 
				 * terminate or whatever, even if this thread is not 
				 * interrupted. 
				 */
				logger.trace("[{}]: messageBlockingQueue.poll() timed out after {} ms",
						subClassName, RABBITMQ_PRODUCER_TIMEOUT_MS);
			}
		} else {
			logger.error(
					"[{}]: messageBlockingQueue is null. This producer thread helper will not be able to send messages.",
					subClassName);
		}
	}

	//	@PreDestroy
	//	public void terminate() {
	//		logger.info("[{}]: @PreDestroy: What should/can I do here?...", subClassName);
	//	}

}

package com.qfree.obotest.passagetest1;

import java.io.IOException;

import javax.annotation.PreDestroy;
import javax.enterprise.event.Event;
import javax.enterprise.event.ObserverException;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.qfree.obotest.rabbitmq.RabbitMQMsgAck;
import com.qfree.obotest.rabbitmq.RabbitMQMsgEnvelope;
import com.qfree.obotest.rabbitmq.RabbitMQMsgEnvelopeQualifier_async;
import com.qfree.obotest.rabbitmq.RabbitMQMsgEnvelopeQualifier_sync;
import com.qfree.obotest.rabbitmq.consume.RabbitMQConsumerController;
import com.qfree.obotest.rabbitmq.consume.RabbitMQConsumerController.AckAlgorithms;
import com.qfree.obotest.rabbitmq.consume.RabbitMQConsumerHelper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
//import com.qfree.obotest.eventsender.PassageProtos.Passage;
import com.rabbitmq.client.ShutdownSignalException;

/*
 * This class is used as a base class for helper singleton EJBs (one for each 
 * consumer thread). By using a base class, it is easy to ensure that all such 
 * classes have identical methods. Separate singleton classes are needed because
 * only one singleton object can be instantiated from a singleton EJB class, and
 * we want a different singleton object for each consumer thread (to eliminate 
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
public abstract class RabbitMQConsumerHelperPassageTest1 implements RabbitMQConsumerHelper {

	private static final Logger logger = LoggerFactory.getLogger(RabbitMQConsumerHelperPassageTest1.class);

	private static final String PASSAGE_QUEUE_NAME = "passage_queue_test1";
	/*
	 * Since this thread is never interrupted via Thread.interrupt(), we don't 
	 * want to block for any length of time so that this thread can respond to 
	 * state changes in a timely fashion. So this timeout should be "small". In
	 * addition, we want the acknowledgment queue to be processed frequently in
	 * RabbitMQProducerRunnable (which calls the method handleNextDelivery() of 
	 * this class), so for this reason also this delay should be short.
	 */
	private static final long RABBITMQ_CONSUMER_TIMEOUT_MS = 200;

	/*
	 * This field is used to enable the name of the subclass to be logged if 
	 * this class has been used to create a subclass. This field is set in the
	 * constructor for this class, but it will be set to the name of the 
	 * subclass if an instance of a subclass is constructed.
	 */
	private String subClassName = null;

	private Connection connection = null;
	private Channel channel = null;
	private QueueingConsumer consumer = null;

	@Inject
	ConsumerMsgHandlerPassageTest1 consumerMsgHandler;

	@Inject
	@RabbitMQMsgEnvelopeQualifier_sync
	Event<RabbitMQMsgEnvelope> rabbitMQMsgEnvelopeEvent_sync;

	@Inject
	@RabbitMQMsgEnvelopeQualifier_async
	Event<RabbitMQMsgEnvelope> rabbitMQMsgEnvelopeEvent_async;

	/*
	 * This set was used to detect the situation where it appears that there
	 * are consumed messages that never will be acknowledged with the 
	 * AFTER_PUBLISHED_CONFIRMED acknowledgement algorithm. It does this by
	 * holding the delivery tags of all messages consumed in this thread. As
	 * messages are acknowledged, their delivery tags are removed from this set.
	 * Since, this is a sorted set, it is easy to monitor the minimum and 
	 * maximum values of the delivery keys in this set. If this difference gets
	 * too large, say over 100, then it is quite likely that the messages that
	 * correspond to the small delivery tag values will *never* be acknowledged.
	 * 
	 * Under normal operation, where this problem does not occur, there will
	 * normally be up to perhaps 20 delivery tags in this set and their values
	 * should always be increasing because they should correspond to messages
	 * that were very recently consumed, but which are not yet acknowledged. 
	 * If a problem occurs in the AFTER_PUBLISHED_CONFIRMED algorithm (e.g., one
	 * of the producer threads terminates unexpectedly), delivery tag 
	 * acknowledgement data will be lost and even though the producer thread 
	 * will be restarted by RabbitMQProducerController.heartBeat(), the 
	 * messages corresponding to the lost acknowledgement data will never be
	 * acknowledged in the consumer threads where they were originally consumed.
	 * 
	 * This technique was used to discover that 
	 * RabbitMQProducerConfirmListener.handleAck(...) was unexpectedly throwing 
	 * a java.util.ConcurrentModificationException because access to a data
	 * structure was not appropriately synchronized.
	 * 
	 * This set should no longer be needed, but rather than deleting it and all
	 * code that deals with it, the code is just commented out in case we 
	 * ever need to use it again. 
	 */
	//	private final SortedSet<Long> unackedDeliveryKeySet = Collections.synchronizedSortedSet(new TreeSet<Long>());

	public RabbitMQConsumerHelperPassageTest1() {
		/*
		 * This will be the name of the subclass *if* an a an instance of a 
		 * subclass is constructed. Currently, this class is abstract so an
		 * object of this class will never be instantiate directly, but if this
		 * were done, this field will contain the name of this class, of course.
		 */
		this.subClassName = this.getClass().getSimpleName();
	}

	public Channel getChannel() {
		return channel;
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

	public void closeConnection() throws IOException {
		if (connection != null) {
			connection.close();
		}
	}

	public void openChannel() throws IOException {
		channel = connection.createChannel();
		channel.queueDeclare(PASSAGE_QUEUE_NAME, true, false, false, null);
		if (RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_PUBLISHED) {
			/*
			 * Since an acknowledgement for a consumed message cannot be sent 
			 * until the message is fully processed *and* the outgoing message 
			 * is published, the channel must be configured to receive several 
			 * 5-10? messages before it sends an acknowledgment for an earlier
			 * consumed message. Simple testing seemed to indicate that setting
			 * this to 3-4 worked reasonably OK, but just to be sure I will use
			 * a larger value here. 
			 * 
			 * A value of 1 here means that the RabbitMQ broker will send us 
			 * maximum 1 unacknowledged message at any time. As soon as we 
			 * acknowledge a message, we are free to consume the single 
			 * (unacknowledged) message that the RabbitMQ broker sent us 
			 * earlier. The broker will then send us one more unacknowledged 
			 * message while we are working on the message we just consumed. 
			 * In this way, there should always one message at our end waiting
			 * to be consumed.
			 * 
			 * TODO This parameter should be tuned for the types of messages 
			 *      used in production, as well as the amount of processing done
			 *      on each message.
			 */
			channel.basicQos(10);
		} else if (RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_PUBLISHED_TX) {
			/*
			 * The explanation here is similar to that for the algorithm 
			 * AFTER_PUBLISHED.
			 */
			channel.basicQos(10);
		} else if (RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_PUBLISHED_CONFIRMED) {
			/*
			 * The explanation here is similar to that for the algorithm 
			 * AFTER_PUBLISHED. However, the total "round-trip time" for an 
			 * acknowledgement to be made for a consumed message is considerably
			 * longer for this algorithm than that for the AFTER_PUBLISHED
			 * algorithm since it is necessary here to wait for a confirm to be 
			 * reported by the broker that receives the published messages. 
			 * 
			 * It is reasonable to assume that this round-trip time will be 
			 * comparable than for the AFTER_PUBLISHED_TX algorithm. However, 
			 * unlike with the AFTER_PUBLISHED_TX algorithm, in this 
			 * AFTER_PUBLISHED_CONFIRMED algorithm we are able to continue 
			 * consuming messages while we wait for confirmations from the
			 * broker that receives the published messages. This means that we
			 * can improve the message throughput for this algorithm by 
			 * increasing the maximum number of unconfirmed messages on each
			 * RabbitMQ channel used for message consumption. That is why the 
			 * value specified here in channel.basicQos(...) is larger than for
			 * the other acknowledgment algorithms. However, it still needs to
			 * be optimized for each particular implementation.
			 */
			channel.basicQos(40);	// Optimize QOS for each implementation
		} else if (RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_RECEIVED) {
			channel.basicQos(1);
		} else {
			logger.warn("Untreated case for RabbitMQConsumerController.ackAlgorithm when setting QOS!");
			channel.basicQos(1);
		}
	}

	public void closeChannel() throws IOException {
		if (channel != null) {
			channel.close();
		}
	}

	public void configureConsumer() throws IOException {
		consumer = new QueueingConsumer(channel);

		boolean autoAck = false;
		//		if (RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AUTO) {
		//			autoAck = true;
		//		}
		channel.basicConsume(PASSAGE_QUEUE_NAME, autoAck, consumer);

		logger.info("Channel number = {}", channel.getChannelNumber());
	}

	public void handleNextDelivery(RabbitMQMsgEnvelope rabbitMQMsgEnvelope) throws
			InterruptedException, ShutdownSignalException, ConsumerCancelledException,
			ObserverException, IllegalArgumentException,
			InvalidProtocolBufferException {

		QueueingConsumer.Delivery delivery = consumer.nextDelivery(RABBITMQ_CONSUMER_TIMEOUT_MS);
		if (delivery != null) {

			/*
			 * rabbitMQMsgEnvelope is the object that will be sent to the 
			 * message "handler" to process the message just consumed. Here, the
			 * consumed message is placed in rabbitMQMsgEnvelope.
			 */
			byte[] messageBytes = delivery.getBody();
			rabbitMQMsgEnvelope.setMessage(messageBytes);

			/*
			 * Obtain the delivery tag for the consumed message. This is also
			 * stored in rabbitMQMsgEnvelope, not directly but within its 
			 * rabbitMQMsgAck attribute.
			 */
			long deliveryTag = delivery.getEnvelope().getDeliveryTag();
			RabbitMQMsgAck rabbitMQMsgAck = rabbitMQMsgEnvelope.getRabbitMQMsgAck();
			rabbitMQMsgAck.setDeliveryTag(deliveryTag);

			logger.debug("Received passage message: deliveryTag={}, {} bytes", deliveryTag, messageBytes.length);

			if (RabbitMQConsumerController.MESSAGE_HANDLER_USE_CDI_EVENTS) {
				/*
				 * Process the message in another method by firing a CDI event 
				 * here that is received by a method whose parameter is 
				 * annotated with @Observes and an appropriate qualifier.
				 */
				if (RabbitMQConsumerController.MESSAGE_HANDLER_ASYNCHRONOUS_CALLS) {
					/*
					 * Process the message asynchronously in another thread. For
					 * this to work, the parameter of target method must be 
					 * annotated with:
					 *     @RabbitMQMsgEnvelopeQualifier_async
					 */
					logger.debug("Firing CDI event asynchronously for {}...", rabbitMQMsgEnvelope);
					rabbitMQMsgEnvelopeEvent_async.fire(rabbitMQMsgEnvelope);
				} else {
					/*
					 * Process the message synchronously in this thread. For
					 * this to work, the parameter of target method must be 
					 * annotated with:
					 *     @RabbitMQMsgEnvelopeQualifier_sync
					 */
					logger.debug("Firing CDI event synchronously for {}...", rabbitMQMsgEnvelope);
					rabbitMQMsgEnvelopeEvent_sync.fire(rabbitMQMsgEnvelope);
				}
			} else {
				/*
				 * Process the message by directly calling a method, the
				 * traditional way.
				 */
				//logger.info("consumerMsgHandler = {}", consumerMsgHandler);
				if (RabbitMQConsumerController.MESSAGE_HANDLER_ASYNCHRONOUS_CALLS) {
					logger.debug("Asynchronous call to message handler. rabbitMQMsgEnvelope={}...", rabbitMQMsgEnvelope);
					consumerMsgHandler.processMessage_async(rabbitMQMsgEnvelope);
				} else {
					logger.debug("Synchronous call to message handler. rabbitMQMsgEnvelope={}...", rabbitMQMsgEnvelope);
					consumerMsgHandler.processMessage_sync(rabbitMQMsgEnvelope);
				}
			}

		} else {
			logger.info("********** NO MESSAGE **********"); //TODO DELETEME OR JUST COMMENT OUT?
			/*
			 * This just means that there were no messages to consume after
			 * waiting the timeout period. This in perfectly normal. The 
			 * timeout is implemented so that the calling thread can check 
			 * whether there has been a request made for it to terminate or 
			 * whatever, even if this thread is not interrupted. 
			 * 
			 * IMPORTANT:  If this message appears a lot, it may be necessary
			 *             to increase the "prefetch count" specified with
			 *             Channel.basicQos(...).
			 */
		}
	}

	@PreDestroy
	public void terminate() {
		logger.info("[{}]: This bean will now be destroyed by the container...", subClassName);
	}

}

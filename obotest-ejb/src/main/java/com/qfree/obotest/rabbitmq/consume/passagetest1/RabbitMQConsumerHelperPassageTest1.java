package com.qfree.obotest.rabbitmq.consume.passagetest1;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import javax.annotation.PreDestroy;
import javax.ejb.Lock;
import javax.ejb.LockType;
import javax.enterprise.event.Event;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.qfree.obotest.event.PassageTest1Event;
import com.qfree.obotest.eventlistener.PassageQualifier;
import com.qfree.obotest.protobuf.PassageTest1Protos;
import com.qfree.obotest.rabbitmq.RabbitMQMsgAck;
import com.qfree.obotest.rabbitmq.consume.RabbitMQConsumerController;
import com.qfree.obotest.rabbitmq.consume.RabbitMQConsumerController.AckAlgorithms;
import com.qfree.obotest.rabbitmq.consume.RabbitMQConsumerHelper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
//import com.qfree.obotest.eventsender.PassageProtos.Passage;

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

	/*
	 * This queue holds the RabbitMQ delivery tags and other details for 
	 * messages that are processed in other threads but which must be 
	 * acked/nacked in this consumer thread.
	 */
	private BlockingQueue<RabbitMQMsgAck> acknowledgementQueue;

    @Inject
	@PassageQualifier
	Event<PassageTest1Event> passageEvent;

	public RabbitMQConsumerHelperPassageTest1() {
		/*
		 * This will be the name of the subclass *if* an a an instance of a 
		 * subclass is constructed. Currently, this class is abstract so an
		 * object of this class will never be instantiate directly, but if this
		 * were done, this field will contain the name of this class, of course.
		 */
		this.subClassName = this.getClass().getSimpleName();
	}

	public void setAcknowledgementQueue(BlockingQueue<RabbitMQMsgAck> acknowledgementQueue) {
		this.acknowledgementQueue = acknowledgementQueue;
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
		channel.basicQos(10);
	}

	public void closeChannel() throws IOException {
		if (channel != null) {
			channel.close();
		}
	}

	public void configureConsumer() throws IOException {
		consumer = new QueueingConsumer(channel);
		channel.basicConsume(PASSAGE_QUEUE_NAME, false, consumer);
		logger.info("Channel number = {}", channel.getChannelNumber());
	}

	public void handleNextDelivery() throws InterruptedException, IOException, InvalidProtocolBufferException {

		QueueingConsumer.Delivery delivery = consumer.nextDelivery(RABBITMQ_CONSUMER_TIMEOUT_MS);
		if (delivery != null) {

			long deliveryTag = delivery.getEnvelope().getDeliveryTag();

			byte[] passageBytes = delivery.getBody();

			//			logger.debug("[{}]: Received passage: {} bytes", subClassName, passageBytes.length);

			if (false) {

				/*
				 * Process the message synchronously, here in this thread.
				 * 
				 * TODO Update this to publish an outgoing message
				 * or place and outgoing method in the producer queue, as in ConsumerMsgHandlerPassageTest1?
				 * TODO WE also need to enter an element in the acknowledgement queue if mode=AFTER_SEND
				 */
				PassageTest1Protos.PassageTest1 passage = PassageTest1Protos.PassageTest1.parseFrom(passageBytes);
				String filename = passage.getImageName();
				byte[] imageBytes = passage.getImage().toByteArray();
				logger.debug("[{}]:     Image name = '{}' ({} bytes)", subClassName, filename, imageBytes.length);

			} else {

				/*
				 * Process the message asynchronously in another thread that 
				 * receives a CDI event that is sent from this thread.
				 */

				/*
				 * TODO CAN WE HANDLE THE CASE WHERE THE OBSERVER THREAD CANNOT PROCESS THE MESSAGE FOR SOME REASON?
				 * TODO If there is something wrong with the RabbitMQ message payload...
				 * we might want to sent it to a special RabbitMQ exchange that collects
				 * bad messages, instead of processing it here through the normal workflow?
				 */
				this.firePassageEvent(passageBytes, deliveryTag);

			}

			if (RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_RECEIVED) {
				channel.basicAck(deliveryTag, false);
			}

		} else {
			logger.info("********** NO MESSAGE **********"); //TODO DELETEME
			/*
			 * This just means that there were no messages to consume after
			 * waiting the timeout period. This in perfectly normal. The 
			 * timeout is implemented so that the calling thread can check 
			 * whether there has been a request made for it to terminate or 
			 * whatever, even if this thread is not interrupted. 
			 */
		}
	}

	/**
	 * Acknowledges a consumed RabbitMQ message, according to the supplied
	 * RabbitMQMsgAck argument.
	 * @throws IOException 
	 */
	public void acknowledgeMsg(RabbitMQMsgAck rabbitMQMsgAck) throws IOException {
		if (!rabbitMQMsgAck.isRejected()) {
			// Acknowledge the message, and only this message.
			logger.info("Acking message: {}", rabbitMQMsgAck);
			channel.basicAck(rabbitMQMsgAck.getDeliveryTag(), false);
		} else {
			/*
			 * Reject the message, and request that it be requeued or not 
			 * according to the value of rabbitMQMsgAck.isRequeueRejectedMsg().
			 */
			logger.info("Nacking message: {}", rabbitMQMsgAck);
			channel.basicNack(rabbitMQMsgAck.getDeliveryTag(), false, rabbitMQMsgAck.isRequeueRejectedMsg());
		}
	}

	/*
	 * By making this method (as well as the method that receives the fired 
	 * event) asynchronous, the event mechanism becomes a sort of 
	 * "fire-and-forget" call. If we do annotate this method here with 
	 * @Asynchronous, but not the receiving (@Observes) method, then the Java EE
	 * container will use a pool of threads to fire many events (one in each 
	 * thread), but each thread will wait for the event to be processed before 
	 * proceeding, so it isn't fully asynchronous and nothing is gained. It is
	 * important, therefore, to just follow the rule that @Asynchronous be used 
	 * on both the method that fires the event as well as the method that 
	 * receives (@Observes) the event.
	 * 
	 * Note:  Testing seems to show that it is not actually necessary to 
	 *        annotate with @Asynchronous this method here that does the firing,
	 *        i.e., the key thing is to annotate the method that receives
	 *        (observes) the event. But for the time being I will do this until 
	 *        a reason becomes apparent for not doing it.
	 * 
	 * TODO Investigate if/how we can get information on how the event was processed by the receiver.
	 * 
	 * TODO Should I send the raw Protobuf message in the event object and then parse it in the @Observer method????!!!
	 * 
	 */
	//TODO I AM NOT SURE THIS IS REALLY AN ASYNCHRONOUS CALL. TEST WITH AND WIHTOUT THIS @Asynchronous
	//	@Asynchronous
	@Lock(LockType.WRITE)
	private void firePassageEvent(byte[] passageBytes, long deliveryTag) {

		RabbitMQMsgAck rabbitMQMsgAck = new RabbitMQMsgAck(acknowledgementQueue, deliveryTag);

		try {

			PassageTest1Protos.PassageTest1 passage = PassageTest1Protos.PassageTest1.parseFrom(passageBytes);
			String filename = passage.getImageName();
			byte[] imageBytes = passage.getImage().toByteArray();

			PassageTest1Event passagePayload = new PassageTest1Event();
			passagePayload.setRabbitMQMsgAck(rabbitMQMsgAck);
			passagePayload.setImageName(filename);
			passagePayload.setImageBytes(imageBytes);

			if (RabbitMQConsumerController.unacknowledgeCDIEventsCounterSemaphore.tryAcquire()) {
				logger.debug("[{}]: Firing CDI event for {}, UnackedAvailPermits={}", subClassName, passagePayload,
						RabbitMQConsumerController.unacknowledgeCDIEventsCounterSemaphore.availablePermits());
				try {
					passageEvent.fire(passagePayload);
				} catch (Throwable e) {
					/*
					 * If an exception was thrown firing a CDI event (I don't
					 * know if this is even possible, we release the permit just
					 * acquired because there will probably be no session bean 
					 * to receive the event that would normally do this. 
					 */
					RabbitMQConsumerController.unacknowledgeCDIEventsCounterSemaphore.release();
					logger.error("[{}]: An exception was caught firing a CDI event: {}", subClassName, e);

					/*
					 * TODO If an error occurs here or in one of the other blocks of code in this method,
					 * 		instead of entering rabbitMQMsgAck into the acknowledgement queue, return enough
					 *      information to the handleNextDelivery() method and perform the
					 *      acknowledgement there, even if mode=AFTER_SENT, as I describe in my "ToDo - Work.txt"
					 *      document in the section that starts with:
					 *      "Try to perform all acknowledgement queue manipulations in the consumer thread "run" method ..."
					 */
					if (RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_SENT) {
						rabbitMQMsgAck.setRejected(true);
						rabbitMQMsgAck.setRequeueRejectedMsg(true);
						//TODO We must enter rabbitMQMsgAck into the acknowledgement queue in, acknowledgementQueue
					}
				}
				logger.debug("[{}]: Returned from firing event", subClassName);
			} else {
				if (RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_SENT) {
					rabbitMQMsgAck.setRejected(true);
					rabbitMQMsgAck.setRequeueRejectedMsg(true);
					//TODO We must enter rabbitMQMsgAck into the acknowledgement queue, acknowledgementQueue
					logger.warn("Permit not acquired for CDI event to be sent.");
				} else {
					logger.warn("\n**********\nPermit not acquired for CDI event to be sent. The message will be lost!\n**********");
				}
			}

		} catch (InvalidProtocolBufferException e) {
			logger.error("[{}]: An InvalidProtocolBufferException was thrown. The message will be lost!", subClassName);
			logger.error("Exception details:", e);
		}

	}

	@PreDestroy
	public void terminate() {
		logger.info("[{}]: This bean will now be destroyed by the container...", subClassName);
	}

}

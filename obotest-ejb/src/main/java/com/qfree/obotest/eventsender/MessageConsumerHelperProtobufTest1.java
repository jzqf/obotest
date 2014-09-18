package com.qfree.obotest.eventsender;

import java.io.IOException;

import javax.ejb.Asynchronous;
import javax.ejb.Lock;
import javax.ejb.LockType;
import javax.enterprise.event.Event;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.qfree.obotest.event.PassageTest1Event;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;
//import com.qfree.obotest.eventsender.PassageProtos.Passage;

/*
 * This class is used as a base class for helper singleton EJBs
 * (one for each consumer thread). By using a base class, it is easy to ensure 
 * that all such classes have identical methods. Separate singleton  classes are
 * needed because one singleton object can be instantiated from a singleton EJB'
 * class, and we want a different singleton object per consumer thread (to 
 * eliminate resource contention).
 * 
 * One slight drawback of using a common base class is that all logging is
 * associated with this base class, not the particular EJB singleton class that
 * extends it. One way to get around this is to include:
 * 
 *     this.getClass().getName()  or this.getClass().getSimpleName()
 * 
 * in the log message. To make this as efficient as possible, set the member
 * attribute "subClassName" to this value when an abject of a subclass is 
 * constructed.
 */
public abstract class MessageConsumerHelperProtobufTest1 implements MessageConsumerHelper {

	private static final Logger logger = LoggerFactory.getLogger(MessageConsumerHelperProtobufTest1.class);

	private static final String PASSAGE_QUEUE_NAME = "passage_queue_test1";
	private static final long RABBITMQ_CONSUMER_TIMEOUT_MS = 5000;

	/*
	 * This field is used to enable the name of the subclass to be logged if 
	 * this class has been used to create a subclass. It is the duty of the 
	 * subclass to set this field to this.getClass().getSimpleName() or to
	 * this.getClass().getName(), probably in its constructor.
	 */
	String subClassName = null;

	Connection connection = null;
	Channel channel = null;
	QueueingConsumer consumer = null;

    @Inject
	@PassageQualifier
	Event<PassageTest1Event> passageEvent;

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
		channel.basicQos(1);
	}

	public void closeChannel() throws IOException {
		if (channel != null) {
			channel.close();
		}
	}

	public void configureConsumer() throws IOException {
		consumer = new QueueingConsumer(channel);
		channel.basicConsume(PASSAGE_QUEUE_NAME, false, consumer);
	}

	public void handleDeliveries() throws ShutdownSignalException,
			ConsumerCancelledException, InterruptedException, IOException {
		QueueingConsumer.Delivery delivery = consumer.nextDelivery(RABBITMQ_CONSUMER_TIMEOUT_MS);
		if (delivery != null) {

			byte[] passageBytes = delivery.getBody();

			logger.debug("[{}]: Received passage: {} bytes", subClassName, passageBytes.length);


			if (false) {

				/*
				 * Processing here in this method.
				 */
				PassageProtos.Passage passage = PassageProtos.Passage.parseFrom(passageBytes);
				String filename = passage.getImageName();
				byte[] imageBytes = passage.getImage().toByteArray();
				logger.debug("[{}]:     Image name = '{}' ({} bytes)", subClassName, filename, imageBytes.length);

			} else {

				/*
				 * Processing in another thread theat receives a CDI event that
				 * is sent from this thread.
				 */

				/*
				 * TODO CAN WE HANDLE THE CASE WHERE THE OBSERVER THREAD CANNOT PROCESS THE MESSAGE FOR SOME REASON?
				 * 
				 * How about having the "Observer" thread that receives the event fired
				 * from this thread fire an event back to this thread to tell it to 
				 * acknowledge the event? The acknowledgement event cannot be sent to the 
				 * main thread class because that class is not managed by the container. Therefore,
				 * it would be necessary to fire the acknowledgement event back to the
				 * singleton helper bean to perform the acknowledgement.
				 * In order to implement this, it would be necessary to (at least):
				 * 
				 * 		1.	Send the "delivery tag" along with
				 * 			rest of the event payload to the "Observer" method from this 
				 * 			thread and then later fire this "delivery tag" back to 
				 * 			the singleton helper bean via the acknowledgement event
				 * 			sent from the "Observer" method that processes the RabbitMQ
				 * 			message payload.  Is this possible?
				 * 		
				 * This will mean that we cannot just forward the raw message bytes to the
				 * "Observer" method from this thread, because we need to also send the
				 * "delivery tag"
				 * 
				 * This will be more complicate if we use several RabbitMQ consumer threads,
				 * because it will also be necessary to ensure that the acknowledgement event 
				 * is fired back to the appropriate thread that has the reference to the correct 
				 * channel on which the acknowledgement must be sent.
				 * 
				 * It is probably best to just wait and deal with this functionality at
				 * a later time, since it may never be needed.
				 */

				/*
				 * TODO If there is something wrong with the RabbitMQ message payload, 
				 * we might want to sent it to a special RabbitMQ exchange that collects
				 * bad messages, instead of processing it here through the normal workflow?
				 */

				logger.debug("[{}]: Requesting bean to fire an asynchronous \"passage\" CDI event...", subClassName);
				this.firePassageEvent(passageBytes);
				logger.debug("[{}]: Returned from request to fire the asynchronous \"passage\" CDI event", subClassName);

			}

			channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);

		} else {
			/*
			 * This just means that there were no messages to consume after
			 * waiting the timeout period. This in perfectly normal. The 
			 * timeout is implemented so that the calling thread can check 
			 * whether there has been a request made for it to terminate or 
			 * whatever. 
			 */
			logger.trace("[{}]: consumer.nextDelivery() timed out after {} ms",
					subClassName, RABBITMQ_CONSUMER_TIMEOUT_MS);
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
	 * TODO Should I send the raw Protobuf message in the event object and then parse it in the @Observer method?
	 * 
	 */
	//TODO I AM NOT SURE THIS IS REALLY AN ASYNCHRONOUS CALL. i SHOULD PROBABLY REMOVE IT HERE AND IN THE OTHER CLASS
	@Asynchronous
	@Lock(LockType.WRITE)
	private void firePassageEvent(byte[] passageBytes) {
		logger.debug("[{}]: Creating event payload for passage [{} bytes]", subClassName, passageBytes.length);

		try {

			PassageProtos.Passage passage = PassageProtos.Passage.parseFrom(passageBytes);
			String filename = passage.getImageName();
			byte[] imageBytes = passage.getImage().toByteArray();

			PassageTest1Event passagePayload = new PassageTest1Event();
			passagePayload.setImage_name(filename);
			passagePayload.setImageBytes(imageBytes);

			logger.debug("[{}]: Firing CDI event for {}", subClassName, passagePayload);
			passageEvent.fire(passagePayload);
			logger.debug("[{}]: Returned from firing event", subClassName);

		} catch (InvalidProtocolBufferException e) {
			logger.error("[{}]: An InvalidProtocolBufferException was thrown", subClassName);
			logger.error("Exception details:", e);
		}

	}

	//	@PreDestroy
	//	public void terminate() {
	//		logger.info("[{}]: @PreDestroy: What should/can I do here?...", subClassName);
	//	}

}

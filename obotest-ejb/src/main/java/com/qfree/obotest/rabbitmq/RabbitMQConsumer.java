package com.qfree.obotest.rabbitmq;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qfree.obotest.eventsender.MessageConsumerHelper;
import com.qfree.obotest.rabbitmq.RabbitMQConsumerController.RabbitMQConsumerControllerStates;
import com.qfree.obotest.rabbitmq.RabbitMQConsumerController.RabbitMQConsumerStates;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

/*
 * This class is instantiated explicitly with "new"; hence, it cannot be managed
 * by the Java EE application container. Therefore, it makes no sense to use
 * the annotations, @Stateless, or @LocalBean here. For the same reason it is 
 * not possible to use dependency injection so the @EJB annotation cannot be 
 * used either.
 */
//@Stateless
//@LocalBean
public class RabbitMQConsumer implements Runnable {

	private static final String IMAGE_QUEUE_NAME = "image_queue";
	private static final long RABBITMQ_CONSUMER_TIMEOUT_MS = 5000;

	private volatile RabbitMQConsumerStates state = RabbitMQConsumerStates.STOPPED;

	private static final Logger logger = LoggerFactory.getLogger(RabbitMQConsumer.class);

	//	@EJB - cannot use because this class is not instantiated by the container
	RabbitMQConsumerController rabbitMQConsumerController = null;

	MessageConsumerHelper messageConsumerHelper = null;

	public RabbitMQConsumerStates getState() {
		return state;
	}

	public void setState(RabbitMQConsumerStates state) {
		this.state = state;
	}

	/*
	 * This constructor is necessary, since this is a stateless session bean,
	 * even though all instances of this bean used by the application are 
	 * created with the "new" operator and use a constructor with arguments.
	 */
	public RabbitMQConsumer() {
	}

	public RabbitMQConsumer(
			RabbitMQConsumerController rabbitMQConsumerController,
			MessageConsumerHelper messageConsumerHelper) {
		super();
		this.rabbitMQConsumerController = rabbitMQConsumerController;
		this.messageConsumerHelper = messageConsumerHelper;
	}

	@Override
	public void run() {

		logger.debug("Starting RabbitMQ message consumer...");

		this.setState(RabbitMQConsumerStates.RUNNING);

		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");

		// Do *not* end this URI with "/". It will not work.
		//		factory.setUri("amqp://guest:guest@trdds010.q-free.com:5672");

		//		factory.setHost("trdds010.q-free.com");
		//		factory.setPort(5672);
		//		factory.setUsername("guest");
		//		factory.setPassword("guest");
		//		factory.setVirtualHost("/");

		Connection connection = null;
		Channel channel = null;
		try {

			connection = factory.newConnection();

			try {

				channel = connection.createChannel();
				channel.queueDeclare(IMAGE_QUEUE_NAME, true, false, false, null);
				channel.basicQos(1);

				try {

					QueueingConsumer consumer = new QueueingConsumer(channel);
					channel.basicConsume(IMAGE_QUEUE_NAME, false, consumer);

					logger.debug("Waiting for image(s).");

					while (true) {
						try {

							QueueingConsumer.Delivery delivery = consumer.nextDelivery(RABBITMQ_CONSUMER_TIMEOUT_MS);
							if (delivery != null) {

								byte[] imageBytes = delivery.getBody();

								logger.debug("Received image: " + imageBytes.length + " bytes");

								/*
								 * TODO CAN WE HANDLE THE CASE WHERE THE OBSERVER THREAD CANNOT PROCESS THE MESSAGE FOR SOME REASON?
								 * 
								 * How about having the "Observer" thread that receives the event fired
								 * from this thread fire an event back to this thread to tell it to 
								 * acknowledge the event? The acknowledgement event cannot be sent to this 
								 * class because this class is not managed by the container. Therefore,
								 * it would be necessary to fire the acknowledgement event back to the
								 * singleton helper bean, messageConsumerHelper, to perform the acknowledgement.
								 * In order to implement this, it would be necessary to (at least):
								 * 
								 * 		1.	Register the "channel" object with the helper singleton, 
								 * 			messageConsumerHelper, so that it can execute "channel.basicAck(...)".
								 * 
								 * 		2.	Register the "delivery tag" with the helper singleton or,
								 * 			instead of this we can send the "delivery tag" along with
								 * 			rest of the event payload to the "Observer" method from this 
								 * 			thread and then later fire this "delivery tag" back to 
								 * 			messageConsumerHelper via the acknowledgement event
								 * 			sent from the "Observer" method that processes the RabbitMQ
								 * 			message payload.  Is this possible?
								 * 		
								 * This will mean that we cannot just forward the raw message bytes to the
								 * "Observer" method from this thread, because we need to also send the
								 * "delivery tag"
								 * 
								 * This will be more complicate if we use several RabbitMQ consumer threads,
								 * because it will also be necessay to ensure that the acknowledgement event 
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

								logger.debug("Requesting messageConsumerHelper bean to fire an \"image\" CDI event...");
								messageConsumerHelper.fireImageEvent(imageBytes);
								logger.debug("Returned from request to fire the \"image\" CDI event");

								channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);

							} else {
								/*
								 * This just means that there were no messages to consume after
								 * waiting the timeout period. This in perfectly normal. The 
								 * timeout was implemented so that this thread can check whether
								 * there has been a request made for it to die or whatever. 
								 */
								logger.trace("consumer.nextDelivery() timed out after "
										+ RABBITMQ_CONSUMER_TIMEOUT_MS
										+ " ms.");
							}

						} catch (ShutdownSignalException e) {
							logger.info("ShutdownSignalException received. The RabbitMQ connection is closing.", e);
							break;
						} catch (ConsumerCancelledException e) {
							logger.info("ConsumerCancelledException received. The RabbitMQ connection is closing.", e);
							break;
						} catch (InterruptedException e) {
							/*
							 * RabbitMQConsumerController is probably requesting that this
							 * thread be shut down. We check for this below.
							 */
							logger.info("InterruptedException received.");
						} catch (Throwable e) {
							logger.error("Unexpected exception caught.", e);
						}

						logger.trace("Checking if shutdown was requested...");
						if (rabbitMQConsumerController.getState() == RabbitMQConsumerControllerStates.STOPPED) {
							logger.info("Shutdown request detected. This thread will be allowed to die.");
							break;
						}
					}

				} catch (IOException e) {
					logger.error(
							"Exception thrown setting up consumer object for RabbitMQ channel. This thread will die.",
							e);
				}

			} catch (IOException e) {
				logger.error(
						"Exception thrown setting up RabbitMQ channel for conusuming messages. This thread will die.",
						e);
			} finally {
				if (channel != null) {
					logger.info("Closing RabbitMQ channel...");
					channel.close();
				}
			}

		} catch (IOException e) {
			//TODO Write out more details about the connection attempt
			// What I write out and how will depend on how I specify the host details.
			// For example, I can specify the host, port, username, etc., separately, or
			// I can use a single AMQP URI.
			logger.error("Exception thrown attempting to open connection to RabbitMQ broker. This thread will die.", e);
		} finally {
			if (connection != null) {
				try {
					/*
					 * If this thread is allowed to die without closing the 
					 * connection, there will be one or more unacknowledged 
					 * messages on the RabbitMQ broker, corresponding to the 
					 * value passed to "channel.basicQos(...)" above. The broker
					 * will return them to the queue after it detects the 
					 * connection is lost, but since the application container 
					 * may keep this thread around for some time so that it can
					 * be reused elsewhere, we need to explicitly clean up here.
					 * Since, the broker is supposed to close any open channels
					 * when the connection is closed it is not strictly 
					 * necessary to close the channel above, but good 
					 * programming does demand this.
					 */
					logger.info("Closing RabbitMQ connection...");
					connection.close();
				} catch (IOException e) {
					logger.error("Exception caught closing RabbitMQ connection", e);
				}
			}
		}

		logger.debug("Thread exiting");
		this.setState(RabbitMQConsumerStates.STOPPED);

	}

}

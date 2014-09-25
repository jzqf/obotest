package com.qfree.obotest.rabbitmq.produce;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qfree.obotest.rabbitmq.produce.RabbitMQProducerController.RabbitMQProducerControllerStates;
import com.qfree.obotest.rabbitmq.produce.RabbitMQProducerController.RabbitMQProducerStates;
//TODO This must be eliminated or updated to something related to producing:
import com.rabbitmq.client.ConsumerCancelledException;
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
public class RabbitMQProducer implements Runnable {

	private volatile RabbitMQProducerStates state = RabbitMQProducerStates.STOPPED;

	private static final Logger logger = LoggerFactory.getLogger(RabbitMQProducer.class);

	/*
	 * TODO We can eliminate rabbitMQProducerController if we make RabbitMQProducerController.state public static volatile!!!!!!!!!!
	 * TODO Then we can also eliminate the method RabbitMQProducerController.getState() !!!!!!!!!!!!
	 * TODO If assigning a value to a volatile attribute is thread-safe, then I can also get rid of getState()!!
	 */
	//	@EJB - cannot use because this class is not instantiated by the container
	RabbitMQProducerController rabbitMQProducerController = null;

	RabbitMQProducerHelper messageProducerHelper = null;

	public RabbitMQProducerStates getState() {
		return state;
	}

	public void setState(RabbitMQProducerStates state) {
		this.state = state;
	}

	/*
	 * This constructor is necessary, since this is a stateless session bean,
	 * even though all instances of this bean used by the application are 
	 * created with the "new" operator and use a constructor with arguments.
	 */
	public RabbitMQProducer() {
	}

	public RabbitMQProducer(
			RabbitMQProducerController rabbitMQProducerController,
			RabbitMQProducerHelper messageProducerHelper) {
		super();
		this.rabbitMQProducerController = rabbitMQProducerController;
		this.messageProducerHelper = messageProducerHelper;
	}

	@Override
	public void run() {

		logger.debug("Starting RabbitMQ message producer...");
		this.setState(RabbitMQProducerStates.RUNNING);

		try {
			messageProducerHelper.openConnection();
			try {
				messageProducerHelper.openChannel();
				try {

					//TODO This must be eliminated or updated to something related to producing:
					//					messageProducerHelper.configureProducer(rabbitMQProducerController.getMessageBlockingQueue());

					while (true) {
						try {
							messageProducerHelper.handlePublish();
						} catch (InterruptedException e) {
							/*
							 * RabbitMQProducerController is probably requesting that this
							 * thread be shut down. This is checked below.
							 */
							logger.info("InterruptedException received.");
						} catch (ShutdownSignalException e) {
							logger.info("ShutdownSignalException received. The RabbitMQ connection will close.", e);
							break;
							//TODO Either eliminate this catch block or rename "consumer" to something else here in this message
						} catch (ConsumerCancelledException e) {
							logger.info("ConsumerCancelledException received. The RabbitMQ connection will close.", e);
							break;
						} catch (IOException e) {
							logger.error("IOException received. The RabbitMQ connection will close.", e);
							break;
						} catch (Throwable e) {
							// We log the exception, but do not terminate this thread.
							logger.error("Unexpected exception caught.", e);
						}

						logger.trace("Checking if shutdown was requested...");
						if (rabbitMQProducerController.getState() == RabbitMQProducerControllerStates.STOPPED) {
							logger.info("Shutdown request detected. This thread will terminate.");
							break;
						}
					}

				} catch (Exception e) {
					//TODO Either eliminate this try block or rename "consumer" to something else here in this message
					logger.error(
							"Exception thrown setting up consumer object for RabbitMQ channel. This thread will terminate.",
							e);
				}

			} catch (IOException e) {
				logger.error(
						"Exception thrown setting up RabbitMQ channel for conusuming messages. This thread will terminate.",
						e);
			} finally {
				logger.info("Closing RabbitMQ channel...");
				messageProducerHelper.closeChannel();
			}

		} catch (IOException e) {
			//TODO Write out more details about the connection attempt
			// What I write out and how will depend on how I specify the host details.
			// For example, I can specify the host, port, username, etc., separately, or
			// I can use a single AMQP URI.
			logger.error(
					"Exception thrown attempting to open connection to RabbitMQ broker. This thread will terminate.", e);
		} finally {
			/*
			 * If this thread is allowed to terminate without closing the 
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
			try {
				messageProducerHelper.closeConnection();
			} catch (IOException e) {
				logger.error("Exception caught closing RabbitMQ connection", e);
			}
		}

		logger.debug("Thread exiting");
		this.setState(RabbitMQProducerStates.STOPPED);

	}

}

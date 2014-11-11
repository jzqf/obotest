package com.qfree.obotest.passagetest1;

import java.io.IOException;

import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	private Connection connection = null;
	private Channel channel = null;

	/*
	 * This field is used to enable the name of the subclass to be logged if 
	 * this class has been used to create a subclass. This field is set in the
	 * constructor for this class, but it will be set to the name of the 
	 * _subclass_ if an instance of a subclass is constructed.
	 */
	private String subClassName = null;

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
	public Channel getChannel() {
		return channel;
	}

	@Override
	public void handlePublish(byte[] passageBytes) throws IOException {
			/*
			 * TODO Should I also pass mandatory=true here? This is to ensure that 
			 *      the message gets into at least one queue. If the server cannot 
			 *      do this, it will send the message's deliveryTag (sequence number) 
			 *      back via the installed ReturnListener.  I need to read the 
			 *      section "When will messages be confirmed?" of the document:
			 *      https://www.rabbitmq.com/confirms.html to really understand what
			 *      this means and to ensure that I am handling this situation 
			 *      correctly (if/after I start using the "mandatory" flag). Of
			 *      course, I will also have to implement a ReturnListener as well.
			 */
			channel.basicPublish("", PASSAGE_QUEUE_NAME, MessageProperties.PERSISTENT_BASIC, passageBytes);
	}

	@PreDestroy
	public void terminate() {
		logger.info("[{}]: This bean will now be destroyed by the container...", subClassName);
	}

}

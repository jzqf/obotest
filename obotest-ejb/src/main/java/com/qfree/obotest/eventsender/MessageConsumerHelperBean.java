package com.qfree.obotest.eventsender;

import javax.ejb.Asynchronous;
import javax.ejb.Lock;
import javax.ejb.LockType;
import javax.enterprise.event.Event;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qfree.obotest.event.ImageEvent;

/*
 * This class is used as a base class for both MessageConsumerHelperBean1 and 
 * MessageConsumerHelperBean2. By using a base class, it is easy to ensure that the
 * both MessageConsumerHelperBean1 and MessageConsumerHelperBean2 have identical methods. Separate
 * classes, i.e., MessageConsumerHelperBean1 and MessageConsumerHelperBean2, are needed because
 * each is used to create a singleton bean, something that is not possible to do
 * with a single class.
 * 
 * One slight drawback of using a common base class is that all logging is
 * associated with this base class, not the particular EJB singleton class that
 * extends it. One way to get around this is to include:
 * 
 *     this.getClass().getName()
 * 
 * in the log message.
 */
public abstract class MessageConsumerHelperBean implements MessageConsumerHelper {

	private static final Logger logger = LoggerFactory.getLogger(MessageConsumerHelperBean.class);

	/*
	 * This field is used to enable the name of the subclass to be logged if 
	 * this class has been used to create a subclass. It is the duty of the 
	 * subclass to set this field, probably in its constructor.
	 */
	String subClassName = null;

    @Inject
	@Image
	Event<ImageEvent> imageEvent;

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
	 */
	@Asynchronous
	@Lock(LockType.WRITE)
	@Override
	public void fireImageEvent(byte[] imageBytes) {
		logger.debug("[{}]: Creating event payload for image [{} bytes]", subClassName, imageBytes.length);
		ImageEvent imagePayload = new ImageEvent();
		imagePayload.setImageBytes(imageBytes);
		logger.debug("[{}]: Firing event for {}", subClassName, imagePayload);
		imageEvent.fire(imagePayload);
		logger.debug("[{}]: Returned from firing event", subClassName);
    }

}
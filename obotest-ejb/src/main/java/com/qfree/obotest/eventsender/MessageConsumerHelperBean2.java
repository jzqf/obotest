package com.qfree.obotest.eventsender;

import javax.ejb.ConcurrencyManagement;
import javax.ejb.ConcurrencyManagementType;
import javax.ejb.LocalBean;
import javax.ejb.Singleton;
import javax.enterprise.inject.Alternative;

/*
 * This class is essentially identical to MessageConsumerHelperBean1.
 * 
 * Two identical classes are used to create two singleton session beans that 
 * can divide the workload between two separate MessageMQ consumer threads.
 * 
 * Each of these two classes are differentiated by their qualifiers:
 * 
 *     @HelperBean1 or @HelperBean2
 */
@ConcurrencyManagement(ConcurrencyManagementType.CONTAINER)
@Singleton
@LocalBean
@Alternative
@HelperBean2
public class MessageConsumerHelperBean2 extends MessageConsumerHelperBean {

	public MessageConsumerHelperBean2() {
		super();
		/*
		 * This field is used to enable the name of this subclass to be logged 
		 * from its superclass.
		 */
		this.subClassName = this.getClass().getSimpleName();
	}

	//public class MessageConsumerHelperBean2 implements MessageConsumerHelper {
	//
	//	private static final Logger logger = LoggerFactory.getLogger(MessageConsumerHelperBean2.class);
	//
	//	@Inject
	//	@Image
	//	Event<ImageEvent> imageEvent;
	//
	//	/*
	//	 * By making this method (as well as the method that receives the fired 
	//	 * event) asynchronous, the event mechanism becomes a sort of 
	//	 * "fire-and-forget" call. If we do annotate this method here with 
	//	 * @Asynchronous, but not the receiving (@Observes) method, then the Java EE
	//	 * container will use a pool of threads to fire many events (one in each 
	//	 * thread), but each thread will wait for the event to be processed before 
	//	 * proceeding, so it isn't fully asynchronous and nothing is gained. It is
	//	 * important, therefore, to just follow the rule that @Asynchronous be used 
	//	 * on both the method that fires the event as well as the method that 
	//	 * receives (@Observes) the event. Testing seems to show that it is not 
	//	 * actually necessary to annotate with @Asynchronous this method here that 
	//	 * does the firing, but for the time being I will do this until a reason
	//	 * becomes apparent for not doing it.
	//	 * 
	//	 * TODO Investigate if/how we can get information on how the event was processed by the receiver.
	//	 * 
	//	 */
	//	@Asynchronous
	//	@Lock(LockType.WRITE)
	//	@Override
	//	public void fireImageEvent(byte[] imageBytes) {
	//		logger.debug("Creating event payload for image [{} bytes]", imageBytes.length);
	//		ImageEvent imagePayload = new ImageEvent();
	//		imagePayload.setImageBytes(imageBytes);
	//		logger.debug("Firing event for {}", imagePayload);
	//		imageEvent.fire(imagePayload);
	//		logger.debug("Returned from firing event");
	//	}

}

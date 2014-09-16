package com.qfree.obotest.eventsender;

import javax.ejb.ConcurrencyManagement;
import javax.ejb.ConcurrencyManagementType;
import javax.ejb.LocalBean;
import javax.ejb.Singleton;
import javax.enterprise.inject.Alternative;

/*
 * This class is essentially identical to MessageConsumerHelperImageTestBean2.
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
@HelperBean1
public class MessageConsumerHelperImageTestBean1 extends MessageConsumerHelperImageTest {

	public MessageConsumerHelperImageTestBean1() {
		super();
		/*
		 * This field is used to enable the name of this subclass to be logged 
		 * from its superclass.
		 */
		this.subClassName = this.getClass().getSimpleName();
	}

}

package com.qfree.obotest.rabbitmq.consume.passagetest1;

import javax.ejb.ConcurrencyManagement;
import javax.ejb.ConcurrencyManagementType;
import javax.ejb.LocalBean;
import javax.ejb.Singleton;
import javax.enterprise.inject.Alternative;

import com.qfree.obotest.rabbitmq.HelperBean2;

/*
 * This class is essentially identical to 
 * RabbitMQConsumerHelperPassageTest1Bean1.
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
public class RabbitMQConsumerHelperPassageTest1Bean2 extends RabbitMQConsumerHelperPassageTest1 {

}

package com.qfree.obotest.container;

import javax.transaction.TransactionManager;

/*
 * This class defines a "JTA transaction controller". It is referenced in 
 * persistence.xml. This is necessary because the code in this project runs 
 * *outside* of a Java EE container (e.g., GlassFish). Normally, the container
 * will set this up for you automatically.
 * 
 * *****************************************************************************
 * Note:
 * 
 * In order to get access to the class:
 * 
 *     org.eclipse.persistence.transaction.JTATransactionController
 *     
 * it was necessary to add the "EJB Module v3.2" facet to this project with:
 * 
 *      Properties > Project Facets
 * 
 * I also specified "GlassFish 4.0" on the "Runtimes" tab, but I don't know if
 * this was necessary. After these changes, I could run unit tests with:
 * 
 *     Run As > JUnit Test
 *     
 * But this was just for running within Eclipse (Run As > JUnit Test). On the
 * other hand, running:
 * 
 *  	mvn install
 *  
 * on this project triggered errors, in particular:
 * 
 *     "package org.eclipse.persistence.transaction does not exist"
 *     
 * This is quite reasonable, since Eclipse facets do not provide any 
 * dependencies to Maven.  The solution was to specify the appropriate
 * Maven dependency in pom.xml.  I eventually found that the following worked:
 * 
 *  <dependency>
 *  	<groupId>org.eclipse.persistence</groupId>
 *  	<artifactId>eclipselink</artifactId>
 *  	<version>2.5.2</version>
 *  </dependency>
 *  
 *  With this dependency it is possible to run the unit tests in this project
 *  (Maven module) *outside* of an Eclipse environment, in addition to outside 
 *  of a Java EE container, e.g., GlassFish.
 */
public class JTATransactionController extends org.eclipse.persistence.transaction.JTATransactionController {

	public static final String JNDI_TRANSACTION_MANAGER_NAME = "java:comp/TransactionManager";

	public JTATransactionController() {
		super();
	}

	@Override
	protected TransactionManager acquireTransactionManager() throws Exception {
		return (TransactionManager) jndiLookup(JNDI_TRANSACTION_MANAGER_NAME);
	}
}

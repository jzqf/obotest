package com.qfree.obotest.eventlistener;

import java.io.Serializable;

import javax.ejb.Asynchronous;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.enterprise.event.Observes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qfree.obotest.event.PassageTest1Event;

@Stateless
@LocalBean
public class PassageTest1Handler implements Serializable {

	private static final long serialVersionUID = 1L;

	private static final Logger logger = LoggerFactory.getLogger(PassageTest1Handler.class);

	public PassageTest1Handler() {
		logger.info("PassageTest1Handler instance created");
	}

	@Asynchronous
	public void processPassage(@Observes @PassageQualifier PassageTest1Event event) {
		logger.debug("Start processing passage: {}...", event.toString());
		//		try {
		//			Thread.sleep(1000);		// simulate doing some work
		//		} catch (InterruptedException e) {
		//		}
		logger.debug("Finished processing passage: {}...", event.toString());
	}
}

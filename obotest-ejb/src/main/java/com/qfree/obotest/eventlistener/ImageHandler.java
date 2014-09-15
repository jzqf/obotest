package com.qfree.obotest.eventlistener;

import java.io.Serializable;

import javax.ejb.Asynchronous;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.enterprise.event.Observes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qfree.obotest.event.ImageEvent;
import com.qfree.obotest.eventsender.Image;

@Stateless
@LocalBean
public class ImageHandler implements Serializable {

	private static final long serialVersionUID = 1L;

	private static final Logger logger = LoggerFactory.getLogger(ImageHandler.class);

	public ImageHandler() {
		logger.info("ImageHandler instance created");
	}

	@Asynchronous
	public void processImage(@Observes @Image ImageEvent event) {
		logger.debug("Start processing image: {}...", event.toString());
		//		try {
		//			Thread.sleep(1000);		// simulate doing some work
		//		} catch (InterruptedException e) {
		//		}
		logger.debug("Finished processing image: {}...", event.toString());
	}
}

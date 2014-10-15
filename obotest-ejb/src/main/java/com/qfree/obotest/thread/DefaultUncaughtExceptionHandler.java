package com.qfree.obotest.thread;

import java.lang.Thread.UncaughtExceptionHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultUncaughtExceptionHandler implements UncaughtExceptionHandler {

	private static final Logger logger = LoggerFactory.getLogger(DefaultUncaughtExceptionHandler.class);

	@Override
	public void uncaughtException(Thread t, Throwable e) {
		logger.error("An uncaught exception was thrown for thread {}", t);
		logger.error("The uncaught exception was:", e);
	}

}

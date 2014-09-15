package com.qfree.obotest;

import javax.ejb.LocalBean;
import javax.ejb.Stateless;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Stateless
@LocalBean
public class UnusedStatelessSessionBean {

	private static final Logger logger = LoggerFactory.getLogger(UnusedStatelessSessionBean.class);

    public void businessMethod() {
		logger.debug("Entering businessMethod()...");
    }

}

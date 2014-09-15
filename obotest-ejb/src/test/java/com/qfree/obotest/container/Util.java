package com.qfree.obotest.container;

import javax.persistence.EntityManager;
import javax.persistence.Persistence;

public class Util {

	public static EntityManager getEntityManager() {
		return Persistence.createEntityManagerFactory("obotest-ejb_junit")
				.createEntityManager();
	}
}

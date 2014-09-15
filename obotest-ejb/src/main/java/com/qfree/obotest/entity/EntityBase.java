
package com.qfree.obotest.entity;

public abstract class EntityBase {

	public static boolean isId(Integer id) {
		return (id != null && id > 0);
	}

	public boolean hasId() {
		return isId(getId());
	}

	public abstract Integer getId();

}

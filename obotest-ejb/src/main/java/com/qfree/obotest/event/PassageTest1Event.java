package com.qfree.obotest.event;

import java.io.Serializable;

public class PassageTest1Event implements Serializable {

	private static final long serialVersionUID = 1L;

	private String image_name;

	private byte[] imageBytes;

	public String getImage_name() {
		return image_name;
	}

	public void setImage_name(String image_name) {
		this.image_name = image_name;
	}

	public byte[] getImageBytes() {
		return imageBytes;
	}

	public void setImageBytes(byte[] imageBytes) {
		this.imageBytes = imageBytes;
	}

	@Override
	public String toString() {
		return "PassageTest1Event [name:" + this.image_name + ", bytes:" + imageBytes.length + "]";
	}

}

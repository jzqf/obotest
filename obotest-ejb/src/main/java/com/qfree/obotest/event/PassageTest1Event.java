package com.qfree.obotest.event;

import java.io.Serializable;

public class PassageTest1Event implements Serializable {

	private static final long serialVersionUID = 1L;

	private String imageName;

	private byte[] imageBytes;

	public String getImageName() {
		return imageName;
	}

	public void setImageName(String imageName) {
		this.imageName = imageName;
	}

	public byte[] getImageBytes() {
		return imageBytes;
	}

	public void setImageBytes(byte[] imageBytes) {
		this.imageBytes = imageBytes;
	}

	@Override
	public String toString() {
		return "PassageTest1Event [name:" + this.imageName + ", bytes:" + imageBytes.length + "]";
	}

}

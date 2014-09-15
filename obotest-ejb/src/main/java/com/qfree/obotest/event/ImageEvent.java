package com.qfree.obotest.event;

import java.io.Serializable;

public class ImageEvent implements Serializable {

	private static final long serialVersionUID = 1L;

	private byte[] imageBytes;

	public byte[] getImageBytes() {
		return imageBytes;
	}

	public void setImageBytes(byte[] imageBytes) {
		this.imageBytes = imageBytes;
	}

	@Override
	public String toString() {
		return "ImageEvent [" + imageBytes.length + " bytes]";
	}

}

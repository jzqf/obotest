package com.qfree.obotest.rabbitmq;

public class RabbitMQMsgEnvelope {

	private RabbitMQMsgAck rabbitMQMsgAck;
	private byte[] message;

	public RabbitMQMsgEnvelope(RabbitMQMsgAck rabbitMQMsgAck, byte[] message) {
		super();
		this.rabbitMQMsgAck = rabbitMQMsgAck;
		this.message = message;
	}

	public RabbitMQMsgAck getRabbitMQMsgAck() {
		return rabbitMQMsgAck;
	}

	//	public void setRabbitMQMsgAck(RabbitMQMsgAck rabbitMQMsgAck) {
	//		this.rabbitMQMsgAck = rabbitMQMsgAck;
	//	}

	public byte[] getMessage() {
		return message;
	}

	public void setMessage(byte[] message) {
		this.message = message;
	}

}

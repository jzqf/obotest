package com.qfree.obotest.rabbitmq;

import java.util.UUID;

public class RabbitMQMsgAck {

	private UUID consumerThreadUUID;
	private long deliveryTag;
	/**
	 * If false, the message will be acked.
	 * If true, the message will be nacked.
	 */
	private boolean rejected = false;
	/**
	 * If true, message will be requeued.
	 * 
	 * If false, message will be dead-lettered if a dead-letter exchanged has
	 * been configured; otherwise it will be discarded.
	 * 
	 * Used only if rejected=true.
	 */
	private boolean requeueRejectedMsg = true;

	public RabbitMQMsgAck(UUID consumerThreadUUID, long deliveryTag) {
		super();
		this.consumerThreadUUID = consumerThreadUUID;
		this.deliveryTag = deliveryTag;
	}

	public UUID getConsumerThreadUUID() {
		return consumerThreadUUID;
	}

	public void setConsumerThreadUUID(UUID consumerThreadUUID) {
		this.consumerThreadUUID = consumerThreadUUID;
	}

	public long getDeliveryTag() {
		return deliveryTag;
	}

	public void setDeliveryTag(long deliveryTag) {
		this.deliveryTag = deliveryTag;
	}

	public boolean isRejected() {
		return rejected;
	}

	public void setRejected(boolean rejected) {
		this.rejected = rejected;
	}

	public boolean isRequeueRejectedMsg() {
		return requeueRejectedMsg;
	}

	public void setRequeueRejectedMsg(boolean requeueRejectedMsg) {
		this.requeueRejectedMsg = requeueRejectedMsg;
	}

}

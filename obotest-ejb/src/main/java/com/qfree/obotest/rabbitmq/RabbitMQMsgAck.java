package com.qfree.obotest.rabbitmq;

import java.util.concurrent.BlockingQueue;

public class RabbitMQMsgAck {

	/*
	 * This is the queue into which a RabbitMQMsgAck object should be inserted
	 * if we are in "acknowledge after send" mode. There is one such queue for
	 * each RabbitMQ consumer thread. This attribute is necessary so that the
	 * the message is acked/nacked on the RabbitMQ channel (consumer thread) on
	 * which the message was originally consumed.
	 */
	private BlockingQueue<RabbitMQMsgAck> acknowledgementQueue;
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

	public RabbitMQMsgAck(BlockingQueue<RabbitMQMsgAck> acknowledgementQueue, long deliveryTag) {
		super();
		this.acknowledgementQueue = acknowledgementQueue;
		this.deliveryTag = deliveryTag;
	}

	public BlockingQueue<RabbitMQMsgAck> getAcknowledgementQueue() {
		return acknowledgementQueue;
	}

	public void setAcknowledgementQueue(BlockingQueue<RabbitMQMsgAck> acknowledgementQueue) {
		this.acknowledgementQueue = acknowledgementQueue;
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

	@Override
	public String toString() {
		return "RabbitMQMsgAck [deliveryTag=" + deliveryTag
				+ ", rejected=" + rejected + ", requeueRejectedMsg=" + requeueRejectedMsg + "]";
	}

}

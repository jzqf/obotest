package com.qfree.obotest.rabbitmq;

import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qfree.obotest.rabbitmq.consume.RabbitMQConsumerController;

public class RabbitMQMsgAck {

	private static final Logger logger = LoggerFactory.getLogger(RabbitMQConsumerController.class);

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

	public void queueAck() {
		this.rejected = false;
		queue();
	}

	public void queueNack(boolean requeueRejectedMsg) {
		this.rejected = true;
		this.requeueRejectedMsg = requeueRejectedMsg;
		queue();
	}

	private void queue() {
		/*
		 * Log a warning if the acknowledgement queue is over 90% full.
		 */
		if (acknowledgementQueue.size() > 0.9 * RabbitMQConsumerController.ACKNOWLEDGEMENT_QUEUE_LENGTH) {
			logger.warn("Acknowledgement queue is over 90% full. Current size = {}."
					+ " It may be necessary to increase the maximum capacity.",
					acknowledgementQueue.size());
		}

		/*
		 * Place the RabbitMQMsgAck object in the acknowledgement queue.
		 */
		if (acknowledgementQueue.offer(this)) {
			logger.debug("RabbitMQMsgAck object offered to acknowledgment queue: {}. Queue size = {}",
					this, acknowledgementQueue.size());
		} else {
			logger.warn("Acknowledgement queue is full."
					+ " Message will be requeued when consumer threads are restarted.");
		}
	}

	@Override
	public String toString() {
		return "RabbitMQMsgAck [deliveryTag=" + deliveryTag
				+ ", rejected=" + rejected + ", requeueRejectedMsg=" + requeueRejectedMsg + "]";
	}

}

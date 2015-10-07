package com.hamersaw.basic_pastry.message;

public class RequestRandomNodeMsg extends Message {
	@Override
	public int getMsgType() {
		return Message.REQUEST_RANDOM_NODE_MSG;
	}
}

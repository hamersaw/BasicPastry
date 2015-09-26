package com.hamersaw.basic_pastry.message;

public abstract class Message {
	public static final int ERROR_MSG = 0,
		SUCCESS_MSG = 1,
		REGISTER_NODE_MSG = 2,
		REGISTER_NODE_REPLY_MSG = 3,
		REMOVE_NODE_MSG = 4;
	
	public abstract int getMsgType();
}

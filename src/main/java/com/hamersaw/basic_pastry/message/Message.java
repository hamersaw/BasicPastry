package com.hamersaw.basic_pastry.message;

import java.io.Serializable;

public abstract class Message implements Serializable {
	public static final int ERROR_MSG = 0,
		SUCCESS_MSG = 1,
		REGISTER_NODE_MSG = 2,
		REGISTER_NODE_REPLY_MSG = 3,
		REMOVE_NODE_MSG = 4,
		NODE_JOIN_MSG = 5,
		ROUTING_INFO_MSG = 6;
	
	public abstract int getMsgType();
}

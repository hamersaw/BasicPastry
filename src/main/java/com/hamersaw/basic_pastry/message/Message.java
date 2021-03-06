package com.hamersaw.basic_pastry.message;

import java.io.Serializable;

public abstract class Message implements Serializable {
	public static final int ERROR_MSG = 0,
		SUCCESS_MSG = 1,
		REGISTER_NODE_MSG = 2,
		NODE_INFO_MSG = 3,
		NODE_JOIN_MSG = 4,
		ROUTING_INFO_MSG = 5,
		LOOKUP_NODE_MSG = 6,
		REQUEST_RANDOM_NODE_MSG = 7,
		WRITE_DATA_MSG = 8,
		READ_DATA_MSG = 9;
	
	public abstract int getMsgType();
}

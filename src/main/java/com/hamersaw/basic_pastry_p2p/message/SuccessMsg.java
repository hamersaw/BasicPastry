package com.hamersaw.basic_pastry_p2p.message;

public class SuccessMsg extends Message {
	@Override
	public int getMsgType() {
		return SUCCESS_MSG;
	}
}

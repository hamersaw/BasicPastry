package com.hamersaw.basic_pastry.message;

public class SuccessMsg extends Message {
	@Override
	public int getMsgType() {
		return SUCCESS_MSG;
	}
}

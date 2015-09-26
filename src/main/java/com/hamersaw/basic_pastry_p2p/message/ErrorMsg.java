package com.hamersaw.basic_pastry_p2p.message;

public class ErrorMsg extends Message {
	private String msg;

	public ErrorMsg(String msg) {
		this.msg = msg;
	}

	public String getMsg() {
		return msg;
	}

	@Override
	public int getMsgType() {
		return ERROR_MSG;
	}
}

package com.hamersaw.basic_pastry_p2p.message;

import java.net.InetAddress;

public class RegisterNodeReplyMsg extends Message {
	private byte[] id;
	private InetAddress inetAddress;
	private int port;

	public RegisterNodeReplyMsg(byte[] id, InetAddress inetAddress, int port) {
		this.id = id;
		this.inetAddress = inetAddress;
		this.port = port;
	}

	public byte[] getID() {
		return id;
	}

	public InetAddress getInetAddress() {
		return inetAddress;
	}

	public int getPort() {
		return port;
	}

	@Override
	public int getMsgType() {
		return REGISTER_NODE_REPLY_MSG;
	}
}

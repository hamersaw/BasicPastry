package com.hamersaw.basic_pastry.message;

import java.net.InetAddress;

public class RemoveNodeMsg extends Message {
	private byte[] id;
	private InetAddress inetAddress;
	private int port;

	public RemoveNodeMsg(byte[] id, InetAddress inetAddress, int port) {
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
		return REMOVE_NODE_MSG;
	}
}

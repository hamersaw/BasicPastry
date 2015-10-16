package com.hamersaw.basic_pastry.message;

import java.net.InetAddress;

public class RegisterNodeMsg extends Message {
	private byte[] id;
	private String nodeName;
	private InetAddress inetAddress;
	private int port;

	public RegisterNodeMsg(String nodeName, byte[] id, InetAddress inetAddress, int port) {
		this.nodeName = nodeName;
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

	public String getNodeName() {
		return nodeName;
	}

	@Override
	public int getMsgType() {
		return REGISTER_NODE_MSG;
	}
}

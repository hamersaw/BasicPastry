package com.hamersaw.basic_pastry.message;

import java.net.InetAddress;

public class NodeJoinMsg extends Message {
	private byte[] id;
	private int prefixLength;
	private InetAddress inetAddress;
	private int port;

	public NodeJoinMsg(byte[] id, int prefixLength,  InetAddress inetAddress, int port) {
		this.id = id;
		this.prefixLength = prefixLength;
		this.inetAddress = inetAddress;
		this.port = port;
	}

	public byte[] getID() {
		return id;
	}

	public void setLongestPrefixMatch(int prefixLength) {
		this.prefixLength = prefixLength;
	}

	public int getPrefixLength() {
		return prefixLength;
	}

	public void setInetAddress(InetAddress inetAddress) {
		this.inetAddress = inetAddress;
	}

	public InetAddress getInetAddress() {
		return inetAddress;
	}

	public int getPort() {
		return port;
	}

	@Override
	public int getMsgType() {
		return NODE_JOIN_MSG;
	}
}

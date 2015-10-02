package com.hamersaw.basic_pastry.message;

import java.net.InetAddress;

public class NodeJoinMsg extends Message {
	private byte[] id;
	private short longestPrefixMatch;
	private InetAddress inetAddress;
	private int port;

	public NodeJoinMsg(byte[] id, short longestPrefixMatch,  InetAddress inetAddress, int port) {
		this.id = id;
		this.longestPrefixMatch = longestPrefixMatch;
		this.inetAddress = inetAddress;
		this.port = port;
	}

	public byte[] getID() {
		return id;
	}

	public void setLongestPrefixMatch(short longestPrefixMatch) {
		this.longestPrefixMatch = longestPrefixMatch;
	}

	public short getLongestPrefixMatch() {
		return longestPrefixMatch;
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

package com.hamersaw.basic_pastry.message;

import java.net.InetAddress;

public class NodeJoinMsg extends Message {
	private byte[] id;
	private int longestPrefixMatch;
	private InetAddress inetAddress;
	private int port;

	public NodeJoinMsg(byte[] id, int longestPrefixMatch,  InetAddress inetAddress, int port) {
		this.id = id;
		this.longestPrefixMatch = longestPrefixMatch;
		this.inetAddress = inetAddress;
		this.port = port;
	}

	public byte[] getID() {
		return id;
	}

	public void setLongestPrefixMatch(int longestPrefixMatch) {
		this.longestPrefixMatch = longestPrefixMatch;
	}

	public int getLongestPrefixMatch() {
		return longestPrefixMatch;
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

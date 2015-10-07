package com.hamersaw.basic_pastry.message;

import com.hamersaw.basic_pastry.NodeAddress;

public class LookupNodeMsg extends Message {
	private byte[] id;
	private NodeAddress nodeAddress;
	private String filename;
	private int longestPrefixMatch;

	public LookupNodeMsg(byte[] id, String filename, NodeAddress nodeAddress, int longestPrefixMatch) {
		this.id = id;
		this.filename = filename;
		this.nodeAddress = nodeAddress;
		this.longestPrefixMatch = longestPrefixMatch;
	}

	public byte[] getID() {
		return id;
	}

	public String getFilename() {
		return filename;
	}

	public NodeAddress getNodeAddress() {
		return nodeAddress;
	}

	public void setLongestPrefixMatch(int longestPrefixMatch) {
		this.longestPrefixMatch = longestPrefixMatch;
	}

	public int getLongestPrefixMatch() {
		return longestPrefixMatch;
	}

	@Override
	public int getMsgType() {
		return LOOKUP_NODE_MSG;
	}
}

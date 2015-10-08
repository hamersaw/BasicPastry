package com.hamersaw.basic_pastry.message;

import com.hamersaw.basic_pastry.NodeAddress;

public class LookupNodeMsg extends Message {
	private byte[] id;
	private NodeAddress nodeAddress;
	private String filename;
	private int prefixLength;

	public LookupNodeMsg(byte[] id, String filename, NodeAddress nodeAddress, int prefixLength) {
		this.id = id;
		this.filename = filename;
		this.nodeAddress = nodeAddress;
		this.prefixLength = prefixLength;
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

	public void setLongestPrefixMatch(int prefixLength) {
		this.prefixLength = prefixLength;
	}

	public int getPrefixLength() {
		return prefixLength;
	}

	@Override
	public int getMsgType() {
		return LOOKUP_NODE_MSG;
	}
}

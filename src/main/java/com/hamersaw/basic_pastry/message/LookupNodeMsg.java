package com.hamersaw.basic_pastry.message;

import com.hamersaw.basic_pastry.NodeAddress;

public class LookupNodeMsg extends Message {
	private byte[] id;
	private NodeAddress nodeAddress;
	private int prefixLength;

	public LookupNodeMsg(byte[] id, NodeAddress nodeAddress, int prefixLength) {
		this.id = id;
		this.nodeAddress = nodeAddress;
		this.prefixLength = prefixLength;
	}

	public byte[] getID() {
		return id;
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

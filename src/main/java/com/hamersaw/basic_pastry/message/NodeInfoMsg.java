package com.hamersaw.basic_pastry.message;

import com.hamersaw.basic_pastry.NodeAddress;

public class NodeInfoMsg extends Message {
	private byte[] id;
	private NodeAddress nodeAddress;

	public NodeInfoMsg(byte[] id, NodeAddress nodeAddress) {
		this.id = id;
		this.nodeAddress = nodeAddress;
	}

	public byte[] getID() {
		return id;
	}

	public NodeAddress getNodeAddress() {
		return nodeAddress;
	}

	@Override
	public int getMsgType() {
		return NODE_INFO_MSG;
	}
}

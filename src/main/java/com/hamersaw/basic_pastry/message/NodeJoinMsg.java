package com.hamersaw.basic_pastry.message;

import java.util.LinkedList;
import java.util.List;

import java.net.InetAddress;

import com.hamersaw.basic_pastry.HexConverter;
import com.hamersaw.basic_pastry.NodeAddress;

public class NodeJoinMsg extends Message {
	private byte[] id;
	private int prefixLength;
	private NodeAddress nodeAddress;
	private List<NodeAddress> hops;

	public NodeJoinMsg(byte[] id, int prefixLength, NodeAddress nodeAddress) {
		this.id = id;
		this.prefixLength = prefixLength;
		this.nodeAddress = nodeAddress;
		hops = new LinkedList();
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

	public NodeAddress getNodeAddress() {
		return nodeAddress;
	}

	public void addHop(NodeAddress nodeAddress) {
		hops.add(nodeAddress);
	}

	@Override
	public int getMsgType() {
		return NODE_JOIN_MSG;
	}

	@Override
	public String toString() {
		StringBuilder strBldr = new StringBuilder();
		strBldr.append("ID:" + HexConverter.convertBytesToHex(id) + " HOPS:" + hops.size() + " ");
		strBldr.append("PATH:" + nodeAddress.toString());
		for(NodeAddress nodeAddress : hops) {
			strBldr.append(" -> " + nodeAddress.toString());
		}

		return strBldr.toString();
	}
}

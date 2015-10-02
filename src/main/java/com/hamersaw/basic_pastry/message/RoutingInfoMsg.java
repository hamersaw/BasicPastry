package com.hamersaw.basic_pastry.message;

import java.util.Map;

import com.hamersaw.basic_pastry.NodeAddress;

public class RoutingInfoMsg extends Message {
	private Map<byte[],NodeAddress> leafSet;
	private Map<String,NodeAddress> routingTable;
	private boolean broadcastMsg;

	public RoutingInfoMsg(Map<byte[],NodeAddress> leafSet, Map<String,NodeAddress> routingTable, boolean broadcastMsg) {
		this.leafSet = leafSet;
		this.routingTable = routingTable;
		this.broadcastMsg = broadcastMsg;
	}

	public Map<byte[],NodeAddress> getLeafSet() {
		return leafSet;
	}

	public Map<String,NodeAddress> getRoutingTable() {
		return routingTable;
	}

	public boolean getBroadcastMsg() {
		return broadcastMsg;
	}

	@Override
	public int getMsgType() {
		return ROUTING_INFO_MSG;
	}
}

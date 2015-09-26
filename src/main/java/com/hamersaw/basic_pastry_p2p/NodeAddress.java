package com.hamersaw.basic_pastry_p2p;

import java.net.InetAddress;

public class NodeAddress {
	private InetAddress inetAddress;
	private int port;

	public NodeAddress(InetAddress inetAddress, int port) {
		this.inetAddress = inetAddress;
		this.port = port;
	}

	public InetAddress getInetAddress() {
		return inetAddress;
	}

	public int getPort() {
		return port;
	}

	@Override
	public boolean equals(Object o) {
		if(o instanceof NodeAddress) {
			NodeAddress nodeAddress = (NodeAddress) o;
			int hostName = inetAddress.getHostName().compareTo(nodeAddress.getInetAddress().getHostName());
			int hostAddress = inetAddress.getHostAddress().compareTo(nodeAddress.getInetAddress().getHostAddress());

			if(port == nodeAddress.getPort() && (hostName == 0 || hostAddress == 0)) {
				return true;
			}
		}

		return false;
	}
}

package com.hamersaw.basic_pastry;

import java.io.Serializable;

import java.net.InetAddress;

public class NodeAddress implements Serializable {
	private InetAddress inetAddress;
	private int port;

	public NodeAddress(InetAddress inetAddress, int port) {
		this.inetAddress = inetAddress;
		this.port = port;
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

	@Override
	public String toString() {
		return (inetAddress != null ? inetAddress.toString() : "null") + ":" + port;
	}
}

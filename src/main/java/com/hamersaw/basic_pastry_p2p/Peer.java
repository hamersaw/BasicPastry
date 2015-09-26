package com.hamersaw.basic_pastry_p2p;

public class Peer {
	private byte[] id;

	public Peer() {
		this.id = generateID();
	}

	public Peer(byte[] id) {
		this.id = id;
	}

	public static void main(String[] args) {

	}

	public byte[] generateID() {
		return null;
	}
}

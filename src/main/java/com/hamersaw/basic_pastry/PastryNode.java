package com.hamersaw.basic_pastry;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import java.util.Comparator;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

import java.nio.ByteBuffer;

import java.net.ServerSocket;
import java.net.Socket;

import com.hamersaw.basic_pastry.message.ErrorMsg;
import com.hamersaw.basic_pastry.message.LookupNodeMsg;
import com.hamersaw.basic_pastry.message.Message;
import com.hamersaw.basic_pastry.message.NodeJoinMsg;
import com.hamersaw.basic_pastry.message.NodeInfoMsg;
import com.hamersaw.basic_pastry.message.RegisterNodeMsg;
import com.hamersaw.basic_pastry.message.RoutingInfoMsg;
import com.hamersaw.basic_pastry.message.SuccessMsg;

public class PastryNode extends Thread {
	private static final Logger LOGGER = Logger.getLogger(PastryNode.class.getCanonicalName());
	public static final int ID_BYTES = 2;
	public static int MAX_LEAF_SET_SIZE = 1;
	protected byte[] id;
	protected short idValue;
	protected String idStr, discoveryNodeAddress;
	protected int discoveryNodePort, port;
	protected SortedMap<byte[],NodeAddress> lessThanLS, greaterThanLS;
	protected Map<String,NodeAddress>[] routingTable;
	protected Map<byte[],String> dataStore;
	protected ReadWriteLock readWriteLock;

	public PastryNode(byte[] id, String discoveryNodeAddress, int discoveryNodePort, int port) {
		this.id = id;
		idValue = convertBytesToShort(this.id);
		idStr = HexConverter.convertBytesToHex(id);
		this.discoveryNodeAddress = discoveryNodeAddress;
		this.discoveryNodePort = discoveryNodePort;
		this.port = port;

		//initialize tree map structures
		lessThanLS = new TreeMap(
				new Comparator<byte[]>() {
					@Override public int compare(byte[] b1, byte[] b2) {
						int s1 = lessThanDistance(convertBytesToShort(b1), idValue),
						    s2 = lessThanDistance(convertBytesToShort(b2), idValue);

						if(s1 < s2) {
							return 1;
						} else if(s1 > s2) {
							return -1;
						} else {
							return 0;
						}
					}
				}
			);

		greaterThanLS = new TreeMap(
				new Comparator<byte[]>() {
					@Override public int compare(byte[] b1, byte[] b2) {
						int s1 = greaterThanDistance(convertBytesToShort(b1), idValue),
						    s2 = greaterThanDistance(convertBytesToShort(b2), idValue);

						if(s1 > s2) {
							return 1;
						} else if(s1 < s2) {
							return -1;
						} else {
							return 0;
						}
					}
				}
			);

		//initialize routing table structure
		routingTable = new Map[4];
		for(int i=0; i<routingTable.length; i++) {
			routingTable[i] = new HashMap();
		}

		dataStore = new HashMap();

		//initialize locking mechanize
		readWriteLock = new ReentrantReadWriteLock();
	}

	public static void main(String[] args) {
		try {
			String discoveryNodeAddress = args[0];
			int discoveryNodePort = Integer.parseInt(args[1]);
			int port = Integer.parseInt(args[2]);
			byte[] id = args.length == 4 ? HexConverter.convertHexToBytes(args[3]) : generateRandomID(ID_BYTES);

			new Thread(new PastryNode(id, discoveryNodeAddress, discoveryNodePort, port)).start();
		} catch(Exception e) {
			LOGGER.severe(e.getMessage());
			System.out.println("Usage: PastryNode discoveryNodeAddress discoveryNodePort port [id]");
		}
	}

	@Override
	public void run() {
		try {
			ServerSocket serverSocket = new ServerSocket(port);

			//register your id with the discovery node
			boolean success = false;
			while(!success) {
				LOGGER.info("Registering ID '" + HexConverter.convertBytesToHex(id) + "'  to '" + discoveryNodeAddress + ":" + discoveryNodePort + "'");
				Socket discoveryNodeSocket = new Socket(discoveryNodeAddress, discoveryNodePort);
				RegisterNodeMsg registerNodeMsg = new RegisterNodeMsg(id, serverSocket.getInetAddress(), port);
				ObjectOutputStream out = new ObjectOutputStream(discoveryNodeSocket.getOutputStream());
				out.writeObject(registerNodeMsg);

				ObjectInputStream in = new ObjectInputStream(discoveryNodeSocket.getInputStream());
				Message replyMsg = (Message) in.readObject();
				discoveryNodeSocket.close();
				
				//perform action on reply message
				switch(replyMsg.getMsgType()) {
				case Message.NODE_INFO_MSG:
					NodeInfoMsg nodeInfoMsg = (NodeInfoMsg) replyMsg;
					LOGGER.info("Sending node join message to '" + nodeInfoMsg.getNodeAddress().getInetAddress() + ":" + nodeInfoMsg.getNodeAddress().getPort() + "'");

					//send node join message
					Socket nodeSocket = new Socket(nodeInfoMsg.getNodeAddress().getInetAddress(), nodeInfoMsg.getNodeAddress().getPort());
					NodeJoinMsg nodeJoinMsg = new NodeJoinMsg(id, 0, serverSocket.getInetAddress(), port);
					ObjectOutputStream nodeOut = new ObjectOutputStream(nodeSocket.getOutputStream());
					nodeOut.writeObject(nodeJoinMsg);

					success = true;
					break;
				case Message.SUCCESS_MSG:
					//if you're the first node registering a success messasge is sent back
					success = true;
					break;
				case Message.ERROR_MSG:
					LOGGER.severe(((ErrorMsg)replyMsg).getMsg());
					id = generateRandomID(ID_BYTES);
					continue;
				default:
					LOGGER.severe("Recieved an unexpected message type '" + replyMsg.getMsgType() + "'.");
					return;
				}
			}

			LOGGER.info("Node started successfully");
			while(true) {
				Socket socket = serverSocket.accept();
				LOGGER.fine("Received connection from '" + socket.getInetAddress() + ":" + socket.getPort() + "'.");

				new Thread(new PastryNodeWorker(socket)).start();
			}
		} catch(Exception e) {
			e.printStackTrace();
			LOGGER.severe(e.getMessage());
		}
	}

	private static byte[] generateRandomID(int length) {
		Random random = new Random();
		byte[] bytes = new byte[length];
		for(int i=0; i<bytes.length; i++) {
			bytes[i] = (byte) (random.nextInt() % 256);
		}

		return bytes;
	}

	protected short convertBytesToShort(byte[] bytes) {
		ByteBuffer buf = ByteBuffer.allocate(bytes.length);
		for(int i=0; i<bytes.length; i++) {
			buf.put(bytes[i]);
		}

		return buf.getShort(0);
	}

	protected int getSingleHexDistance(String s1, String s2) {
		return Math.abs(convertSingleHexToInt(s1) - convertSingleHexToInt(s2));
	}

	private int convertSingleHexToInt(String str) {
		return Integer.parseInt(str.replace("a","10").replace("b","11").replace("c","12").replace("d","13").replace("e","14").replace("f","15"));
	}

	protected int lessThanDistance(short s1, short s2) {
		int distance = 0;
		if(s1 < s2) {
			distance = Math.abs(s2 - s1);
		} else if(s1 > s2) {
			distance = Math.abs(Short.MAX_VALUE - s1) + Math.abs(s2 - Short.MIN_VALUE);
		}

		return distance;
	}

	protected int greaterThanDistance(short s1, short s2) {
		int distance = 0;
		if(s1 > s2) {
			distance = Math.abs(s1 - s2);
		} else if(s1 < s2) {
			distance = Math.abs(s1 - Short.MIN_VALUE) + Math.abs(Short.MAX_VALUE - s2);
		}

		return distance;
	}

	protected NodeAddress searchRoutingTableExact(byte[] nodeID, int prefixLength) {
		readWriteLock.readLock().lock();
		try {
			return routingTable[prefixLength].get(HexConverter.convertBytesToHex(nodeID).substring(prefixLength, prefixLength+1));
		} finally {
			readWriteLock.readLock().unlock();
		}
	}

	protected NodeAddress searchRoutingTableClosest(byte[] nodeID, int prefixLength) {
		readWriteLock.readLock().lock();
		try {
			String nodeIDStr = HexConverter.convertBytesToHex(nodeID);
			int minDistance = getSingleHexDistance(nodeIDStr.substring(prefixLength, prefixLength+1), nodeIDStr.substring(prefixLength, prefixLength+1));
			NodeAddress nodeAddress = null;
			for(Entry<String,NodeAddress> entry : routingTable[prefixLength].entrySet()) {
				int distance = getSingleHexDistance(entry.getKey(), nodeIDStr.substring(prefixLength, prefixLength+1));
				if(distance < minDistance) {
					minDistance = distance;
					nodeAddress = entry.getValue();
				}
			}
			return nodeAddress;
		} finally {
			readWriteLock.readLock().unlock();
		}
	}

	protected NodeAddress searchLeafSetClosest(byte[] nodeID) {
		readWriteLock.readLock().lock();
		try {
			short nodeIDValue = convertBytesToShort(nodeID);
			int minDistance = Math.min(lessThanDistance(idValue, nodeIDValue), greaterThanDistance(idValue, nodeIDValue));
			NodeAddress minNodeAddress = null;

			//check less than leaf set distances
			for(byte[] bytes : lessThanLS.keySet()) {
				short bytesValue = convertBytesToShort(bytes);
				int distance = Math.min(lessThanDistance(bytesValue, nodeIDValue), greaterThanDistance(bytesValue, nodeIDValue));
				if(distance < minDistance) {
					minDistance = distance;
					minNodeAddress = lessThanLS.get(bytes);
				}
			}

			//check greater than leaf set distances
			for(byte[] bytes : greaterThanLS.keySet()) {
				short bytesValue = convertBytesToShort(bytes);
				int distance = Math.min(lessThanDistance(bytesValue, nodeIDValue), greaterThanDistance(bytesValue, nodeIDValue));
				if(distance < minDistance) {
					minDistance = distance;
					minNodeAddress = greaterThanLS.get(bytes);
				}
			}

			if(minNodeAddress == null) {
				return new NodeAddress(null, port);
			} else {
				return minNodeAddress;
			}
		} finally {
			readWriteLock.readLock().unlock();
		}
	}

	protected Map<byte[],NodeAddress> getRelevantLeafSet() {
		readWriteLock.readLock().lock();
		try {
			//for now just adding every element of leaf set - everything is relevant
			Map<byte[],NodeAddress> relevantLeafSet = new HashMap();
			relevantLeafSet.putAll(lessThanLS);
			relevantLeafSet.putAll(greaterThanLS);
			relevantLeafSet.put(id, new NodeAddress(null, port));
			return relevantLeafSet;
		} finally {
			readWriteLock.readLock().unlock();
		}
	}

	protected Map<String,NodeAddress> getRelevantRoutingTable(int prefixLength) {
		readWriteLock.readLock().lock();
		try {
			return routingTable[prefixLength];
		} finally {
			readWriteLock.readLock().unlock();
		}
	}

	protected void updateLeafSet(byte[] addID, NodeAddress nodeAddress) {
		LOGGER.finest("updateLeafSet(" + HexConverter.convertBytesToHex(addID) + "," + nodeAddress + ")");
		readWriteLock.writeLock().lock();
		try {
			//check if this id belongs to this node
			if(java.util.Arrays.equals(addID, id)) {
				return;
			}

			short addIDValue = convertBytesToShort(addID);

			//search for value in less than leaf set
			boolean lessThanFound = false;
			for(byte[] bytes : lessThanLS.keySet()) {
				if(java.util.Arrays.equals(bytes, addID)) {
					return;
				}
			}

			if(!lessThanFound) {
				if(lessThanLS.size() < MAX_LEAF_SET_SIZE) { //leaf set can hold more less than nodes
					LOGGER.fine("\tAdded node " + HexConverter.convertBytesToHex(addID) + ":" + convertBytesToShort(addID) + " because there was open room in less than leaf set");
					lessThanLS.put(addID, nodeAddress);
				} else if(lessThanDistance(addIDValue, idValue) < lessThanDistance(convertBytesToShort(lessThanLS.firstKey()), idValue)) {
					LOGGER.fine("\tRemoved node " + HexConverter.convertBytesToHex(lessThanLS.firstKey()) + ":" + convertBytesToShort(lessThanLS.firstKey()) + " to add node " + HexConverter.convertBytesToHex(addID) + ":" + convertBytesToShort(addID));
					lessThanLS.remove(lessThanLS.firstKey());
					lessThanLS.put(addID, nodeAddress);
				}
			}

			//search for value in greater than leaf set
			boolean greaterThanFound = false;
			for(byte[] bytes : greaterThanLS.keySet()) {
				if(java.util.Arrays.equals(bytes, addID)) {
					greaterThanFound = true;
				}
			}

			if(!greaterThanFound) {
				if(greaterThanLS.size() < MAX_LEAF_SET_SIZE) { //leaf set can hold more greater than nodes
					LOGGER.fine("\tAdded node " + HexConverter.convertBytesToHex(addID) + ":" + convertBytesToShort(addID) + " because there was open room in greater than leaf set");
					greaterThanLS.put(addID, nodeAddress);
				} else if(greaterThanDistance(addIDValue, idValue) < greaterThanDistance(convertBytesToShort(greaterThanLS.lastKey()), idValue)) {
					LOGGER.fine("\tRemoved node " + HexConverter.convertBytesToHex(greaterThanLS.lastKey()) + ":" + convertBytesToShort(greaterThanLS.lastKey()) + " to add node " + HexConverter.convertBytesToHex(addID) + ":" + convertBytesToShort(addID));
					greaterThanLS.remove(greaterThanLS.lastKey());
					greaterThanLS.put(addID, nodeAddress);
				}
			}
		} finally {
			readWriteLock.writeLock().unlock();
		}
	}

	protected void updateRoutingTable(String addIDStr, NodeAddress nodeAddress, int prefixLength) {
		readWriteLock.writeLock().lock();
		try {
			//check if this is this node
			if(idStr.substring(prefixLength, prefixLength+1).equals(addIDStr)) {
				return;
			}

			//add entry to routing table
			if(!routingTable[prefixLength].containsKey(addIDStr)) {
				routingTable[prefixLength].put(addIDStr, nodeAddress);
			}
		} finally {
			readWriteLock.writeLock().unlock();
		}
	}

	protected class PastryNodeWorker extends Thread{
		protected Socket socket;

		public PastryNodeWorker(Socket socket) {
			this.socket  = socket;
		}

		@Override
		public void run() {
			try {
				//read request message
				ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
				Message requestMsg = (Message) in.readObject();

				Message replyMsg = null;
				switch(requestMsg.getMsgType()) {
				case Message.NODE_JOIN_MSG:
					LOGGER.info("Recieved node join message.");
					NodeJoinMsg nodeJoinMsg = (NodeJoinMsg) requestMsg;
					if(nodeJoinMsg.getInetAddress() == null) {
						nodeJoinMsg.setInetAddress(socket.getInetAddress()); //TODO don't like doing this but i don't know a better way
					}
					int p = nodeJoinMsg.getPrefixLength();

					//search for an exact match in the routing table
					NodeAddress nodeAddress = searchRoutingTableExact(nodeJoinMsg.getID(), p);
					if(nodeAddress != null) {
						//update longest prefix match if we have indeed found a match
						nodeJoinMsg.setLongestPrefixMatch((nodeJoinMsg.getPrefixLength() + 1));
					}

					//search for closest node in routing table
					if(nodeAddress == null) {
						nodeAddress = searchRoutingTableClosest(nodeJoinMsg.getID(), p);
					}

					//find closest node in leaf set
					if(nodeAddress == null) {
						nodeAddress = searchLeafSetClosest(nodeJoinMsg.getID());
					}

					//send routing information to joining node
					Socket joinNodeSocket = new Socket(nodeJoinMsg.getInetAddress(), nodeJoinMsg.getPort());
					ObjectOutputStream joinNodeOut = new ObjectOutputStream(joinNodeSocket.getOutputStream());
					joinNodeOut.writeObject(
						new RoutingInfoMsg(
							getRelevantLeafSet(),
							p,
							getRelevantRoutingTable(p),
							nodeAddress.getInetAddress() == null //last routing info message recieved by the joining node
						)
					);

					joinNodeSocket.close();

					//if we found a closer node forward the node join message
					if(nodeAddress.getInetAddress() != null) {
						LOGGER.info("Forwarding node join to '" + nodeAddress + "'");
						Socket nodeSocket = new Socket(nodeAddress.getInetAddress(), nodeAddress.getPort());
						ObjectOutputStream nodeOut = new ObjectOutputStream(nodeSocket.getOutputStream());
						nodeOut.writeObject(nodeJoinMsg);

						nodeSocket.close();
					}
					break;
				case Message.ROUTING_INFO_MSG:
					RoutingInfoMsg routingInfoMsg = (RoutingInfoMsg) requestMsg;
					LOGGER.info("Recieved routing info message with " + routingInfoMsg.getLeafSet().size() + " routes.");

					//loop through leaf set
					for(Entry<byte[],NodeAddress> entry : routingInfoMsg.getLeafSet().entrySet()) {
						//update leaf set
						if(entry.getValue().getInetAddress() == null) {
							updateLeafSet(entry.getKey(), new NodeAddress(socket.getInetAddress(), entry.getValue().getPort()));
						} else {
							updateLeafSet(entry.getKey(), entry.getValue());
						}

						//update routing table
						if(!java.util.Arrays.equals(entry.getKey(), id)) { //if not this id
							String nodeIDStr = HexConverter.convertBytesToHex(entry.getKey());
							int i=0;
							for(i=0; i<4; i++) {
								if(idStr.charAt(i) != nodeIDStr.charAt(i)) {
									nodeIDStr = "" + nodeIDStr.charAt(i);
									break;
								}
							}

							if(entry.getValue().getInetAddress() == null) {
								updateRoutingTable(nodeIDStr, new NodeAddress(socket.getInetAddress(), entry.getValue().getPort()), i);
							} else {
								updateRoutingTable(nodeIDStr, entry.getValue(), i);
							}
						}
					}

					//update routing table
					for(Entry<String,NodeAddress> entry : routingInfoMsg.getRoutingTable().entrySet()) {
						updateRoutingTable(entry.getKey(), entry.getValue(), routingInfoMsg.getPrefixLength());
					}

					//print out leaf set and routing table
					readWriteLock.readLock().lock();
					try {
						StringBuilder routingInfo = new StringBuilder("----LEAF SET----");
						for(Entry<byte[],NodeAddress> entry : lessThanLS.entrySet()) {
							routingInfo.append("\n" + HexConverter.convertBytesToHex(entry.getKey()) + ":" + convertBytesToShort(entry.getKey()) + " - " + entry.getValue());
						}
						routingInfo.append("\n" + HexConverter.convertBytesToHex(id) + ":" + convertBytesToShort(id) + " - " + port);
						for(Entry<byte[],NodeAddress> entry : greaterThanLS.entrySet()) {
							routingInfo.append("\n" + HexConverter.convertBytesToHex(entry.getKey()) + ":" + convertBytesToShort(entry.getKey()) + " - " + entry.getValue());
						}
						routingInfo.append("\n----------------");

						routingInfo.append("\n----ROUTING TABLE----");
						for(int i=0; i<routingTable.length; i++) {
							Map<String,NodeAddress> map = routingTable[i];
							routingInfo.append("\nprefix length:" + i);
							for(Entry<String,NodeAddress> entry : map.entrySet()) {
								routingInfo.append("\n\t" + entry.getKey() + " : " + entry.getValue());
							}
						}
						routingInfo.append("\n---------------------");
						LOGGER.info(routingInfo.toString());
					} finally {
						readWriteLock.readLock().unlock();
					}

					//if this is a message from the closest node send routing information to every node in leaf set
					readWriteLock.readLock().lock();
					try {
						if(routingInfoMsg.getBroadcastMsg()) {
							List<NodeAddress> nodeBlacklist = new LinkedList();

							//send to less than leaf set
							for(Entry<byte[],NodeAddress> entry : lessThanLS.entrySet()) {
								if(nodeBlacklist.contains(entry.getValue())) {
									continue;
								} else {
									nodeBlacklist.add(entry.getValue());
								}

								//find longest prefix match
								String nodeIDStr = HexConverter.convertBytesToHex(entry.getKey());
								int i=0;
								for(i=0; i<4; i++) {
									if(idStr.charAt(i) != nodeIDStr.charAt(i)) {
										break;
									}
								}

								//send routing update
								Socket nodeSocket = new Socket(entry.getValue().getInetAddress(), entry.getValue().getPort());
								ObjectOutputStream nodeOut = new ObjectOutputStream(nodeSocket.getOutputStream());
								nodeOut.writeObject(new RoutingInfoMsg(getRelevantLeafSet(), i, routingTable[i], false));

								nodeSocket.close();
							}

							//send to greater than leaf set
							for(Entry<byte[],NodeAddress> entry : greaterThanLS.entrySet()) {
								if(nodeBlacklist.contains(entry.getValue())) {
									continue;
								} else {
									nodeBlacklist.add(entry.getValue());
								}

								//find longest prefix match
								String nodeIDStr = HexConverter.convertBytesToHex(entry.getKey());
								int i=0;
								for(i=0; i<4; i++) {
									if(idStr.charAt(i) != nodeIDStr.charAt(i)) {
										break;
									}
								}

								//send routing update
								Socket nodeSocket = new Socket(entry.getValue().getInetAddress(), entry.getValue().getPort());
								ObjectOutputStream nodeOut = new ObjectOutputStream(nodeSocket.getOutputStream());
								nodeOut.writeObject(new RoutingInfoMsg(getRelevantLeafSet(), i, routingTable[i], false));

								nodeSocket.close();
							}

							//send to routing table
							for(int i=0; i<routingTable.length; i++) {
								Map<String,NodeAddress> map = routingTable[i];

								for(Entry<String,NodeAddress> entry : map.entrySet()) {
									if(nodeBlacklist.contains(entry.getValue())) {
										continue;
									} else {
										nodeBlacklist.add(entry.getValue());
									}

									Socket nodeSocket = new Socket(entry.getValue().getInetAddress(), entry.getValue().getPort());
									ObjectOutputStream nodeOut = new ObjectOutputStream(nodeSocket.getOutputStream());
									nodeOut.writeObject(new RoutingInfoMsg(getRelevantLeafSet(), i, map, false));
	
									nodeSocket.close();

								}
							}
						}
					} finally {
						readWriteLock.readLock().unlock();
					}
					break;
				case Message.LOOKUP_NODE_MSG:
					LookupNodeMsg lookupNodeMsg = (LookupNodeMsg) requestMsg;
					LOGGER.info("Recieved lookup node message with id '" + HexConverter.convertBytesToHex(lookupNodeMsg.getID()) + "'.");
					if(lookupNodeMsg.getNodeAddress().getInetAddress() == null) {
						lookupNodeMsg.getNodeAddress().setInetAddress(socket.getInetAddress()); //TODO better way to do this
					}

					NodeAddress forwardAddr = null;

					//check if data belongs in leaf set
					int nodeIDValue = convertBytesToShort(lookupNodeMsg.getID()),
						lsMinValue = convertBytesToShort(lessThanLS.firstKey()),
						lsMaxValue = convertBytesToShort(greaterThanLS.lastKey());

					if((lsMaxValue > lsMinValue && lsMinValue <= nodeIDValue && lsMaxValue >= nodeIDValue) //min=-10, id=-6, max=-4
						|| (lsMaxValue < lsMinValue && (lsMinValue <= nodeIDValue || lsMaxValue >= nodeIDValue))) { //min = 10, id = -4, max = -6
						
						forwardAddr = searchLeafSetClosest(lookupNodeMsg.getID());
					}

					if(forwardAddr == null) {
						//search for exact prefix match in routing table
						forwardAddr = searchRoutingTableExact(lookupNodeMsg.getID(), lookupNodeMsg.getPrefixLength());
						
						if(forwardAddr != null) {
							lookupNodeMsg.setLongestPrefixMatch(lookupNodeMsg.getPrefixLength() + 1);
						}
					}

					if(forwardAddr == null) {
						//search clostest value in routing table
						forwardAddr = searchRoutingTableClosest(lookupNodeMsg.getID(), lookupNodeMsg.getPrefixLength());
					}

					if(forwardAddr == null) {
						//worst case forward to closest node in leaf set
						forwardAddr = searchLeafSetClosest(lookupNodeMsg.getID());
					}

					if(forwardAddr.getInetAddress() == null) {
						//this is the node where data needs to reside
						LOGGER.info("TODO this is the node that will hold the data. Send response to '" + lookupNodeMsg.getNodeAddress() + "'");

						Socket socket = new Socket(lookupNodeMsg.getNodeAddress().getInetAddress(), lookupNodeMsg.getNodeAddress().getPort());
						ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
						out.writeObject(new NodeInfoMsg(id, new NodeAddress(null, port)));
						socket.close();

						//TODO get next message - WriteFileMsg/ReadFileMsg - make it happen

						LOGGER.info("Stored data with hash '" + HexConverter.convertBytesToHex(lookupNodeMsg.getID()) + " under file '" + lookupNodeMsg.getFilename() + "'.");
						dataStore.put(lookupNodeMsg.getID(), lookupNodeMsg.getFilename());
					} else {
						//forward request to the correct node
						LOGGER.info("Forwarding store data message to node '" + forwardAddr + "'.");
						Socket socket = new Socket(forwardAddr.getInetAddress(), forwardAddr.getPort());
						ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
						out.writeObject(lookupNodeMsg);

						socket.close();
					}

					break;
				default:
					LOGGER.severe("Unrecognized request message type '" + requestMsg.getMsgType() + "'");
					break;
				}

				//write reply message
				if(replyMsg != null) {
					ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
					out.writeObject(replyMsg);
				}
			} catch(Exception e) {
				e.printStackTrace();
				LOGGER.severe(e.getMessage());
			}
		}
	}
}

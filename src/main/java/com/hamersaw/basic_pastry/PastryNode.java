package com.hamersaw.basic_pastry;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
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
import com.hamersaw.basic_pastry.message.ReadDataMsg;
import com.hamersaw.basic_pastry.message.RegisterNodeMsg;
import com.hamersaw.basic_pastry.message.RoutingInfoMsg;
import com.hamersaw.basic_pastry.message.SuccessMsg;
import com.hamersaw.basic_pastry.message.WriteDataMsg;

public class PastryNode extends Thread {
	private static final Logger LOGGER = Logger.getLogger(PastryNode.class.getCanonicalName());
	public static final int ID_BYTES = 2;
	public static int MAX_LEAF_SET_SIZE = 1;
	protected String name;
	protected byte[] id;
	protected short idValue;
	protected String idStr, storageDir, discoveryNodeAddress;
	protected int discoveryNodePort, port;
	protected SortedMap<byte[],NodeAddress> lessThanLS, greaterThanLS;
	protected Map<String,NodeAddress>[] routingTable;
	protected List<String> dataStore;
	protected ReadWriteLock readWriteLock;

	public PastryNode(String name, byte[] id, String storageDir, String discoveryNodeAddress, int discoveryNodePort, int port) {
		this.name = name;
		this.id = id;
		idValue = convertBytesToShort(this.id);
		idStr = HexConverter.convertBytesToHex(id);
		this.storageDir = storageDir;
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

		dataStore = new LinkedList();

		//initialize locking mechanize
		readWriteLock = new ReentrantReadWriteLock();
	}

	public static void main(String[] args) {
		try {
			String name = args[0], storageDir = args[1], discoveryNodeAddress = args[2];
			int discoveryNodePort = Integer.parseInt(args[3]);
			int port = Integer.parseInt(args[4]);
			byte[] id = args.length == 6 ? HexConverter.convertHexToBytes(args[5]) : generateRandomID(ID_BYTES);

			new Thread(new PastryNode(name, id, storageDir, discoveryNodeAddress, discoveryNodePort, port)).start();
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
				LOGGER.info("Registering ID '" + HexConverter.convertBytesToHex(id) + "' to '" + discoveryNodeAddress + ":" + discoveryNodePort + "'");
				Socket discoveryNodeSocket = new Socket(discoveryNodeAddress, discoveryNodePort);
				RegisterNodeMsg registerNodeMsg = new RegisterNodeMsg(name, id, serverSocket.getInetAddress(), port);
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
					NodeJoinMsg nodeJoinMsg = new NodeJoinMsg(id, 0, new NodeAddress(name, null, port));
					nodeJoinMsg.addHop(nodeInfoMsg.getNodeAddress());
					Socket nodeSocket = new Socket(nodeInfoMsg.getNodeAddress().getInetAddress(), nodeInfoMsg.getNodeAddress().getPort());
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
			int minDistance = getSingleHexDistance(idStr.substring(prefixLength, prefixLength+1), nodeIDStr.substring(prefixLength, prefixLength+1));
			short minValue = (short) convertSingleHexToInt(idStr.substring(prefixLength, prefixLength+1));
			NodeAddress nodeAddress = null;
			for(Entry<String,NodeAddress> entry : routingTable[prefixLength].entrySet()) {
				int distance = getSingleHexDistance(entry.getKey(), nodeIDStr.substring(prefixLength, prefixLength+1));
				short value = (short) convertSingleHexToInt(entry.getKey());
				if(distance < minDistance || (distance == minDistance && minValue < value )) {
					minDistance = distance;
					minValue = value;
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
			short minValue = nodeIDValue;
			NodeAddress minNodeAddress = null;

			//check less than leaf set distances
			for(byte[] bytes : lessThanLS.keySet()) {
				short bytesValue = convertBytesToShort(bytes);
				int distance = Math.min(lessThanDistance(bytesValue, nodeIDValue), greaterThanDistance(bytesValue, nodeIDValue));
				if(distance < minDistance || (distance == minDistance && minValue < bytesValue)) {
					minDistance = distance;
					minValue = bytesValue;
					minNodeAddress = lessThanLS.get(bytes);
				}
			}

			//check greater than leaf set distances
			for(byte[] bytes : greaterThanLS.keySet()) {
				short bytesValue = convertBytesToShort(bytes);
				int distance = Math.min(lessThanDistance(bytesValue, nodeIDValue), greaterThanDistance(bytesValue, nodeIDValue));
				if(distance < minDistance || (distance == minDistance && minValue < bytesValue)) {
					minDistance = distance;
					minValue = bytesValue;
					minNodeAddress = greaterThanLS.get(bytes);
				}
			}

			if(minNodeAddress == null) {
				return new NodeAddress(name, null, port);
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
			relevantLeafSet.put(id, new NodeAddress(name, null, port));
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

	protected boolean updateLeafSet(byte[] addID, NodeAddress nodeAddress) {
		LOGGER.finest("updateLeafSet(" + HexConverter.convertBytesToHex(addID) + "," + nodeAddress + ")");
		boolean changed = false;
		readWriteLock.writeLock().lock();
		try {
			//check if this id belongs to this node
			if(java.util.Arrays.equals(addID, id)) {
				return changed;
			}

			short addIDValue = convertBytesToShort(addID);

			//search for value in less than leaf set
			boolean lessThanFound = false;
			for(byte[] bytes : lessThanLS.keySet()) {
				if(java.util.Arrays.equals(bytes, addID)) {
					return changed;
				}
			}

			if(!lessThanFound) {
				if(lessThanLS.size() < MAX_LEAF_SET_SIZE) { //leaf set can hold more less than nodes
					LOGGER.fine("\tAdded node " + HexConverter.convertBytesToHex(addID) + ":" + convertBytesToShort(addID) + " because there was open room in less than leaf set");
					lessThanLS.put(addID, nodeAddress);
					changed = true;
				} else if(lessThanDistance(addIDValue, idValue) < lessThanDistance(convertBytesToShort(lessThanLS.firstKey()), idValue)) {
					LOGGER.fine("\tRemoved node " + HexConverter.convertBytesToHex(lessThanLS.firstKey()) + ":" + convertBytesToShort(lessThanLS.firstKey()) + " to add node " + HexConverter.convertBytesToHex(addID) + ":" + convertBytesToShort(addID));
					lessThanLS.remove(lessThanLS.firstKey());
					lessThanLS.put(addID, nodeAddress);
					changed = true;
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
					changed = true;
				} else if(greaterThanDistance(addIDValue, idValue) < greaterThanDistance(convertBytesToShort(greaterThanLS.lastKey()), idValue)) {
					LOGGER.fine("\tRemoved node " + HexConverter.convertBytesToHex(greaterThanLS.lastKey()) + ":" + convertBytesToShort(greaterThanLS.lastKey()) + " to add node " + HexConverter.convertBytesToHex(addID) + ":" + convertBytesToShort(addID));
					greaterThanLS.remove(greaterThanLS.lastKey());
					greaterThanLS.put(addID, nodeAddress);
					changed = true;
				}
			}
		} finally {
			readWriteLock.writeLock().unlock();
		}

		return changed;
	}

	protected boolean updateRoutingTable(String addIDStr, NodeAddress nodeAddress, int prefixLength) {
		readWriteLock.writeLock().lock();
		try {
			//check if this is this node
			if(idStr.substring(prefixLength, prefixLength+1).equals(addIDStr)) {
				return false;
			}

			//add entry to routing table
			if(!routingTable[prefixLength].containsKey(addIDStr)) {
				routingTable[prefixLength].put(addIDStr, nodeAddress);
				return true;
			}
		} finally {
			readWriteLock.writeLock().unlock();
		}

		return false;
	}

	protected void printRoutingInfo() {
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
	}

	protected String getFilename(String storageDirectory, String filename) {
		if(filename.charAt(0) == File.separatorChar) {
			return storageDirectory + filename;
		} else {
			return storageDirectory + File.separatorChar + filename;
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
					NodeJoinMsg nodeJoinMsg = (NodeJoinMsg) requestMsg;
					if(nodeJoinMsg.getNodeAddress().getInetAddress() == null) {
						nodeJoinMsg.getNodeAddress().setInetAddress(socket.getInetAddress()); //TODO don't like doing this but i don't know a better way
					}
					LOGGER.info("Recieved node join message '" + nodeJoinMsg.toString() + "'.");
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
					if(nodeAddress == null || nodeJoinMsg.hopContains(nodeAddress)) {
						nodeAddress = searchLeafSetClosest(nodeJoinMsg.getID());
					}

					//send routing information to joining node
					Socket joinNodeSocket = new Socket(nodeJoinMsg.getNodeAddress().getInetAddress(), nodeJoinMsg.getNodeAddress().getPort());
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
						LOGGER.info("Forwarding node join message with id '" + HexConverter.convertBytesToHex(nodeJoinMsg.getID()) + "' to '" + nodeAddress + "'");
						nodeJoinMsg.addHop(nodeAddress);
						Socket nodeSocket = new Socket(nodeAddress.getInetAddress(), nodeAddress.getPort());
						ObjectOutputStream nodeOut = new ObjectOutputStream(nodeSocket.getOutputStream());
						nodeOut.writeObject(nodeJoinMsg);

						nodeSocket.close();
					}
					break;
				case Message.ROUTING_INFO_MSG:
					RoutingInfoMsg routingInfoMsg = (RoutingInfoMsg) requestMsg;
					LOGGER.fine("Recieved routing info message with " + routingInfoMsg.getLeafSet().size() + " routes.");
					boolean changed = false;

					//loop through leaf set
					for(Entry<byte[],NodeAddress> entry : routingInfoMsg.getLeafSet().entrySet()) {
						//update leaf set
						if(entry.getValue().getInetAddress() == null) {
							changed = updateLeafSet(entry.getKey(), new NodeAddress(entry.getValue().getNodeName(), socket.getInetAddress(), entry.getValue().getPort())) || changed;
						} else {
							changed = updateLeafSet(entry.getKey(), entry.getValue()) || changed;
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
								changed = updateRoutingTable(nodeIDStr, new NodeAddress(entry.getValue().getNodeName(), socket.getInetAddress(), entry.getValue().getPort()), i) || changed;
							} else {
								changed = updateRoutingTable(nodeIDStr, entry.getValue(), i) || changed;
							}
						}
					}

					//update routing table
					for(Entry<String,NodeAddress> entry : routingInfoMsg.getRoutingTable().entrySet()) {
						changed = updateRoutingTable(entry.getKey(), entry.getValue(), routingInfoMsg.getPrefixLength()) || changed;
					}

					//print out leaf set and routing table
					if(changed) {
						printRoutingInfo();
					}

					readWriteLock.readLock().lock();
					try {
						//if this is a message from the closest node send routing information to every node in leaf set
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

						//transfer data to other nodes if needed
						for(String dataID : dataStore) {
							short dataIDValue = convertBytesToShort(HexConverter.convertHexToBytes(dataID));
							int minDistance = Math.min(lessThanDistance(idValue, dataIDValue), greaterThanDistance(idValue, dataIDValue));
							NodeAddress forwardNodeAddress = null;

							//check less than leaf set
							for(Entry<byte[],NodeAddress> entry : lessThanLS.entrySet()) {
								short nodeIDValue = convertBytesToShort(entry.getKey());
								int distance = Math.min(lessThanDistance(dataIDValue, nodeIDValue), greaterThanDistance(dataIDValue, nodeIDValue)); 
								if(distance < minDistance) {
									minDistance = distance;
									forwardNodeAddress = entry.getValue();
								}
							}

							//check greater than leaf set
							for(Entry<byte[],NodeAddress> entry : greaterThanLS.entrySet()) {
								short nodeIDValue = convertBytesToShort(entry.getKey());
								int distance = Math.min(lessThanDistance(dataIDValue, nodeIDValue), greaterThanDistance(dataIDValue, nodeIDValue)); 
								if(distance < minDistance) {
									minDistance = distance;
									forwardNodeAddress = entry.getValue();
								}
							}

							if(forwardNodeAddress != null) {
								//read file
								File file = new File(getFilename(storageDir,dataID));
								byte[] data = new byte[(int)file.length()];
								FileInputStream fileIn = new FileInputStream(file);
								if(fileIn.read(data) != data.length) {
									throw new Exception("Unknown error reading file.");
								}

								fileIn.close();

								//send write data message to node
								LOGGER.info("Forwarding data with id '" + dataID + "' to node " + forwardNodeAddress + ".");
								Socket forwardSocket = new Socket(forwardNodeAddress.getInetAddress(), forwardNodeAddress.getPort());
								ObjectOutputStream forwardOut = new ObjectOutputStream(forwardSocket.getOutputStream());
								forwardOut.writeObject(new WriteDataMsg(HexConverter.convertHexToBytes(dataID), data));

								forwardSocket.close();
								file.delete();
							}
						}
					} finally {
						readWriteLock.readLock().unlock();
					}
					break;
				case Message.LOOKUP_NODE_MSG:
					LookupNodeMsg lookupNodeMsg = (LookupNodeMsg) requestMsg;
					if(lookupNodeMsg.getNodeAddress().getInetAddress() == null) {
						lookupNodeMsg.getNodeAddress().setInetAddress(socket.getInetAddress()); //TODO better way to do this
					}

					LOGGER.info("Recieved lookup node message '" + lookupNodeMsg.toString() + "'");
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
						LOGGER.info("This is the closest node. Send response to '" + lookupNodeMsg.getNodeAddress() + "'");

						Socket socket = new Socket(lookupNodeMsg.getNodeAddress().getInetAddress(), lookupNodeMsg.getNodeAddress().getPort());
						ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
						out.writeObject(new NodeInfoMsg(id, new NodeAddress(name, null, port)));
						socket.close();
					} else {
						//forward request to the correct node
						lookupNodeMsg.addHop(forwardAddr);
						LOGGER.info("Forwarding lookup node message for id '" + HexConverter.convertBytesToHex(lookupNodeMsg.getID())+ "' to node '" + forwardAddr + "'.");
						Socket socket = new Socket(forwardAddr.getInetAddress(), forwardAddr.getPort());
						ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
						out.writeObject(lookupNodeMsg);

						socket.close();
					}

					break;
				case Message.WRITE_DATA_MSG:
					WriteDataMsg writeDataMsg = (WriteDataMsg) requestMsg;
					String writeDataID = HexConverter.convertBytesToHex(writeDataMsg.getID());
					
					//write data to disk
					File writeFile = new File(getFilename(storageDir, writeDataID));
					writeFile.getParentFile().mkdirs();
					FileOutputStream out = new FileOutputStream(writeFile);
					for(byte b : writeDataMsg.getData()) {
						out.write(b);
					}
					out.close();

					//add id to datastore structure
					dataStore.add(writeDataID);
					LOGGER.info("Wrote data with id '" + writeDataID + "'.");
					break;
				case Message.READ_DATA_MSG:
					ReadDataMsg readDataMsg = (ReadDataMsg) requestMsg;
					String readDataID = HexConverter.convertBytesToHex(readDataMsg.getID());

					//check if datastore contains readDataID
					if(!dataStore.contains(readDataID)) {
						replyMsg = new ErrorMsg("Node does not contain data with id '" + readDataID + "'.");
					} else {
						//read data from disk
						File readFile = new File(getFilename(storageDir, readDataID));
						byte[] data = new byte[(int)readFile.length()];
						FileInputStream fileIn = new FileInputStream(readFile);
						if(fileIn.read(data) != readFile.length()) {
							replyMsg = new ErrorMsg("Error reading data.");
						} else {
							replyMsg = new WriteDataMsg(readDataMsg.getID(), data);
						}

						fileIn.close();
					}

					LOGGER.info("Read data with id '" + readDataID + "'.");
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

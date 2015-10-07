package com.hamersaw.basic_pastry;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;
import java.util.Map;
import java.util.Map.Entry;
import java.util.HashMap;
import java.util.Random;

import com.hamersaw.basic_pastry.message.ErrorMsg;
import com.hamersaw.basic_pastry.message.Message;
import com.hamersaw.basic_pastry.message.NodeInfoMsg;
import com.hamersaw.basic_pastry.message.RemoveNodeMsg;
import com.hamersaw.basic_pastry.message.RegisterNodeMsg;
import com.hamersaw.basic_pastry.message.RequestRandomNodeMsg;
import com.hamersaw.basic_pastry.message.SuccessMsg;

public class DiscoveryNode extends Thread {
	private static final Logger LOGGER = Logger.getLogger(DiscoveryNode.class.getCanonicalName());
	private Random random;
	protected int port;
	protected Map<byte[],NodeAddress>  nodes;
	protected ReadWriteLock readWriteLock;

	public DiscoveryNode(int port) {
		random = new Random();
		this.port = port;
		nodes = new HashMap();
		readWriteLock = new ReentrantReadWriteLock();
	}

	public static void main(String[] args) {
		try {
			int port = Integer.parseInt(args[0]);

			new Thread(new DiscoveryNode(port)).start();
		} catch(Exception e) {
			System.out.println("Usage: DiscoveryNode port");
		}
	}

	@Override
	public void run() {
		try {
			ServerSocket serverSocket = new ServerSocket(port);
			LOGGER.info("discovery node started successfully on port " + port);

			//accept connections
			while(true) {
				Socket socket = serverSocket.accept();
				LOGGER.fine("Received connection from '" + socket.getInetAddress() + ":" + socket.getPort() + "'.");

				new Thread(new DiscoveryNodeWorker(socket)).start();
			}
		} catch(Exception e) {
			LOGGER.severe(e.getMessage());
		}
	}

	protected void addNode(byte[] id, NodeAddress nodeAddress) throws Exception {
		//check to see if id is already registered
		readWriteLock.readLock().lock();
		try {
			//check to see if id already exists in cluster
			for(byte[] array : nodes.keySet()) {
				if(java.util.Arrays.equals(array, id)) {
					throw new Exception("ID '" + HexConverter.convertBytesToHex(id) + "' already exists in cluster");
				}
			}
		} finally {
			readWriteLock.readLock().unlock();
		}

		//add node to cluster using id and 
		readWriteLock.writeLock().lock();
		try {
			nodes.put(id, nodeAddress);
			LOGGER.info("Added ID '" + HexConverter.convertBytesToHex(id) + "' for node at '" + nodeAddress + "'.");
		} finally {
			readWriteLock.writeLock().unlock();
		}
	}

	protected byte[] getRandomNode() throws Exception {
		readWriteLock.readLock().lock();
		try {
			if(nodes.size() != 0) {
				//generate random number and iterate through peers while decrementing count
				int count = random.nextInt() % nodes.size();
				if(count < 0) {
					count *= -1;
				}

				//find "count"th element in nodes
				for(byte[] id : nodes.keySet()) {
					if(count-- == 0) {
						return id;
					}
				}
			}

			return null;
		} finally {
			readWriteLock.readLock().unlock();
		}
	}

	protected void removeNode(byte[] id, NodeAddress nodeAddress) throws Exception {
		readWriteLock.writeLock().lock();
		try {
			//search for matching id
			byte[] removeArray = null;
			for(byte[] array : nodes.keySet()) {
				if(java.util.Arrays.equals(array, id)) {
					removeArray = array;
				}
			}

			if(removeArray == null) {
				throw new Exception("Unable to remove node. ID '" + HexConverter.convertBytesToHex(id) + "' not found.");
			} else {
				//check if inet address is matching
				if(!nodes.get(removeArray).equals(nodeAddress)) {
					throw new Exception("Unable to remove node. Requesting node address does not match stored address.");
				} else {
					//remove node
					nodes.remove(removeArray);
					LOGGER.info("Removed ID '" + HexConverter.convertBytesToHex(id) + "' for node at '" + nodeAddress + "'.");
				}
			}
		} finally {
			readWriteLock.writeLock().unlock();
		}
	}

	private class DiscoveryNodeWorker extends Thread{
		protected Socket socket;

		public DiscoveryNodeWorker(Socket socket) {
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
				case Message.REGISTER_NODE_MSG:
					RegisterNodeMsg registerNodeMsg = (RegisterNodeMsg) requestMsg;

					try {
						byte[] id = getRandomNode(); //getting random node before so we don't ahve to worry about blacklisting the node we're adding
						if(id == null) {
							replyMsg = new SuccessMsg();
						} else {
							readWriteLock.readLock().lock();
							try {
								replyMsg = new NodeInfoMsg(id, nodes.get(id));
							} finally {
								readWriteLock.readLock().unlock();
							}
						}

						addNode(registerNodeMsg.getID(), new NodeAddress(socket.getInetAddress(), registerNodeMsg.getPort()));
					} catch(Exception e) {
						replyMsg = new ErrorMsg(e.getMessage());
					}
					break;
				case Message.REMOVE_NODE_MSG:
					RemoveNodeMsg removeNodeMsg = (RemoveNodeMsg) requestMsg;

					try {
						removeNode(removeNodeMsg.getID(), new NodeAddress(socket.getInetAddress(), removeNodeMsg.getPort()));
						replyMsg = new SuccessMsg();
					} catch(Exception e) {
						replyMsg = new ErrorMsg(e.getMessage());
					}
					break;
				case Message.REQUEST_RANDOM_NODE_MSG:
					try {
						byte[] id = getRandomNode();
						if(id == null) {
							replyMsg = new ErrorMsg("There aren't any nodes registered in the cluster yet.");
						} else {
							readWriteLock.readLock().lock();
							try {
								replyMsg = new NodeInfoMsg(id, nodes.get(id));
							} finally {
								readWriteLock.readLock().unlock();
							}
						}
					} catch(Exception e) {
						replyMsg = new ErrorMsg(e.getMessage());
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
				LOGGER.severe(e.getMessage());
			}
		}
	}
}

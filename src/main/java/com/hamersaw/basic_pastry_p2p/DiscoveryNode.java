package com.hamersaw.basic_pastry_p2p;

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

import com.hamersaw.basic_pastry_p2p.message.ErrorMsg;
import com.hamersaw.basic_pastry_p2p.message.Message;
import com.hamersaw.basic_pastry_p2p.message.RemoveNodeMsg;
import com.hamersaw.basic_pastry_p2p.message.RegisterNodeMsg;
import com.hamersaw.basic_pastry_p2p.message.RegisterNodeReplyMsg;
import com.hamersaw.basic_pastry_p2p.message.SuccessMsg;

public class DiscoveryNode extends Thread {
	private static final Logger LOGGER = Logger.getLogger(DiscoveryNode.class.getCanonicalName());
	private Random random;
	protected int port;
	protected Map<byte[],NodeAddress>  nodes;
	protected ReadWriteLock readWriteLock;

	public DiscoveryNode(int port) {
		random = new Random();
		nodes = new HashMap();
		readWriteLock = new ReentrantReadWriteLock();
	}

	public static void main(String[] args) {
		try {
			int port = Integer.parseInt(args[0]);

			new Thread(
				new DiscoveryNode(port)
			).start();
		} catch(Exception e) {
			System.out.println("Usage: DiscoveryNode port");
			System.exit(1);
		}
	}

	@Override
	public void run() {
		try {
			ServerSocket serverSocket = new ServerSocket(port);
			LOGGER.info("Chunk server started successfully");

			while(true) {
				Socket socket = serverSocket.accept();
				LOGGER.fine("Received connection from '" + socket.getInetAddress() + ":" + socket.getPort() + "'.");

				new Thread(new DiscoveryNodeWorker(socket, this)).start();
			}
		} catch(Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	protected void addNode(byte[] id, NodeAddress nodeAddress) throws Exception {
		//check to see if id is already registered
		readWriteLock.readLock().lock();
		try {
			//check to see if id already exists in cluster
			for(byte[] array : nodes.keySet()) {
				if(java.util.Arrays.equals(array, id)) {
					throw new Exception("ID already exists in cluster");
				}
			}
		} finally {
			readWriteLock.readLock().unlock();
		}

		//add node to cluster using id and 
		readWriteLock.writeLock().lock();
		try {
			nodes.put(id, nodeAddress);
		} finally {
			readWriteLock.writeLock().unlock();
		}
	}

	protected RegisterNodeReplyMsg genNodeReplyMsg() throws Exception {
		readWriteLock.readLock().lock();
		try {
			if(nodes.size() != 0) {
				//generate random number and iterate through peers until found
				int num = random.nextInt() % nodes.size();	
				for(Entry<byte[],NodeAddress> entry : nodes.entrySet()) {
					if(num-- == 0) {
						return new RegisterNodeReplyMsg(entry.getKey(), entry.getValue().getInetAddress(), entry.getValue().getPort());
					}
				}
			}

			return new RegisterNodeReplyMsg(null, null, -1);
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
				throw new Exception("Unable to remove node. ID not found.");
			} else {
				//check if inet address is matching
				if(!nodes.get(removeArray).equals(nodeAddress)) {
					throw new Exception("Unable to remove node. Requesting node address does not match stored address.");
				} else {
					//remove node
					nodes.remove(removeArray);
				}
			}
		} finally {
			readWriteLock.writeLock().unlock();
		}
	}

	private class DiscoveryNodeWorker extends Thread{
		protected Socket socket;

		public DiscoveryNodeWorker(Socket socket, DiscoveryNode discoveryNode) {
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
						replyMsg = genNodeReplyMsg(); //generating before so we don't have to worry about the node we're adding
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

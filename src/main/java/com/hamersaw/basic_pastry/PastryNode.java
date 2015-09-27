package com.hamersaw.basic_pastry;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;
import java.util.Random;

import java.net.ServerSocket;
import java.net.Socket;

import com.hamersaw.basic_pastry.message.ErrorMsg;
import com.hamersaw.basic_pastry.message.Message;
import com.hamersaw.basic_pastry.message.RegisterNodeMsg;
import com.hamersaw.basic_pastry.message.RegisterNodeReplyMsg;

public class PastryNode extends Thread {
	private static final Logger LOGGER = Logger.getLogger(PastryNode.class.getCanonicalName());
	protected byte[] id;
	protected String discoveryNodeAddress;
	protected int discoveryNodePort, port;
	protected ReadWriteLock readWriteLock;

	public PastryNode(byte[] id, String discoveryNodeAddress, int discoveryNodePort, int port) {
		this.id = id;
		this.discoveryNodeAddress = discoveryNodeAddress;
		this.discoveryNodePort = discoveryNodePort;
		this.port = port;
		readWriteLock = new ReentrantReadWriteLock();
	}

	public static void main(String[] args) {
		try {
			String discoveryNodeAddress = args[0];
			int discoveryNodePort = Integer.parseInt(args[1]);
			int port = Integer.parseInt(args[2]);
			byte[] id = args.length == 4 ? HexConverter.convertHexToBytes(args[3]) : generateRandomID(2);

			new Thread(
				new PastryNode(id, discoveryNodeAddress, discoveryNodePort, port)
			).start();
		} catch(Exception e) {
			e.printStackTrace();
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
				case Message.REGISTER_NODE_REPLY_MSG:
					RegisterNodeReplyMsg registerNodeReplyMsg = (RegisterNodeReplyMsg) replyMsg;
					LOGGER.info("Recieved node info for '" + HexConverter.convertBytesToHex(registerNodeReplyMsg.getID()) + "' at '" + registerNodeReplyMsg.getInetAddress() + ":" + registerNodeReplyMsg.getPort() + "'");
					//TODO add the node to your cache
					success = true;
					break;
				case Message.SUCCESS_MSG:
					//if you're the first node registering a success messasge is sent back
					success = true;
					break;
				case Message.ERROR_MSG:
					LOGGER.severe(((ErrorMsg)replyMsg).getMsg());
					id = generateRandomID(2);
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

	private class PastryNodeWorker extends Thread{
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

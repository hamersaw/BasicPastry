package com.hamersaw.basic_pastry;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import java.net.ServerSocket;
import java.net.Socket;

import java.util.logging.Logger;
import java.util.Scanner;

import com.hamersaw.basic_pastry.message.ErrorMsg;
import com.hamersaw.basic_pastry.message.LookupNodeMsg;
import com.hamersaw.basic_pastry.message.Message;
import com.hamersaw.basic_pastry.message.NodeInfoMsg;
import com.hamersaw.basic_pastry.message.ReadDataMsg;
import com.hamersaw.basic_pastry.message.RequestRandomNodeMsg;
import com.hamersaw.basic_pastry.message.WriteDataMsg;

public class StoreData {
	private static final Logger LOGGER = Logger.getLogger(StoreData.class.getCanonicalName());

	public static void main(String[] args) {
		int port = 0;
		String discoveryNodeAddr = null;
		int discoveryNodePort = 0;

		try {   
			port = Integer.parseInt(args[0]);
			discoveryNodeAddr = args[1];
			discoveryNodePort = Integer.parseInt(args[2]);
		} catch(Exception e) {
			System.out.println("Usage: Client port controllerHosName controllerPort");
			System.exit(1);
		}

		String input = "";
		Scanner scanner = new Scanner(System.in);
		while(true) {
			try {
				System.out.print("Options\nS) Store File\nR) Retrieve File\nQ) Quit\nInput:");
				input = scanner.nextLine();

				if(input.equalsIgnoreCase("Q")) {
					break;
				} else if(!input.equalsIgnoreCase("S") && !input.equalsIgnoreCase("R")) {
					LOGGER.severe("Unknown input. Please reenter.");
					continue;
				}

				//read in the filename
				byte[] id = null;
				System.out.print("\tFilename:");
				File file = new File(scanner.nextLine());

				//read in id
				System.out.print("\tID (leave blank to auto generate one):");
				String idStr = scanner.nextLine();
				if(!idStr.equals("")) {
					id = HexConverter.convertHexToBytes(idStr);
				} else {
					short idValue = (short) (file.getName().hashCode() % (int)Short.MAX_VALUE);
					id = new byte[2];
					id[0] = (byte) (idValue & 0xff);
					id[1] = (byte) ((idValue >> 8) & 0xff);
				}

				//get random node
				NodeAddress seedNodeAddress = getRandomNode(discoveryNodeAddr, discoveryNodePort);

				//lookup id in cluster
				NodeAddress nodeAddress = lookupNode(id, seedNodeAddress, port);

				if(input.equalsIgnoreCase("S")) {
					if(!file.exists()) {
						throw new Exception("File '" + file.getName() + "' does not exist.");
					}

					//read data into buffer
					FileInputStream in = new FileInputStream(file);
					byte[] data = new byte[(int)file.length()];
					if(in.read(data) != data.length) {
						LOGGER.severe("Error reading data into buffer.");
						return;
					}

					in.close();

					//write data to the node
					WriteDataMsg writeDataMsg = new WriteDataMsg(id, data);
					Socket socket = new Socket(nodeAddress.getInetAddress(), nodeAddress.getPort());
					ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
					out.writeObject(writeDataMsg);

					socket.close();
					LOGGER.info("Sent data with id '" + HexConverter.convertBytesToHex(id) + "' to node " + nodeAddress + ".");
				} else if(input.equalsIgnoreCase("R")) {
					//read in storage directory
					System.out.print("\tStorage Directory:");
					file = new File(scanner.nextLine() + File.separator + file.getName());
					file.getParentFile().mkdirs();

					//request file from node
					ReadDataMsg readDataMsg = new ReadDataMsg(id);
					Socket socket = new Socket(nodeAddress.getInetAddress(), nodeAddress.getPort());
					ObjectOutputStream socketOut = new ObjectOutputStream(socket.getOutputStream());
					socketOut.writeObject(readDataMsg);

					//parse reply message
					Message replyMsg = (Message) new ObjectInputStream(socket.getInputStream()).readObject();
					socket.close();

					if(replyMsg.getMsgType() == Message.ERROR_MSG) {
						throw new Exception(((ErrorMsg)replyMsg).getMsg());
					} else if(replyMsg.getMsgType() != Message.WRITE_DATA_MSG) {
						throw new Exception("Recieved an unexpected message type '" + replyMsg.getMsgType() + "'.");
					}

					WriteDataMsg writeDataMsg = (WriteDataMsg) replyMsg;

					//write file contents
					FileOutputStream out = new FileOutputStream(file);
					for(byte b : writeDataMsg.getData()) {
						out.write(b);
					}

					out.close();
					LOGGER.info("Wrote data to file '" + file.getCanonicalPath() + "'.");
				} else if(!input.equalsIgnoreCase("Q")) {
					LOGGER.severe("Unknown input. Please reenter.");
				}
			} catch(Exception e) {
				e.printStackTrace();
				LOGGER.severe(e.getMessage());
			}
		}
	}

	public static NodeAddress getRandomNode(String discoveryNodeAddr, int discoveryNodePort) throws Exception {
		RequestRandomNodeMsg requestRandomNodeMsg = new RequestRandomNodeMsg();
		Socket socket = new Socket(discoveryNodeAddr, discoveryNodePort);
		ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
		out.writeObject(requestRandomNodeMsg);

		Message replyMsg = (Message) new ObjectInputStream(socket.getInputStream()).readObject();
		socket.close();

		//parse reply message
		if(replyMsg.getMsgType() == Message.ERROR_MSG) {
			throw new Exception(((ErrorMsg)replyMsg).getMsg());
		} else if(replyMsg.getMsgType() != Message.NODE_INFO_MSG) {
			throw new Exception("Recieved an unexpected message type '" + replyMsg.getMsgType() + "'.");
		}

		return ((NodeInfoMsg)replyMsg).getNodeAddress();
	}

	public static NodeAddress lookupNode(byte[] id, NodeAddress seedNodeAddress, int serverPort) throws Exception {
		//start up a server socket to accept the connection
		ServerSocket serverSocket = new ServerSocket(serverPort);

		//send store data message to random node
		LookupNodeMsg lookupNodeMsg = new LookupNodeMsg(id, new NodeAddress(null, serverPort), 0);
		lookupNodeMsg.addHop(seedNodeAddress);
		Socket seedSocket = new Socket(seedNodeAddress.getInetAddress(), seedNodeAddress.getPort());
		ObjectOutputStream seedOut = new ObjectOutputStream(seedSocket.getOutputStream());
		seedOut.writeObject(lookupNodeMsg);
	
		seedSocket.close();
		LOGGER.info("Sent lookup node message with id '" + HexConverter.convertBytesToHex(id) + "' to node " + seedNodeAddress);

		//receive connection on server socket from node where data should reside
		Socket nodeSocket = serverSocket.accept();
		Message replyMsg = (Message) new ObjectInputStream(nodeSocket.getInputStream()).readObject();

		//ensure we get a node info message
		if(replyMsg.getMsgType() == Message.ERROR_MSG) {
			throw new Exception(((ErrorMsg)replyMsg).getMsg());
		} else if(replyMsg.getMsgType() != Message.NODE_INFO_MSG) {
			throw new Exception("Recieved an unexpected message type '" + replyMsg.getMsgType() + "'.");
		}

		//parse reply message
		NodeInfoMsg nodeInfoMsg = (NodeInfoMsg) replyMsg;
		if(nodeInfoMsg.getNodeAddress().getInetAddress() == null) {
			nodeInfoMsg.getNodeAddress().setInetAddress(nodeSocket.getInetAddress());
		}

		serverSocket.close();
		nodeSocket.close();
		return nodeInfoMsg.getNodeAddress();
	}
}

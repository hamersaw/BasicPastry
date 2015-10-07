package com.hamersaw.basic_pastry;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import java.net.ServerSocket;
import java.net.Socket;

import java.util.logging.Logger;
import java.util.Scanner;

import com.hamersaw.basic_pastry.message.ErrorMsg;
import com.hamersaw.basic_pastry.message.Message;
import com.hamersaw.basic_pastry.message.NodeInfoMsg;
import com.hamersaw.basic_pastry.message.RequestRandomNodeMsg;
import com.hamersaw.basic_pastry.message.LookupNodeMsg;

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
		while(!input.equalsIgnoreCase("q")) {
			try {
				System.out.print("Options\nS) Store File\nR) Retrieve File\nQ) Quit\nInput:");
				input = scanner.nextLine();

				if(input.equalsIgnoreCase("S")) {
					//read in filename
					String filename;
					FileInputStream fileInputStream;
					long dataLength;
					byte[] id = null;
					try {
						//read in the filename
						System.out.print("\tFilename:");
						filename = scanner.nextLine();
						File file = new File(filename);
						dataLength = file.length();
						fileInputStream = new FileInputStream(file);

						//get id
						System.out.print("\tID (leave blank to auto generate one):");
						String idStr = scanner.nextLine();
						if(!idStr.equals("")) {
							id = HexConverter.convertHexToBytes(idStr);
						}
					} catch(FileNotFoundException e) {
						System.out.println("File not found.");
						continue;
					} catch(Exception e) {
						e.printStackTrace();
						continue;
					}

					//if id not supplied generate one	
					if(id == null) {
						//TODO generate id based on file hash (name, content, etc)?
					}

					//contact discovery node to get a random node to start with
					RequestRandomNodeMsg requestRandomNodeMsg = new RequestRandomNodeMsg();
					Socket socket = new Socket(discoveryNodeAddr, discoveryNodePort);
					ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
					out.writeObject(requestRandomNodeMsg);

					ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
					Message replyMsg = (Message) in.readObject();
					socket.close();

					//parse reply message
					if(replyMsg.getMsgType() == Message.ERROR_MSG) {
						LOGGER.severe(((ErrorMsg)replyMsg).getMsg());
						return;
					} else if(replyMsg.getMsgType() != Message.NODE_INFO_MSG) {
						LOGGER.severe("Recieved an unexpected message type '" + replyMsg.getMsgType() + "'.");
						return;
					}

					//start up a server socket to accept the connection
					ServerSocket serverSocket = new ServerSocket(port);

					//send store data message to random node
					NodeInfoMsg nodeInfoMsg = (NodeInfoMsg) replyMsg;
					LookupNodeMsg lookupNodeMsg = new LookupNodeMsg(id, filename, new NodeAddress(null, port), 0);
					Socket nodeSocket = new Socket(nodeInfoMsg.getNodeAddress().getInetAddress(), nodeInfoMsg.getNodeAddress().getPort());
					ObjectOutputStream nodeOut = new ObjectOutputStream(nodeSocket.getOutputStream());
					nodeOut.writeObject(lookupNodeMsg);
				
					nodeSocket.close();

					//receive connection on server socket from node where data should reside
					nodeSocket = serverSocket.accept();
					in = new ObjectInputStream(nodeSocket.getInputStream());
					Message replyMsg = (Message) in.readObject();

					//ensure we get a success message
					if(replyMsg.getMsgType() == Message.ERROR_MSG) {
						LOGGER.severe(((ErrorMsg)replyMsg).getMsg());
						return;
					} else if(replyMsg.getMsgType() != Message.SUCCESS_MSG) {
						LOGGER.severe("Recieved an unexpected message type '" + replyMsg.getMsgType() + "'.");
						return;
					}

					//send parts of the file in different chunks
					LOGGER.info("TODO send file data over");

					System.out.println("Store File not yet implemented");
				} else if(input.equalsIgnoreCase("R")) {
					System.out.println("Retrieve File not yet implemented");
				} else if(!input.equalsIgnoreCase("Q")) {
					System.out.println("Unknown input. Please reenter.");
				}
			} catch(Exception e) {
				LOGGER.severe(e.getMessage());
			}
		}
	}
}

package client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;
import java.net.SocketTimeoutException;

import org.apache.log4j.Logger;

import client.KVCommInterface;
import common.messages.KVMessage;
import common.messages.MessageType;
import common.messages.KVAdminMessage;

public class Client {

	private Logger logger = Logger.getRootLogger();
	private Set<KVCommInterface> listeners;
	private boolean running;
	
	private Socket clientSocket;
	private OutputStream output;
 	private InputStream input;
	
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024 * BUFFER_SIZE;
	
	
	public Client(String address, int port) 
			throws UnknownHostException, IOException, ConnectException{

		clientSocket = new Socket(address, port);
		// For now set timeout to be large so transfers can go through
		clientSocket.setSoTimeout(1000);
		listeners = new HashSet<KVCommInterface>();
		setRunning(true);
		logger.debug("Connection established");
		output = clientSocket.getOutputStream();
		input = clientSocket.getInputStream();
	}
	
	// New constructor for custom timeout
	public Client(String address, int port, int timeout) 
			throws UnknownHostException, IOException, ConnectException{

		clientSocket = new Socket(address, port);
		// For now set timeout to be large so transfers can go through
		clientSocket.setSoTimeout(timeout);
		listeners = new HashSet<KVCommInterface>();
		setRunning(true);
		logger.debug("Connection established");
		output = clientSocket.getOutputStream();
		input = clientSocket.getInputStream();
	}
	
	public int soTimeout() {
		try {
			return clientSocket.getSoTimeout();
		} catch (Exception e) {
			return -1;
		}
	}
	
	// Use client.logInfo("asdf") to log information
	public void logInfo(String input){
		logger.info(input);
		return;
	}
	
	// Use client.logError("asdf") to log errors
	public void logError(String input){
		logger.error(input);
		return;
	}
	
	public KVMessage getResponse(){
		KVMessage response = new MessageType("", "getResponse_NOT_PROCESSED", "", "");
		if (isRunning()) {
			try {
				response = receiveMessage();
				
			} catch (IOException ioe) {
				if(isRunning()) {
					System.out.println("Error:> "+ioe.getMessage());
					logger.error("Connection lost!");
					closeConnection();
					return new MessageType("", "TIME_OUT", "", "");
				}
			}				
		}
		return response;
	}
	
	public synchronized void closeConnection() {
		logger.debug("try to close connection ...");
		try {
			tearDownConnection();
		} catch (IOException ioe) {
			logger.error("Unable to close connection!");
		}
	}
	
	private void tearDownConnection() throws IOException {
		setRunning(false);
		logger.debug("tearing down the connection ...");
		if (clientSocket != null) {
			input.close();
			output.close();
			clientSocket.close();
			clientSocket = null;
			logger.debug("connection closed!");
		}
	}
	
	public boolean isRunning() {
		return running;
	}
	
	public void setRunning(boolean run) {
		running = run;
	}
	
	public void addListener(KVCommInterface listener){
		listeners.add(listener);
	}
	
	/**
	 * Method sends a KVMessage using this socket.
	 * @param msg the message that is to be sent.
	 * @throws IOException some I/O error regarding the output stream 
	 */
	public void sendMessage(KVMessage msg) throws IOException {
		byte[] msgBytes = msg.getMsgBytes();
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		logger.debug("Send message:\t '" + msg.getMsg() + "'");
    }
	
	
	private KVMessage receiveMessage() throws IOException {
		//TODO: implement FAILED handling from server
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];
		
		/* read first char from stream */
		byte read = (byte) input.read();	
		boolean reading = true;
		
		while(read != 10 && reading) {/* carriage return */
			/* if buffer filled, copy to msg array */
			if(index == BUFFER_SIZE) {
				if(msgBytes == null){
					tmp = new byte[BUFFER_SIZE];
					System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
				} else {
					tmp = new byte[msgBytes.length + BUFFER_SIZE];
					System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
					System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
							BUFFER_SIZE);
				}

				msgBytes = tmp;
				bufferBytes = new byte[BUFFER_SIZE];
				index = 0;
			} 
			
			/* only read valid characters, i.e. letters and numbers */
			if((read > 31 && read < 127)) {
				bufferBytes[index] = read;
				index++;
			}
			
			/* stop reading is DROP_SIZE is reached */
			if(msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
				reading = false;
			}
			
			/* read next char from stream */
			read = (byte) input.read();
		}
		
		if(msgBytes == null){
			tmp = new byte[index];
			System.arraycopy(bufferBytes, 0, tmp, 0, index);
		} else {
			tmp = new byte[msgBytes.length + index];
			System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
			System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
		}
		
		msgBytes = tmp;
		
		/* build final String */
		KVMessage msg = new KVAdminMessage(msgBytes); //reply from server should include status
		if (msg.getError() != null){
			logger.error("Client: Received invalid message from server: "+msg.getMsg());
			logger.error(msg.getError());
		}
		logger.debug("Receive message:\t '" + msg.getMsg() + "'");
		return msg;
    }
 	
}
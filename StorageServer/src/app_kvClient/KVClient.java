package app_kvClient;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.net.ConnectException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import logger.LogSetup;

import client.KVStore;
import common.messages.KVMessage;
import common.messages.MessageType;

public class KVClient {

	private static Logger logger = Logger.getRootLogger();
	private static final String PROMPT = "StorageServiceClient> ";
	private BufferedReader stdin;
	private boolean stop = false;
	
	private String serverAddress;
	private int serverPort;
	private KVStore kvstore = null;
	
	public void run() {
		while(!stop) {
			stdin = new BufferedReader(new InputStreamReader(System.in));
			System.out.print(PROMPT);
			
			try {
				String cmdLine = stdin.readLine();
				this.handleCommand(cmdLine);
			} catch (IOException e) {
				stop = true;
				printError("CLI does not respond - Application terminated ");
			}
		}
	}

	public void handleCommand(String cmdLine) {
		//parse cmdLine by spaces	
		if (cmdLine.trim().length() == 0){
			//ignore empty commands
			return;
		}
		
		// Parses the input this way so we allow multiple spaces between command and key and value. AKA "  put   foo   bar 1 2 3 " is equivalent to "put foo bar 1 2 3 "
		String header=" ", status=" ", key=" ", value=" ";
		cmdLine = cmdLine.replaceAll("^\\s+", "");
		if(cmdLine.indexOf(' ') > -1) {
			header = cmdLine.substring(0, cmdLine.indexOf(' '));
			String sub0 = cmdLine.substring(cmdLine.indexOf(' ')+1);
			sub0 = sub0.replaceAll("^\\s+", "");
			if(sub0.indexOf(' ') > -1) {
				key = sub0.substring(0, sub0.indexOf(' '));
				value = sub0.substring(sub0.indexOf(' ')+1);
				value = value.replaceAll("^\\s+", "");
			} else {
				key = sub0.trim();
			}
		} else {
			header = cmdLine.trim();
		}

		MessageType msg = new MessageType(header, status, key, value);
		
		if (msg.error != null){
			printError((msg.error));
		}
		else{
			switch (msg.getHeader()) {
			case "connect":
				try{
					serverAddress = msg.getKey();
					serverPort = Integer.parseInt(msg.getValue());
					kvstore = new KVStore(serverAddress, serverPort);
					kvstore.connect();
					System.out.println("Connection successful!");
				} catch(NumberFormatException nfe) {
					printError("No valid address. Port must be a number!");
					logger.warn("Unable to parse argument <port>");
				} catch (ConnectException e) {
					printError("Connection refused by host! (Try a different port number)");
					logger.warn("Connection refused!");
				} catch (UnknownHostException e) {
					printError("Unknown Host!");
					logger.warn("Unknown Host!");
				} catch (IOException e) {
					printError("Could not establish connection!");
					logger.warn("Could not establish connection!");
				} catch (IllegalArgumentException nfe) {
					printError("Port must be between 1 and 65535!");
					logger.warn("Input port out of range.");
				} 
				break;
			case "disconnect":
				if (kvstore != null){
					kvstore.disconnect();
					kvstore = null;
				}
				else{
					printError("Not connected!");
				}
				break;
			case "put":
				if (kvstore != null){
					try{
						KVMessage put_result = kvstore.put(msg.getKey(), msg.getValue());
						if(put_result.getStatus().equals("PUT_SUCCESS")) {
							// Put successful
							System.out.println("Put successful!");
							System.out.println("Key: " + put_result.getKey());
							System.out.println("Value: " + put_result.getValue());
						} else if(put_result.getStatus().equals("PUT_UPDATE")) {
							// Put update
							System.out.println("Updated old value!");
							System.out.println("Key: " + put_result.getKey());
							System.out.println("New Value: " + put_result.getValue());
						} else if(put_result.getStatus().equals("PUT_ERROR")) {
							// Put error
							System.out.println("ERROR processing put!");
							System.out.println("Key: " + put_result.getKey());
							System.out.println("Value: " + put_result.getValue());
							logger.warn("Put error");
						} else if(put_result.getStatus().equals("DELETE_SUCCESS")) {
							// Delete
							System.out.println("Deleting value in key!");
							System.out.println("Key: " + put_result.getKey());
						} else if(put_result.getStatus().equals("DELETE_ERROR")) {
							// Delete error
							System.out.println("ERROR deleting value in key!");
							System.out.println("Key: " + put_result.getKey());
							logger.warn("Delete error");
						} else {
							// Problem with store or server, unknown status to the client
							System.out.println("Unknown return status!");
							System.out.println("Key: " + put_result.getKey());
							System.out.println("Value: " + put_result.getValue());
							logger.warn("Client cannot interpret status message " + put_result.getStatus());
						}
					}
					catch (Exception e){
						printError("Client: put exception");
					}
				}
				else{
					printError("Not connected!");
				}
				break;
			case "get":
				if (kvstore != null){
					try{
						System.out.println(Integer.toString(kvstore.soTimeout()));
						KVMessage get_result = kvstore.get(msg.getKey());
						if(get_result.getStatus().equals("GET_SUCCESS")) {
							// Get successful
							System.out.println("Get successful!");
							System.out.println("Key: " + get_result.getKey());
							System.out.println("Value: " + get_result.getValue());
						} else if(get_result.getStatus().equals("GET_ERROR")) {
							// Get error
							System.out.println("ERROR processing get!");
							System.out.println("Key: " + get_result.getKey());
							logger.warn("Get error");
						} else {
							// Problem with store or server, unknown status to the client
							System.out.println("Unknown return status!");
							System.out.println("Key: " + get_result.getKey());
							logger.warn("Client cannot interpret status message " + get_result.getStatus());
						}
					}
					catch (Exception e){
						printError("Client: get exception");
					}
				}
				else{
					printError("Not connected!");
				}
				break;
			case "logLevel":
				String level = setLevel(msg.getKey());
				if(level.equals(LogSetup.UNKNOWN_LEVEL)) {
					printError("No valid log level!");
					printPossibleLogLevels();
				} else {
					System.out.println(PROMPT + 
							"Log level changed to level " + level);
				}
				break;
			case "help":
				printHelp();
				break;
			case "quit":
				stop = true;
				if (kvstore != null){
					kvstore.disconnect();
					kvstore = null;
				}
				System.out.println(PROMPT + "Application exit!");
				break;
			}
		}
	}

	
	private void printHelp() {
		StringBuilder sb = new StringBuilder();
		sb.append(PROMPT).append("STORAGE SERVICE CLIENT HELP (Usage):\n");
		sb.append(PROMPT);
		sb.append("::::::::::::::::::::::::::::::::");
		sb.append("::::::::::::::::::::::::::::::::\n");
		sb.append(PROMPT).append("connect <host> <port>");
		sb.append("\t establishes a connection to a server\n");
		sb.append(PROMPT).append("get <key>");
		sb.append("\t\t sends a get request for key to the server \n");
		sb.append(PROMPT).append("put <key> <value>");
		sb.append("\t\t sends a put request for (key,value) to the server \n");
		sb.append(PROMPT).append("disconnect");
		sb.append("\t\t\t disconnects from the server \n");
		
		sb.append(PROMPT).append("logLevel");
		sb.append("\t\t\t changes the logLevel \n");
		sb.append(PROMPT).append("\t\t\t\t ");
		sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");
		
		sb.append(PROMPT).append("quit ");
		sb.append("\t\t\t exits the program");
		System.out.println(sb.toString());
	}
	
	private void printPossibleLogLevels() {
		System.out.println(PROMPT 
				+ "Possible log levels are:");
		System.out.println(PROMPT 
				+ "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
	}

	private String setLevel(String levelString) {
		
		if(levelString.equals(Level.ALL.toString())) {
			logger.setLevel(Level.ALL);
			return Level.ALL.toString();
		} else if(levelString.equals(Level.DEBUG.toString())) {
			logger.setLevel(Level.DEBUG);
			return Level.DEBUG.toString();
		} else if(levelString.equals(Level.INFO.toString())) {
			logger.setLevel(Level.INFO);
			return Level.INFO.toString();
		} else if(levelString.equals(Level.WARN.toString())) {
			logger.setLevel(Level.WARN);
			return Level.WARN.toString();
		} else if(levelString.equals(Level.ERROR.toString())) {
			logger.setLevel(Level.ERROR);
			return Level.ERROR.toString();
		} else if(levelString.equals(Level.FATAL.toString())) {
			logger.setLevel(Level.FATAL);
			return Level.FATAL.toString();
		} else if(levelString.equals(Level.OFF.toString())) {
			logger.setLevel(Level.OFF);
			return Level.OFF.toString();
		} else {
			return LogSetup.UNKNOWN_LEVEL;
		}
	}

	private void printError(String error){
		System.out.println(PROMPT + "Error! " +  error);
	}
	
    /**
     * Main entry point for the echo server application. 
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {
    	try {
			new LogSetup("logs/client.log", Level.WARN);
			KVClient app = new KVClient();
			app.run();
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		}
    }
}

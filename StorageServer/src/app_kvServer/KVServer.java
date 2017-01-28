package app_kvServer;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
// Remove this because already imported below
// import java.io.IOException;

// Import Libs for HashMaps and other useful things
/*
 HashMap Class Methods

 void clear(): It removes all the key and value pairs from the specified Map.
 Object clone(): It returns a copy of all the mappings of a map and used for cloning them into another map.
 boolean containsKey(Object key): It is a boolean function which returns true or false based on whether the specified key is found in the map.
 boolean containsValue(Object Value): Similar to containsKey() method, however it looks for the specified value instead of key.
 Value get(Object key): It returns the value for the specified key.
 boolean isEmpty(): It checks whether the map is empty. If there are no key-value mapping present in the map then this function returns true else false.
 Set keySet(): It returns the Set of the keys fetched from the map.
 value put(Key k, Value v): Inserts key value mapping into the map.
 int size(): Returns the size of the map, Number of key-value mappings.
 Collection values(): It returns a collection of values of map.
 Value remove(Object key): It removes the key-value pair for the specified key. Used in the above example.
 void putAll(Map m): Copies all the elements of a map to the another specified map.
 */
import java.util.*;
import java.lang.*;
import java.io.*;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.messages.MessageType;

import app_kvServer.ClientConnection;
/**
 * Represents a simple Echo Server implementation.
 */
public class KVServer extends Thread {
	// I will leave some var names as name instead of m_name since they were given in the skeleton code and I don't want to break stuff
	private static Logger logger = Logger.getRootLogger();
	private int port;
	// Add private variables for storing the cache size and caching strategy inside the Server
	private int m_cacheSize;
	private String m_strategy;
	// Add private variable to keep track of number of entries currently in the cache
	private int m_currentCacheEntries;
	// Add private variable to keep track of number of entries currently in the hard disk storage file (Might be useful later on)
	private int m_currentHardDiskEntries;
	// This string stores the harddisk file path
	private String m_hardDiskFilePath;
	// This string stores the harddisk file name
	private String m_hardDiskFileName;
	// This variable tells whether at the beginning of execution a harddisk file exists
	private boolean m_hardDiskFileExists;
	// This is the File Instance for the hardDisk File
	private File m_hardDiskFileInstance;
	// This Writer is responsible for Writing to the harddisk file
	private PrintWriter m_hardDiskFileWriter;
	// This Reader is responsible for Reading the harddisk file
	private BufferedReader m_hardDiskFileReader;

	// Create three maps for cache and one map for harddisk file
	// This map stores the cache key pairs with key, number
	Map<String, Integer> m_cacheNumberMap;
	// This map stores the cache key pairs with key, value
	Map<String, String> m_cacheValueMap;
	// This map stores the cache key pairs with key, times used for LFU
	Map<String, Integer> m_cacheFrequencyMap;
	// This map stores the harddisk file key pairs with key, value
	Map<String, String> m_hardDiskValueMap;

	private ServerSocket serverSocket;
	private boolean running;

	/**
	 * Constructs a (Echo-) Server object which listens to connection attempts 
	 * at the given port.
	 * 
	 * @param port a port number which the Server is listening to in order to 
	 * establish a socket connection to a client. The port number should 
	 * reside in the range of dynamic ports, i.e 49152 - 65535.
	 */
	/**
	 * Start KV Server at given port
	 * @param port given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed 
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache 
	 *           is full and there is a GET- or PUT-request on a key that is 
	 *           currently not contained in the cache. Options are "FIFO", "LRU", 
	 *           and "LFU".
	 */
	public KVServer(int port, int cacheSize, String strategy) {

		// Initialize the private variables of the server object
		System.out.println("Initializing Server Variables");
		this.port = port;
		this.m_cacheSize = cacheSize;
		this.m_strategy = strategy;
		this.m_currentCacheEntries = 0;
		this.m_currentHardDiskEntries = 0;

		// Initialize the maps for the server object
		System.out.println("Initializing Server Maps");
		this.m_cacheNumberMap = new HashMap<String, Integer>();
		this.m_cacheValueMap = new HashMap<String, String>();
		this.m_cacheFrequencyMap = new HashMap<String, Integer>();
		this.m_hardDiskValueMap = new HashMap<String, String>();

		// Initialize harddisk file information
		System.out.println("Initializing Hard Disk File Variables");
		// Get where the program is running from (where the project is)
		this.m_hardDiskFilePath = System.getProperty("user.dir");
		// HardCode file name to be storage.txt
		this.m_hardDiskFileName = "storage.txt";
		// Output harddisk file location and name for debugging
		System.out.println("HardDiskFile Name is : " + m_hardDiskFileName + " HardDiskFile Path is : " + m_hardDiskFilePath);
		// Initialize the harddisk File Instance
		m_hardDiskFileInstance = new File(m_hardDiskFilePath + m_hardDiskFileName);
		// Initialize a new harddisk file for writing or see if it already exists
		try {
			m_hardDiskFileExists = m_hardDiskFileInstance.createNewFile();
		} catch (IOException e) {
			System.out.println("Error when trying to initialize file instance");
			e.printStackTrace();
		}
		if (m_hardDiskFileExists) {
			System.out.println("Hard disk File already exists");
		} else {
			System.out.println("Created New Hard disk File");
		}
		// Initialize the file writer to null, assign when we write
		// m_hardDiskFileWriter = new PrintWriter(m_hardDiskFileInstance);
		m_hardDiskFileWriter = null;
		// Initialize the file reader to null, assign when we read
		m_hardDiskFileReader = null;

		// Start the server object
		System.out.println("Starting Server");
		this.start();
	}

	// This function is used to rewrite the harddisk file with the key value pairs from the harddisk map
	// Returns boolean, true for success, false for failure
	public boolean overwriteHardDiskFile() throws IOException {
		try {
			System.out.println("Starting to overwrite Hard disk File from Hard Disc Map");
			// Initialize the file writer with our file
			this.m_hardDiskFileWriter = new PrintWriter(this.m_hardDiskFileInstance);
			int linesWritten = 0;
			// Iterate through all pairs of hard disk map
			for ( Map.Entry<String, String> iteratorDummy : this.m_hardDiskValueMap.entrySet()) {
				// For each key/value pair, print it and write it to the hard disc File as key first then value each on its own line
				System.out.println("Inserting Key: " + iteratorDummy.getKey());
				this.m_hardDiskFileWriter.println(iteratorDummy.getKey());
				System.out.println("Inserting Value: " + iteratorDummy.getValue());
				this.m_hardDiskFileWriter.println(iteratorDummy.getValue());
				linesWritten = linesWritten + 2;
			}
			// Close the Writer when finished and return
			this.m_hardDiskFileWriter.close();
			return true;
		} catch (Exception ex) {
			System.out.println("Encountered Error while trying to overwrite hard disc map");
			System.out.println(ex.getMessage());
			this.m_hardDiskFileWriter.close();
			return false;
		}
	}
	// This function is used to repopulate the harddisk map with the key value pairs from the harddisk file
	// Returns boolean, true for success, false for failure
	public boolean repopulateHardDiskFile() throws IOException {
		try {
			System.out.println("Starting to repopulate Hard disk Map from Hard Disc File");
			// Initialize the file reader with our file
			this.m_hardDiskFileReader = new BufferedReader(new FileReader(this.m_hardDiskFileInstance));
			String CurrentLine;
			String Key = null;
			String Value = null;
			int counter = 0;
			// Read lines until end of file
			while ((CurrentLine = this.m_hardDiskFileReader.readLine()) != null) {
				// For each line read, print it and add it to the hard disc map as key first then value
				if (counter % 2 == 0) {
					// If counter is even the line is a key
					System.out.println("Key is: " + CurrentLine);
					Key = CurrentLine;
				} else {
					// If counter is odd the line is a value and add it to the map
					System.out.println("Value is: " + CurrentLine);
					Value = CurrentLine;
					this.m_hardDiskValueMap.put(Key, Value);
				}
				// Increment the counter after every line is read
				counter = counter + 1;
			}
			// Set map size and close
			this.m_currentHardDiskEntries = this.m_hardDiskValueMap.size();
			this.m_hardDiskFileReader.close();
			return true;
		} catch (Exception ex) {
			System.out.println("Encountered Error while trying to repopulate hard disc map");
			System.out.println(ex.getMessage());
			this.m_hardDiskFileReader.close();
			// Reset harddisk Cache 
			this.m_hardDiskValueMap.clear();
			return false;
		}
	}
	// This function is the entry point for handling a client message, at this point the message is valid, first called in ClientConnection
	public common.messages.KVMessage handleClientMessage(common.messages.KVMessage msg) {
		String Header = msg.getHeader();
		common.messages.KVMessage returnMsg;
		boolean success = true;
		// Decide on the appropriate handler based on what the client message was through the use of a switch statement
		switch (Header) {
		case "connect": 
			returnMsg = handleConnect(msg);
			break;
		case "disconnect":
			returnMsg = handleDisconnect(msg);
			break;
		case "put":
			returnMsg = handlePut(msg);
			break;
		case "get":
			returnMsg = handleHelp(msg);
			break;
		case "logLevel":
			returnMsg = handleLogLevel(msg);
			break;
		case "help":
			returnMsg = handleHelp(msg);
			break;
		case "quit":
			returnMsg = handleQuit(msg);
			break;
		default:
			return returnMsg = new common.messages.MessageType(" ", " ", " ", " ");
		}
		return returnMsg;
	}
	// This function is used to handle a client connect request
	public common.messages.KVMessage handleConnect(common.messages.KVMessage msg) {
		System.out.println("Handling Connect");
		common.messages.KVMessage returnMsg = new common.messages.MessageType(" ", " ", " ", " "); ;
		return returnMsg;
	}
	// This function is used to handle a client disconnect request
	public common.messages.KVMessage handleDisconnect(common.messages.KVMessage msg) {
		System.out.println("Handling Disconnect");
		common.messages.KVMessage returnMsg = new common.messages.MessageType(" ", " ", " ", " "); ;
		return returnMsg;
	}
	// This function is used to handle a client log level change request
	public common.messages.KVMessage handleLogLevel(common.messages.KVMessage msg) {
		System.out.println("Handling Log Level");
		common.messages.KVMessage returnMsg = new common.messages.MessageType(" ", " ", " ", " "); ;
		return returnMsg;
	}
	// This function is used to handle a client help message
	public common.messages.KVMessage handleHelp(common.messages.KVMessage msg) {
		System.out.println("Handling Help");
		common.messages.KVMessage returnMsg = new common.messages.MessageType(" ", " ", " ", " "); ;
		return returnMsg;
	}
	// This function is used to handle a client quit message
	public common.messages.KVMessage handleQuit(common.messages.KVMessage msg) {
		System.out.println("Handling Quit");
		common.messages.KVMessage returnMsg = new common.messages.MessageType(" ", " ", " ", " "); ;
		return returnMsg;
	}
	// This function is used to handle a client put request
	public common.messages.KVMessage handlePut(common.messages.KVMessage msg) {
		System.out.println("Handling Put");
		common.messages.KVMessage returnMsg = new common.messages.MessageType(" ", " ", " ", " "); ;
		return returnMsg;
	}
	// This function is used to handle a client get request
	public common.messages.KVMessage handleGet(common.messages.KVMessage msg) {
		System.out.println("Handling Get");
		common.messages.KVMessage returnMsg = new common.messages.MessageType(" ", " ", " ", " "); ;
		return returnMsg;
	}
	// This function is used to put key value pair into the cache
	public boolean insertIntoCache(String key, String value) {
		// When we call this function we know Cache is already Full
		System.out.println("Inserting into Cache Key: " + key + " Value: " + value);
		return true;
	}
	// This function is used to evict a key value pair according to FIFO
	public boolean evictFIFO(String key, String value) {
		// When we call this function we know Cache is already Full
		System.out.println("Evicting using FIFO from Cache From inserting Key: " + key + " Value: " + value);
		return true;
	}
	// This function is used to evict a key value pair according to LRU
	public boolean evictLRU(String key, String value) {
		// When we call this function we know Cache is already Full
		System.out.println("Evicting using LRU from Cache From inserting Key: " + key + " Value: " + value);
		return true;
	}
	// This function is used to evict a key value pair according to LFU
	public boolean evictLFU(String key, String value) {
		// When we call this function we know Cache is already Full
		System.out.println("Evicting using LFU from Cache From inserting Key: " + key + " Value: " + value);
		return true;
	}
	/**
	 * Initializes and starts the server. 
	 * Loops until the the server should be closed.
	 */
	public void run() {

		running = initializeServer();

		if(serverSocket != null) {
			while(isRunning()){
				try {
					Socket client = serverSocket.accept();                
					// pass a pointer reference of the server to each client socket instance so they can use operations on the storage database
					ClientConnection connection = 
							new ClientConnection(client,this);
					new Thread(connection).start();

					logger.info("Connected to " 
							+ client.getInetAddress().getHostName() 
							+  " on port " + client.getPort());
				} catch (IOException e) {
					logger.error("Error! " +
							"Unable to establish connection. \n", e);
				}
			}
		}
		logger.info("Server stopped.");
	}

	private boolean isRunning() {
		return this.running;
	}

	/**
	 * Stops the server insofar that it won't listen at the given port any more.
	 */
	public void stopServer(){
		running = false;
		try {
			serverSocket.close();
		} catch (IOException e) {
			logger.error("Error! " +
					"Unable to close socket on port: " + port, e);
		}
	}

	private boolean initializeServer() {
		logger.info("Initialize server ...");
		try {
			serverSocket = new ServerSocket(port);
			logger.info("Server listening on port: " 
					+ serverSocket.getLocalPort());    
			return true;

		} catch (IOException e) {
			logger.error("Error! Cannot open server socket:");
			if(e instanceof BindException){
				logger.error("Port " + port + " is already bound!");
			}
			return false;
		}
	}

	/**
	 * Main entry point for the echo server application. 
	 * @param args contains the port number at args[0].
	 */
	public static void main(String[] args) {
		try {
			new LogSetup("logs/server.log", Level.ALL);
			// change the amount of commandline arguments to be parsed into 3
			// first one is port, second is cache size, third one is strategy
			if(args.length != 3) {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: Server <int port> <int cacheSize> <string strategy: FIFO, LRU or LFU>!");
			} else {
				int port = Integer.parseInt(args[0]);
				int cacheSize = Integer.parseInt(args[1]);
				String strategy = args[2];
				// Handle invalid input for server strategy argument
				if (!strategy.equals("FIFO") && !strategy.equals("FIFO") && !strategy.equals("FIFO")) {
					System.out.println("Error! strategy argument invalid!");
					System.out.println("Usage: Server <int port> <int cacheSize> <string strategy: FIFO, LRU or LFU>! Please try again");
					System.exit(0);
				}
				new KVServer(port, cacheSize, strategy).start();
			}
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		} catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid argument <port>! Not a number!");
			System.out.println("Usage: Server <port>!");
			System.exit(1);
		}
	}
}

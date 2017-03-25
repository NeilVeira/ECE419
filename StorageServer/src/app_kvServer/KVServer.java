package app_kvServer;
import java.net.BindException;
import java.net.SocketException;
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
import java.math.BigInteger;
import java.io.*;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.HashRing;
import common.HashRing.Server;
import common.messages.*;
import app_kvServer.ClientConnection;
import client.Client;

/**
 * Represents a simple Echo Server implementation.
 */
public class KVServer extends Thread {
	public enum ServerStatus {
		ACTIVE, 		/* Processes client requests */
		STOPPED,		/* Does not process client requests. Note that it's still "running" in that it still listens on the socket. */
		WRITE_LOCKED	/* Only processes get requests */
	}
	ServerStatus status;
	
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
	// This is the lock object for ensuring multiple clientconnections don't access hard disk entries at the same time 
	private Object m_myLock;

	// Create three maps for cache and one map for harddisk file
	// This map stores the cache key pairs with key, value
	Map<String, String> m_cacheValueMap;
	// This map stores the cache key  for FIFO
	LinkedList<String> m_cacheFIFOList;
	// This map stores the cache  LRU
	LinkedList<String> m_cacheLRUList;
	// This map stores the cache key pairs with key, times used for LFU
	Map<String, Integer> m_cacheLFUMap;

	// This map stores the harddisk file key pairs with key, value
	Map<String, String> m_hardDiskValueMap;

	private ServerSocket serverSocket;
	private boolean running;
	
	private HashRing metadata;
	private int id;

	/**
	 * Constructs a KVServer object which listens to connection attempts 
	 * at the given port. This constructor does all the initialization and starts 
	 * the server running.
	 * 
	 * @param port a port number which the Server is listening to in order to 
	 * 		establish a socket connection to a client. The port number should 
	 * 		reside in the range of dynamic ports, i.e 49152 - 65535.
	 * @param cacheSize specifies how many key-value pairs the server is allowed 
 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache 
 *           is full and there is a GET- or PUT-request on a key that is 
 *           currently not contained in the cache. Options are "FIFO", "LRU", 
 *           and "LFU".
	 * @param id identifier for this server. It will use a different hard disk file name
 * 			 based on this integer.            		
	 */
	public KVServer(int port, int cacheSize, String strategy, int id) {
		// Initialize the private variables of the server object
		System.out.println("Initializing Server Variables");
		logger.info("Initializing Server Variables");
		this.port = port;
		this.id = id;
		
		// Initialize harddisk file information
		this.m_hardDiskFileName = "storage_" + id + ".txt";
		System.out.println("Initializing Hard Disk File Variables");
		logger.info("Initializing Hard Disk File Variables");
		// Get where the program is running from (where the project is)
		this.m_hardDiskFilePath = System.getProperty("user.dir");
		// Output harddisk file location and name for debugging
		System.out.println("HardDiskFile Name is : " + m_hardDiskFileName + " HardDiskFile Path is : " + m_hardDiskFilePath);
		logger.info("HardDiskFile Name is : " + m_hardDiskFileName + " HardDiskFile Path is : " + m_hardDiskFilePath);
		// Initialize the harddisk File Instance
		m_hardDiskFileInstance = new File(m_hardDiskFilePath + "/" + m_hardDiskFileName);
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
		
		//For now always create the server in the stopped state initially. May change this later.
		this.status = ServerStatus.STOPPED;
		initKVServer("", cacheSize, strategy);
	}
	
	/**
	 * Construct a KVServer with only a port and id. This is used for constructing a KVServer
	 * without initializing the cache attributes and without starting it. The server will be
	 * in the STOPPED state which is unable to service client requests. 
	 */
	public KVServer(int port, int id) {
		this.port = port;
		this.id = id;
		this.m_hardDiskFileName = "storage_" + id + ".txt";
		// Initialize harddisk file information
		System.out.println("Initializing Hard Disk File Variables");
		logger.info("Initializing Hard Disk File Variables");
		// Get where the program is running from (where the project is)
		this.m_hardDiskFilePath = System.getProperty("user.dir");
		// Output harddisk file location and name for debugging
		System.out.println("HardDiskFile Name is : " + m_hardDiskFileName + " HardDiskFile Path is : " + m_hardDiskFilePath);
		logger.info("HardDiskFile Name is : " + m_hardDiskFileName + " HardDiskFile Path is : " + m_hardDiskFilePath);
		// Initialize the harddisk File Instance
		m_hardDiskFileInstance = new File(m_hardDiskFilePath + "/" + m_hardDiskFileName);
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
		
		this.status = ServerStatus.STOPPED;
	}
	
	/**
	 * Initialize most of the internal KVServer data objects. 
	 */
	public void initKVServer(String metadata, int cacheSize, String replacementStrategy) {
		this.m_cacheSize = cacheSize;
		this.m_strategy = replacementStrategy;
		this.metadata = new HashRing(metadata);
		
		this.m_currentCacheEntries = 0;
		this.m_currentHardDiskEntries = 0;

		// Initialize the maps for the server object
		System.out.println("Initializing Server Maps");
		logger.info("Initializing Server Maps");
		this.m_cacheValueMap = new HashMap<String, String>();
		this.m_cacheFIFOList = new LinkedList<String>();
		this.m_cacheLRUList = new LinkedList<String>();
		this.m_cacheLFUMap = new HashMap<String, Integer>();
		this.m_hardDiskValueMap = new HashMap<String, String>();
    
		//Initialize the lock
		this.m_myLock = new Object();
		
		//load data from hard disk file
		repopulateHardDiskMap();

		// Start the server object
		System.out.println("Starting Server");
		logger.info("Starting Server");
		this.start();
	}
	
	/**
	 * Functions control the server's status.
	 */
	public void stopServer() {
		this.status = ServerStatus.STOPPED;
	}
	public void startServer() {
		this.status = ServerStatus.ACTIVE;
	}
	public void lockWrite() {
		this.status = ServerStatus.WRITE_LOCKED;
	}
	public void unLockWrite() {
		startServer();
	}
	public String getStatus() {
		return this.status.toString();
	}

	// This function is used to rewrite the harddisk file with the key value pairs from the harddisk map
	// Returns boolean, true for success, false for failure
	private boolean overwriteHardDiskFile() {
		try {
			System.out.println("Starting to overwrite Hard disk File from Hard Disc Map");
			logger.info("Starting to overwrite Hard disk File from Hard Disc Map");
			// Initialize the file writer with our file
			this.m_hardDiskFileWriter = new PrintWriter(this.m_hardDiskFileInstance);
			int linesWritten = 0;
			// Iterate through all pairs of hard disk map
			for ( Map.Entry<String, String> iteratorDummy : this.m_hardDiskValueMap.entrySet()) {
				// For each key/value pair, print it and write it to the hard disc File as key first then value each on its own line
				//System.out.println("Inserting Key: " + iteratorDummy.getKey());
				this.m_hardDiskFileWriter.println(iteratorDummy.getKey());
				//System.out.println("Inserting Value: " + iteratorDummy.getValue());
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
	private boolean repopulateHardDiskMap() {
		try {
			System.out.println("Starting to repopulate Hard disk Map from Hard Disc File");
			logger.info("Starting to repopulate Hard disk Map from Hard Disc File");
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
					//System.out.println("Key is: " + CurrentLine);
					Key = CurrentLine;
				} else {
					// If counter is odd the line is a value and add it to the map
					//System.out.println("Value is: " + CurrentLine);
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
			try {
				this.m_hardDiskFileReader.close();
			} catch (IOException e) {
				System.out.println("Encountered Error while trying to close file reader");
				e.printStackTrace();
			}
			// Reset harddisk Cache 
			this.m_hardDiskValueMap.clear();
			return false;
		}
	}
	// This function is the entry point for handling a client message, at this point the message is valid, first called in ClientConnection
	public KVMessage handleClientMessage(KVMessage msg) {
		String header = msg.getHeader();
		KVMessage returnMsg;
		// Decide on the appropriate handler based on what the client message was through the use of a switch statement
		switch (header) {
		case "connect": 
			returnMsg = handleConnect(msg);
			break;
		case "disconnect":
			returnMsg = handleDisconnect(msg);
			break;
		case "put":
			returnMsg = handlePut(msg);
			break;
		case "admin_put":
			returnMsg = handleAdminPut(msg);
			break;
		case "get":
			returnMsg = handleGet(msg);
			break;
		case "logLevel":
			returnMsg = handleLogLevel(msg);
			break;
		case "help":
			returnMsg = handleHelp(msg);
			break;
		case "shutdown":
			returnMsg = handleShutdown(msg);
			break;
		case "init":
			returnMsg = handleInit(msg);
			break;
		case "start":
			returnMsg = handleStart(msg);
			break;
		case "stop":
			returnMsg = handleStop(msg);
			break;
		case "metadata":
			returnMsg = handleMetadata(msg);
			break;
		case "addNode":
			returnMsg = handleAddNode(msg);
			break;
		case "removeNode":
			returnMsg = handleRemoveNode(msg);
			break;
		default:
			return returnMsg = new KVAdminMessage("", "", "", "");
		}
		return returnMsg;
	}
	
	public KVMessage handleInit(KVMessage msg) {
		//We may not actually need this message. The ECS can construct a server with the 
		//cache size and replacement strategy specified, and just send a metadata message to 
		//initialize the metadata. This way the metadata update message essentially replaces 
		//the init message in terms of functionality.
		return msg;
	}
	
	public KVMessage handleStart(KVMessage msg) {
		startServer();
		return new KVAdminMessage("start","SUCCESS","","");
	}
	
	public KVMessage handleStop(KVMessage msg) {
		stopServer();
		return new KVAdminMessage("stop","SUCCESS","","");
	}
	
	/**
	 * Handle a metadata update message received from the ecs.
	 * Metadata is stored in the value field. 
	 * This function is used when all necessary data transfers are already
	 * complete, so we just need to update the internal metadata variable.
	 */
	public KVMessage handleMetadata(KVMessage msg) {
		this.metadata = new HashRing(msg.getValue());
		return new KVAdminMessage("metadata","SUCCESS","","");
	}
	
	// This function is used to handle a client connect request
	public KVMessage handleConnect(KVMessage msg) {
		System.out.println("Handling Connect, echo back nothing to do");
		logger.info("Handling Connect, echo back nothing to do");
		KVMessage returnMsg = msg;
		return returnMsg;
	}
	// This function is used to handle a client disconnect request
	public KVMessage handleDisconnect(KVMessage msg) {
		System.out.println("Handling Disconnect, echo back nothing to do");
		logger.info("Handling Disconnect, echo back nothing to do");
		KVMessage returnMsg = msg;
		return returnMsg;
	}
	// This function is used to handle a client log level change request
	public KVMessage handleLogLevel(KVMessage msg) {
		System.out.println("Handling Log Level");
		logger.info("Handling Log Level");
		String Key = msg.getKey();
		String Value = msg.getValue();
		// Set the new log level
		logger.setLevel(Level.toLevel(Value));
		KVMessage returnMsg = new KVAdminMessage("logLevel", "SUCCESS", " ", " ");
		return returnMsg;
	}
	// This function is used to handle a client help message
	public KVMessage handleHelp(KVMessage msg) {
		System.out.println("Handling Help, echo back nothing to do");
		logger.info("Handling Help, echo back nothing to do");
		KVMessage returnMsg = msg; ;
		return returnMsg;
	}
	
	// This function is used to handle a quit message
	// These are processed even in the stopped state because they come from the ECS.
	public KVMessage handleShutdown(KVMessage msg) {
		System.out.println("Handling Shutdown");
		logger.info("Handling Shutdown");
		closeServer();
		KVMessage returnMsg = new KVAdminMessage("shutdown", "SUCCESS", " ", " ");
		return returnMsg;
	}
	
	// This function is used to handle a client get request
	public KVMessage handleGet(KVMessage msg) {
		if (status == ServerStatus.STOPPED){
			return new KVAdminMessage("get","SERVER_STOPPED",msg.getKey(),msg.getValue());
		}		
		
		System.out.println("Handling Get");
		logger.info("Handling Get");
		String Key = msg.getKey();
		String Value = msg.getValue();
		
		//check if this server is responsible for this key
		Server responsible = metadata.getResponsible(Key);
		if (!metadata.canGet(this.id, Key)){
			return new KVAdminMessage("get","SERVER_NOT_RESPONSIBLE",msg.getKey(),metadata.toString());
		}
		
		KVMessage returnMsg = null;
		boolean success = false;
		// First check whether the Key Value pair get wants is in the cache
		boolean keyExists = this.m_cacheValueMap.containsKey(Key);
		if (keyExists) {
			// Cache Hit, get the Value from the cacheMap
			Value = this.m_cacheValueMap.get(Key);
			// Update
			success = this.updateCacheHit(Key, Value);
			if (!success) {
				// If for some reason updating the pair in cache failed then return failure message
				returnMsg = new KVAdminMessage("get", "GET_ERROR", Key, Value);
				return returnMsg;
			} else {
				// Set success message and end of this get operation
				returnMsg = new KVAdminMessage("get", "GET_SUCCESS", Key, Value);
			}
		} else {
			// Cache Miss
			// Need to load from hard disk file into map
			success = this.repopulateHardDiskMap();
			if (!success) {
				// If for some reason the load failed then return failure message
				returnMsg = new KVAdminMessage("get", "GET_ERROR", Key, Value);
				return returnMsg;
			}
			// Need to get the Key Value pair from hard disk map
			// Check if it exists
			keyExists = this.m_hardDiskValueMap.containsKey(Key);
			if (!keyExists) {
				// If the pair does not exist in the hard disk file either
				returnMsg = new KVAdminMessage("get", "GET_ERROR", Key, Value);
				return returnMsg;
			}
			// Get the Value from hard disk map
			Value = this.m_hardDiskValueMap.get(Key);
			// Insert this Key Value Pair into the cache
			success = this.insertIntoCache(Key, Value);
			if (!success) {
				// If for some reason the writing to cache failed then return failure message
				returnMsg = new KVAdminMessage("get", "GET_ERROR", Key, Value);
				return returnMsg;
			} else {
				// Set success message and end of this put-add operation
				returnMsg = new KVAdminMessage("get", "GET_SUCCESS", Key, Value);
			}
		}
		return returnMsg;
	}
	
	public int getPort() {
		return port;
	}
	
	// This function is used to handle a client put request
	public KVMessage handlePut(KVMessage msg) {
		if (status == ServerStatus.STOPPED){
			return new KVAdminMessage("get","SERVER_STOPPED",msg.getKey(),msg.getValue());
		} else if (status == ServerStatus.WRITE_LOCKED){
			return new KVAdminMessage("get","SERVER_WRITE_LOCK",msg.getKey(),msg.getValue());
		}
		
		System.out.println("Handling Put");
		logger.info("Handling Put");
		String Key = msg.getKey();
		String Value = msg.getValue();
		//check if this server is responsible for this key		
		Server responsible = metadata.getResponsible(Key);
		//System.out.println(Integer.toString(responsible.id));
		//System.out.println(Integer.toString(this.id));
		//System.out.println(Integer.toString(this.getPort()));
		//System.out.println(this.metadata.toString());	
		if (responsible.id != this.id){
			return new KVAdminMessage("put","SERVER_NOT_RESPONSIBLE",msg.getKey(),metadata.toString());
		}
		if(!updateReplicas(msg)) {
			System.out.println("Responsible server: failed to update replicas!");
			logger.error("Responsible server: failed to update replicas!");
		}
		return doPut(Key,Value);
	}
	
	/**
	 * This function is used to handle an admin put request from another server.
	 * It is essentially the same as handlePut except it skips the status and 
	 * responsibility checking (servers should be able to put to each other at any time).
	 */
	public KVMessage handleAdminPut(KVMessage msg) {		
		if(msg.getStatus().equals("PUT_REPLICA")) {
			System.out.println("Received replica update message, updating values.");
			logger.info("Received replica update message, updating values.");
		} else {
			System.out.println("Handling Admin Put");
			logger.info("Handling Admin Put");
		}
		String Key = msg.getKey();
		String Value = msg.getValue();
		return doPut(Key,Value);
	}
	
	/**
	 * Do the actual put operation on (Key, Value) pair
	 */
	private KVMessage doPut(String Key, String Value) {
		KVMessage returnMsg = null;
		// first load the hard disk file into our map
		boolean success = this.repopulateHardDiskMap();
		if (!success) {
			// If for some reason the load failed then return failure message
			returnMsg = new KVAdminMessage("put", "PUT_ERROR", Key, Value);
			return returnMsg;
		}
		
		// Decide whether it is a update, delete or add
		if (Value.equals("null")) {
			// this is a delete operation, so remove Key Value pair from map
			this.m_hardDiskValueMap.remove(Key);
			// Rewrite hard Disk file
			success = this.overwriteHardDiskFile();
			if (!success) {
				// If for some reason the rewrite failed then return failure message
				returnMsg = new KVAdminMessage("put", "DELETE_ERROR", Key, Value);
				return returnMsg;
			}
			// Need to Delete the Key/Value pair if it is in the cache as well
			success = this.deleteFromCache(Key, Value);
			if (!success) {
				// If for some reason the deleting from the cache failed then return failure message
				returnMsg = new KVAdminMessage("put", "DELETE_ERROR", Key, Value);
				return returnMsg;
			} else {
				// Set success message and end of this put-add operation
				returnMsg = new KVAdminMessage("put", "DELETE_SUCCESS", Key, Value);
			}
		}
		else if (!this.m_hardDiskValueMap.containsKey(Key)) {
			// This is an add operation, so add Key Value pair into map
			this.m_hardDiskValueMap.put(Key, Value);
			// Rewrite hard Disk file
			success = this.overwriteHardDiskFile();
			if (!success) {
				// If for some reason the rewrite failed then return failure message
				returnMsg = new KVAdminMessage("put", "PUT_ERROR", Key, Value);
				return returnMsg;
			}
			// Now try to put the Key/Value pair into the Cache
			success = this.insertIntoCache(Key, Value);
			if (!success) {
				// If for some reason the writing to cache failed then return failure message
				returnMsg = new KVAdminMessage("put", "PUT_ERROR", Key, Value);
				return returnMsg;
			} else {
				// Set success message and end of this put-add operation
				returnMsg = new KVAdminMessage("put", "PUT_SUCCESS", Key, Value);
			}
		} 
		else {
			// this is a update operation, so update the Key value Pair, put will update the original pair, or create one if it doesn't exist
			this.m_hardDiskValueMap.put(Key, Value);
			// Rewrite hard Disk file
			success = this.overwriteHardDiskFile();
			if (!success) {
				// If for some reason the rewrite failed then return failure message
				returnMsg = new KVAdminMessage("put", "PUT_ERROR", Key, Value);
				return returnMsg;
			}
			// Need to get cache to use the new Key/Value pair if it is in the cache as well
			success = this.insertIntoCache(Key, Value);
			if (!success) {
				// If for some reason the writing to cache failed then return failure message
				returnMsg = new KVAdminMessage("put", "PUT_ERROR", Key, Value);
				return returnMsg;
			} else {
				// Set success message and end of this put-add operation
				returnMsg = new KVAdminMessage("put", "PUT_UPDATE", Key, Value);
			}
		}
		return returnMsg;
	}
	
	/**
	 * Handle the message of adding the server stored in the key field
	 * This server is responsible for transferring data to it. 
	 */
	public KVMessage handleAddNode(KVMessage msg) {
		ServerStatus prevStatus = this.status;
		this.status = ServerStatus.WRITE_LOCKED;
		//Note: there's no need to write lock the new server because it should be in the 
		//stopped state so no client can write to it anyway. 
		
		Server server = new Server(msg.getKey());
		metadata.addServer(server);
		
		boolean success = transferData(server);		
		this.status = prevStatus;
		if (success){
			return new KVAdminMessage("addNode","SUCCESS",msg.getKey(),msg.getValue());
		}
		else{
			return new KVAdminMessage("addNode","FAILED",msg.getKey(),msg.getValue());
		}
	}
	
	/**
	 * Remove this server from the system and transfer all of its
	 * data to the server contained in the key. 
	 */
	public KVMessage handleRemoveNode(KVMessage msg) {
		ServerStatus prevStatus = this.status;
		this.status = ServerStatus.WRITE_LOCKED;
		//Note: there's no need to write lock the successor server that we're transferring to
		//because it currently thinks it's not responsible for this part of the data (metadata update
		//comes after all transferring is complete).
		Server successor = new Server(msg.getKey());
		Server thisServer = new Server(msg.getValue());
		metadata.removeServer(thisServer);
		
		boolean success = transferData(successor);
		this.status = prevStatus;
		if (success){
			return new KVAdminMessage("removeNode","SUCCESS",msg.getKey(),msg.getValue());
		}
		else{
			return new KVAdminMessage("removeNode","FAILED",msg.getKey(),msg.getValue());
		}
	}
	
	/**
	 * Locks this server and transfers data to another server.
	 * Note that the metadata must be updated before calling this function
	 */
	private boolean transferData(Server server) {
		if (server.id == this.id){
			//this can happen if there is only 1 server in the metadata. 
			return true;
		}
		
		logger.info("Transferring data to server "+server.toString());
		try {
			//connect to server as a client
			Client client = new Client(server.ipAddress, server.port);
			KVMessage response = client.getResponse();
			
			//for every (key,value) pair, check whether the responsible server is the given server
			ArrayList<String> movedKeys = new ArrayList<String>();
			for (Map.Entry<String,String> entry : m_hardDiskValueMap.entrySet()){
				String key = entry.getKey();
				String value = entry.getValue();
				Server responsible = metadata.getResponsible(key);
				logger.debug("key = "+key+", responsible = "+responsible);
				
				if (responsible != null && responsible.id == server.id){
					movedKeys.add(key);
					logger.debug("Transferring "+key);
					//send a special put message which overrides status and responsibility checking
					KVMessage request = new KVAdminMessage("admin_put","",key,value);
					client.sendMessage(request);
					response = client.getResponse();
					logger.debug("Response status: "+response.getStatus());
				}
			}
			
			//delete all the moved keys from this server
			logger.debug("Deleting all transferred keys from this server");
			for (String key : movedKeys) {
				doPut(key, "null");
			}
			
		} catch (Exception e){
			return false;
		}
		return true;
	}
	
	/**
	 * This method will send update messages to the replicas. Returns true on success
	 */
	private boolean updateReplicas(KVMessage msg) {
		String key = msg.getKey();
		String value = msg.getValue();
		
		// Function in HashRing that pulls out the two servers we need to connect to
		HashRing.Replicas replicas = metadata.getReplicas(key);
		
		// Construct the message
		KVMessage request = new KVAdminMessage("admin_put", "PUT_REPLICA", key, value);
		
		Server replica = replicas.first;
		
		logger.debug("Trying to connect to replica server "+replica.toString());
		
		try {
			Client client = new Client(replica.ipAddress, replica.port);
			KVMessage response = client.getResponse();
			if(!response.getStatus().equals("CONNECT_SUCCESS")) {
				logger.debug("Failed connecting to replica server!");
				return false;
			}
			client.sendMessage(request);
			response = client.getResponse();
			if(!"PUT_UPDATE PUT_SUCCESS".contains(response.getStatus())) {
				logger.debug("Replica server update failed!");
				return false;
			} else {
				logger.debug("Replica server update success!");
			}
		} catch (Exception e) {
			logger.debug("Unable to connect to replica server "+replica.toString());
			return false;
		}
		
		replica = replicas.second;
		
		logger.debug("Trying to connect to replica server "+replica.toString());

		try {
			Client client = new Client(replica.ipAddress, replica.port);
			KVMessage response = client.getResponse();
			if(!response.getStatus().equals("CONNECT_SUCCESS")) {
				logger.debug("Failed connecting to replica server!");
				return false;
			}
			client.sendMessage(request);
			response = client.getResponse();
			if(!"PUT_UPDATE PUT_SUCCESS".contains(response.getStatus())) {
				logger.debug("Replica server update failed!");
				return false;
			} else {
				logger.debug("Replica server update success!");
			}
		} catch (Exception e) {
			logger.debug("Unable to connect to replica server "+replica.toString());
			return false;
		}
		
		return true;
	}
	
	// This function is used to update the Cache Key Value Pair in case it was used
	private boolean updateCacheHit(String key, String value) {
		// When we call this function we know Cache has the key value pair
		System.out.println("Got Hit from Cache, Pair was Key: " + key + " Value: " + value);
		logger.info("Got Hit from Cache, Pair was Key: " + key + " Value: " + value);
		// Just need to increase the usage of this key pair by one for LFU
		this.m_cacheLFUMap.put(key, this.m_cacheLFUMap.get(key) + 1 );
		// Does not effect FIFO linked list
		// Update LRU linked list by moving that pair to end of the list (We insert at end of list and remove the start of list)
		int index = this.m_cacheLRUList.indexOf(key);
		this.m_cacheLRUList.remove(index);
		this.m_cacheLRUList.add(key);
		// No change to current entries amount
		return true;
	}
	// This function is used to delete key value pair from the cache
	private boolean deleteFromCache(String key, String value) {
		// Insert Scoped Lock here
		synchronized(m_myLock) {
			// When we call this function we don't know if Cache has the Key Value Pair we want to delete
			System.out.println("Deleting from Cache Key: " + key + " Value: " + value);
			logger.info("Deleting from Cache Key: " + key + " Value: " + value);
			// remove the pair to all the other maps and lists we need
			// remove value in cache value map
			this.m_cacheValueMap.remove(key);
			// remove value in cache LFU map
			this.m_cacheLFUMap.remove(key);
			// remove key in FIFO linked list
			this.m_cacheFIFOList.removeFirstOccurrence(key);
			// remove key in  LRU linked list
			this.m_cacheLRUList.removeFirstOccurrence(key);
			// decrease size of cache pairs by 1
			this.m_currentCacheEntries = this.m_currentCacheEntries - 1;
			return true;
		}
	}
	// This function is used to add a new key value pair into the cache
	private void addToCache(String key, String value) {
		// add the pair to all the other maps and lists we need
		// add value in cache value map
		this.m_cacheValueMap.put(key, value);
		// add value in cache LFU map, start the usage at 1
		this.m_cacheLFUMap.put(key, 1);
		// add key to end of FIFO linked list
		this.m_cacheFIFOList.add(key);
		// add key to end of LRU linked list
		this.m_cacheLRUList.add(key);
		// increase size of cache pairs by 1
		this.m_currentCacheEntries = this.m_currentCacheEntries + 1;
	}
	// This function is used to put key value pair into the cache
	private boolean insertIntoCache(String key, String value) {
		// Insert Scoped Lock here
		synchronized(m_myLock) {
			// When we call this function we don't know if Cache is already Full or if that key value pair already exists in it
			System.out.println("Inserting into Cache Key: " + key + " Value: " + value);
			logger.info("Inserting into Cache Key: " + key + " Value: " + value);
			// Check whether this is an update(already exist in cache) if so then we won't have to evict anything 
			boolean isUpdate =this.m_cacheValueMap.containsKey(key);
			if (isUpdate) {
				// update value in cache value map
				this.m_cacheValueMap.put(key, value);
				// update as if we got a hit
				boolean updateSuccess = this.updateCacheHit(key, value);
				return updateSuccess;
			}
			// Not an update but adding new entry, Check whether the cache is full 
			boolean cacheFull = this.m_currentCacheEntries > this.m_cacheSize;
			if (cacheFull) {
				boolean evictSuccess = false;
				// do a switch statement from strategy and evict first according to one of them and then add to the cache
				switch (this.m_strategy) {
				case "FIFO": 
					evictSuccess = evictFIFO();
					break;
				case "LRU":
					evictSuccess = evictLRU();
					break;
				case "LFU":
					evictSuccess = evictLFU();
					break;
				default:
					evictSuccess = false;
				}
				if (!evictSuccess) {
					return false;
				}
				this.addToCache(key,value);
				return true;
			} else {
				// add the pair
				this.addToCache(key, value);
			}
			return true;
		}
	}
	// This function is used to evict a key value pair according to FIFO
	private boolean evictFIFO() {
		// When we call this function we know Cache is already Full
		// Find the key of the element we want to evict
		String key = this.m_cacheFIFOList.getFirst();
		System.out.println("Evicting using FIFO from Cache Key: " + key);
		logger.info("Evicting using FIFO from Cache Key: " + key);
		// remove the pair to all the other maps and lists we need
		// remove value in cache value map
		this.m_cacheValueMap.remove(key);
		// remove value in cache LFU map
		this.m_cacheLFUMap.remove(key);
		// remove key in FIFO linked list
		this.m_cacheFIFOList.removeFirstOccurrence(key);
		// remove key in  LRU linked list
		this.m_cacheLRUList.removeFirstOccurrence(key);
		// decrease size of cache pairs by 1
		this.m_currentCacheEntries = this.m_currentCacheEntries - 1;
		return true;
	}
	// This function is used to evict a key value pair according to LRU
	private boolean evictLRU() {
		// When we call this function we know Cache is already Full
		// Find the key of the element we want to evict
		// Iterate through all pairs of LFU map
		String key = this.m_cacheLRUList.getFirst();
		System.out.println("Evicting using LRU from Cache Key: " + key);
		logger.info("Evicting using LRU from Cache Key: " + key);
		// remove the pair to all the other maps and lists we need
		// remove value in cache value map
		this.m_cacheValueMap.remove(key);
		// remove value in cache LFU map
		this.m_cacheLFUMap.remove(key);
		// remove key in FIFO linked list
		this.m_cacheFIFOList.removeFirstOccurrence(key);
		// remove key in  LRU linked list
		this.m_cacheLRUList.removeFirstOccurrence(key);
		// decrease size of cache pairs by 1
		this.m_currentCacheEntries = this.m_currentCacheEntries - 1;
		return true;
	}
	// This function is used to evict a key value pair according to LFU
	private boolean evictLFU() {
		// When we call this function we know Cache is already Full
		// Find the key of the element we want to evict
		String key = null;
		int frequency = 99999999;
		for ( Map.Entry<String, Integer> iteratorDummy : this.m_cacheLFUMap.entrySet()) {
			// For each key/value pair, see if it was less frequently used than the previous lowest
			if (iteratorDummy.getValue() < frequency) {
				key = iteratorDummy.getKey();
				frequency = iteratorDummy.getValue();
				System.out.println("Lesser Used Key Found: " + iteratorDummy.getKey());
				System.out.println("Frequency was: " + iteratorDummy.getValue());
			}
		}
		System.out.println("Evicting using LFU from Cache Key: " + key);
		logger.info("Evicting using LFU from Cache Key: " + key);
		// remove the pair to all the other maps and lists we need
		// remove value in cache value map
		this.m_cacheValueMap.remove(key);
		// remove value in cache LFU map
		this.m_cacheLFUMap.remove(key);
		// remove key in FIFO linked list
		this.m_cacheFIFOList.removeFirstOccurrence(key);
		// remove key in  LRU linked list
		this.m_cacheLRUList.removeFirstOccurrence(key);
		// decrease size of cache pairs by 1
		this.m_currentCacheEntries = this.m_currentCacheEntries - 1;
		return true;
	}
	
	/**
	 * Initializes and starts the server. 
	 * Loops until the the server should be closed.
	 */
	public void run() {

		running = initializeSocket();

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
					
				} catch (SocketException e) {
					logger.info("Socket closed!", e);
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
	public void closeServer(){
		logger.info("Shutting down server");
		running = false;
		try {
			serverSocket.close();
		} catch (IOException e) {
			logger.error("Error! " +
					"Unable to close socket on port: " + port, e);
		}
	}

	/**
	 * Creates the socket and starts listening on it.
	 */
	private boolean initializeSocket() {
		logger.info("Initialize socket ...");
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
	
	private static void printUsage() {
		System.out.println("Valid usages:");
		System.out.println("\t<port> <id>");
		System.out.println("\t<port> <cache size> <replacement strategy>");
		System.out.println("\t<port> <cache size> <replacement strategy> <id>");
	}

	/**
	 * Main entry point for the echo server application. 
	 * Valid ways to initialize with arguments:
	 * 		<port> <id>
	 * 		<port> <cache size> <replacement strategy>
	 * 		<port> <cache size> <replacement strategy> <id>
	 */
	public static void main(String[] args) {
		try {
			String portStr="50000", strategy="FIFO", cacheSizeStr="1", idStr="0";
			
			//determine what each argument represents based on the number of arguments.
			if (args.length == 2){
				//interpret 2 arguments as port and id
				portStr = args[0];
				idStr = args[1];
			}
			else if (args.length == 3){
				//interpret 3 arguments as port, cache size, and replacement strategy
				portStr = args[0];
				cacheSizeStr = args[1];
				strategy = args[2];
			}
			else if (args.length == 4){
				//interpret 3 arguments as port, cache size, replacement strategy, and id
				portStr = args[0];
				cacheSizeStr = args[1];
				strategy = args[2];
				idStr = args[3];
			}
			else{
				System.out.println("Error! Invalid number of arguments!");
				KVServer.printUsage();
				System.exit(0);
			}
			
			new LogSetup("logs/server_"+idStr+".log", Level.DEBUG);
			
			//validity check arguments
			if (!strategy.equals("FIFO") && !strategy.equals("LRU") && !strategy.equals("LFU")) {
				System.out.println("Error! strategy argument invalid! Must be one of FIFO, LRU, LFU");
				System.exit(0);
			}
			int port = Integer.parseInt(portStr);
			int cacheSize = Integer.parseInt(cacheSizeStr);
			int id = Integer.parseInt(idStr);
			
			if (args.length == 2){
				new KVServer(port, id);
			}
			else{
				new KVServer(port, cacheSize, strategy, id);
			}
			
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		} catch (NumberFormatException nfe) {
			System.out.println("Error! Arugments port, cache size, and id must be integers");
			KVServer.printUsage();
			nfe.printStackTrace();
			System.exit(1);
		}
	}
}

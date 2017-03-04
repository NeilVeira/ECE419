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
		logger.info("Initializing Server Variables");
		this.port = port;
		this.m_cacheSize = cacheSize;
		this.m_strategy = strategy;
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
		// Initialize harddisk file information
		System.out.println("Initializing Hard Disk File Variables");
		logger.info("Initializing Hard Disk File Variables");
		// Get where the program is running from (where the project is)
		this.m_hardDiskFilePath = System.getProperty("user.dir");
		// HardCode file name to be storage.txt
		this.m_hardDiskFileName = "storage.txt";
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

		// Start the server object
		System.out.println("Starting Server");
		logger.info("Starting Server");
		this.start();
	}

	// This function is used to rewrite the harddisk file with the key value pairs from the harddisk map
	// Returns boolean, true for success, false for failure
	public boolean overwriteHardDiskFile() {
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
	public boolean repopulateHardDiskMap() {
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
	public common.messages.KVMessage handleClientMessage(common.messages.KVMessage msg) {
		String Header = msg.getHeader();
		common.messages.KVMessage returnMsg;
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
			returnMsg = handleGet(msg);
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
		System.out.println("Handling Connect, echo back nothing to do");
		logger.info("Handling Connect, echo back nothing to do");
		common.messages.KVMessage returnMsg = msg;
		return returnMsg;
	}
	// This function is used to handle a client disconnect request
	public common.messages.KVMessage handleDisconnect(common.messages.KVMessage msg) {
		System.out.println("Handling Disconnect, echo back nothing to do");
		logger.info("Handling Disconnect, echo back nothing to do");
		common.messages.KVMessage returnMsg = msg;
		return returnMsg;
	}
	// This function is used to handle a client log level change request
	public common.messages.KVMessage handleLogLevel(common.messages.KVMessage msg) {
		System.out.println("Handling Log Level");
		logger.info("Handling Log Level");
		String Key = msg.getKey();
		String Value = msg.getValue();
		// Set the new log level
		logger.setLevel(Level.toLevel(Value));
		common.messages.KVMessage returnMsg = new common.messages.MessageType("logLevel", "success", " ", " ");
		return returnMsg;
	}
	// This function is used to handle a client help message
	public common.messages.KVMessage handleHelp(common.messages.KVMessage msg) {
		System.out.println("Handling Help, echo back nothing to do");
		logger.info("Handling Help, echo back nothing to do");
		common.messages.KVMessage returnMsg = msg; ;
		return returnMsg;
	}
	// This function is used to handle a client quit message
	public common.messages.KVMessage handleQuit(common.messages.KVMessage msg) {
		System.out.println("Handling Quit");
		logger.info("Handling Quit");
		common.messages.KVMessage returnMsg = new common.messages.MessageType("quit", "success", " ", " ");
		// Should be handled in ClientConnection to terminate that socket only
		return returnMsg;
	}
	// This function is used to handle a client get request
	public common.messages.KVMessage handleGet(common.messages.KVMessage msg) {
		System.out.println("Handling Get");
		logger.info("Handling Get");
		String Key = msg.getKey();
		String Value = msg.getValue();
		common.messages.KVMessage returnMsg = null;
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
				returnMsg = new common.messages.MessageType("get", "GET_ERROR", Key, Value);
				return returnMsg;
			} else {
				// Set success message and end of this get operation
				returnMsg = new common.messages.MessageType("get", "GET_SUCCESS", Key, Value);
			}
		} else {
			// Cache Miss
			// Need to load from hard disk file into map
			success = this.repopulateHardDiskMap();
			if (!success) {
				// If for some reason the load failed then return failure message
				returnMsg = new common.messages.MessageType("get", "GET_ERROR", Key, Value);
				return returnMsg;
			}
			// Need to get the Key Value pair from hard disk map
			// Check if it exists
			keyExists = this.m_hardDiskValueMap.containsKey(Key);
			if (!keyExists) {
				// If the pair does not exist in the hard disk file either
				returnMsg = new common.messages.MessageType("get", "GET_ERROR", Key, Value);
				return returnMsg;
			}
			// Get the Value from hard disk map
			Value = this.m_hardDiskValueMap.get(Key);
			// Insert this Key Value Pair into the cache
			success = this.insertIntoCache(Key, Value);
			if (!success) {
				// If for some reason the writing to cache failed then return failure message
				returnMsg = new common.messages.MessageType("get", "GET_ERROR", Key, Value);
				return returnMsg;
			} else {
				// Set success message and end of this put-add operation
				returnMsg = new common.messages.MessageType("get", "GET_SUCCESS", Key, Value);
			}
		}
		return returnMsg;
	}
	// This function is used to handle a client put request
	public common.messages.KVMessage handlePut(common.messages.KVMessage msg) {
		System.out.println("Handling Put");
		logger.info("Handling Put");
		String Key = msg.getKey();
		String Value = msg.getValue();
		common.messages.KVMessage returnMsg = null;
		// first load the hard disk file into our map
		boolean success = this.repopulateHardDiskMap();
		if (!success) {
			// If for some reason the load failed then return failure message
			returnMsg = new common.messages.MessageType("put", "PUT_ERROR", Key, Value);
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
				returnMsg = new common.messages.MessageType("put", "DELETE_ERROR", Key, Value);
				return returnMsg;
			}
			// Need to Delete the Key/Value pair if it is in the cache as well
			success = this.deleteFromCache(Key, Value);
			if (!success) {
				// If for some reason the deleting from the cache failed then return failure message
				returnMsg = new common.messages.MessageType("put", "DELETE_ERROR", Key, Value);
				return returnMsg;
			} else {
				// Set success message and end of this put-add operation
				returnMsg = new common.messages.MessageType("put", "DELETE_SUCCESS", Key, Value);
			}
		}
		else if (!this.m_hardDiskValueMap.containsKey(Key)) {
			// This is an add operation, so add Key Value pair into map
			this.m_hardDiskValueMap.put(Key, Value);
			// Rewrite hard Disk file
			success = this.overwriteHardDiskFile();
			if (!success) {
				// If for some reason the rewrite failed then return failure message
				returnMsg = new common.messages.MessageType("put", "PUT_ERROR", Key, Value);
				return returnMsg;
			}
			// Now try to put the Key/Value pair into the Cache
			success = this.insertIntoCache(Key, Value);
			if (!success) {
				// If for some reason the writing to cache failed then return failure message
				returnMsg = new common.messages.MessageType("put", "PUT_ERROR", Key, Value);
				return returnMsg;
			} else {
				// Set success message and end of this put-add operation
				returnMsg = new common.messages.MessageType("put", "PUT_SUCCESS", Key, Value);
			}
		} 
		else {
			// this is a update operation, so update the Key value Pair, put will update the original pair, or create one if it doesn't exist
			this.m_hardDiskValueMap.put(Key, Value);
			// Rewrite hard Disk file
			success = this.overwriteHardDiskFile();
			if (!success) {
				// If for some reason the rewrite failed then return failure message
				returnMsg = new common.messages.MessageType("put", "PUT_ERROR", Key, Value);
				return returnMsg;
			}
			// Need to get cache to use the new Key/Value pair if it is in the cache as well
			success = this.insertIntoCache(Key, Value);
			if (!success) {
				// If for some reason the writing to cache failed then return failure message
				returnMsg = new common.messages.MessageType("put", "PUT_ERROR", Key, Value);
				return returnMsg;
			} else {
				// Set success message and end of this put-add operation
				returnMsg = new common.messages.MessageType("put", "PUT_UPDATE", Key, Value);
			}
		}
		return returnMsg;
	}
	// This function is used to update the Cache Key Value Pair in case it was used
	public boolean updateCacheHit(String key, String value) {
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
	public boolean deleteFromCache(String key, String value) {
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
	public void addToCache(String key, String value) {
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
	public boolean insertIntoCache(String key, String value) {
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
	public boolean evictFIFO() {
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
	public boolean evictLRU() {
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
	public boolean evictLFU() {
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
				if (!strategy.equals("FIFO") && !strategy.equals("LRU") && !strategy.equals("LFU")) {
					System.out.println("Error! strategy argument invalid!");
					System.out.println("Usage: Server <int port> <int cacheSize> <string strategy: FIFO, LRU or LFU>! Please try again");
					System.exit(0);
				}
				new KVServer(port, cacheSize, strategy);
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

import java.io.IOException;
import java.io.FileNotFoundException;
import java.net.SocketTimeoutException;
import java.net.SocketException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;
import java.util.Stack;
import java.io.FileWriter;
import java.io.FileReader;
import java.io.PrintWriter;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;


public class CS346 {

	public static final int startPort = 9100;
	public static int timestamp = 0;
    public static InputStream is;
    public static OutputStream os;
	private static int Qw;
	private static int Qr;
	private static int n;
	//servers is used to instantiate the servers and/or threads
	private Server[] servers;
	private Client[] clients;
	
	private static volatile boolean close = false;
    
    public static void usage() {
        System.out.println("Usage:");
        System.out.println("\tjava CS346 <Number of servers> <Qw> <Qr>");
        System.out.println("\t\t<Number of servers>: integer number between 4 and 10. Spawns that many servers.");
        System.out.println("\t\t<Qw>: non-zero positive integer. Sets the value of Qw");
        System.out.println("\t\t<Qr>: non-zero positive integer. Sets the value of Qr");
        System.out.println("\t<Qr> + <Qw> must be greater than <Number of servers>");
		System.out.println("\t<Qw> * 2 much be greater than <Number of servers>");
        System.exit(1);
    }
    
	private class LogManager {
		private int serverID;
		private String filename;
		private FileWriter fileWriter;
		public LogManager(int serverID) {
			this.serverID = serverID;
			this.filename = this.serverID + "-log.txt";
			try {
				fileWriter = new FileWriter(filename);
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			}
			String startMessage = "<LogManager>: the log of Server " + serverID + " begins here.\n";
			String statusMessage = "<Server " + serverID + ">: " + n + " servers have been created. \n<Server " + serverID + ">: " + Qw + " servers are required for a write quorum. \n<Server " + serverID + ">: " + Qr + " servers are required for a read quorum.\n";
			try {
				fileWriter.write(startMessage);
				fileWriter.flush();
				fileWriter.write(statusMessage);
				fileWriter.flush();
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			}
		}

		public void write(String text) {
			try {
				fileWriter.append("<Server " + serverID + ">: " + text + "\n");
				fileWriter.flush();
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
	}
    
	//each server is assigned a unique id
	//a server has a log manager
	//a server has a lock manager
	private class Server implements Runnable{
		private int id;
		private int port;
		private int dataItem;
		private ServerSocket serverSocket;
		private int lock;
		private LogManager logger;
		private int latestTimestamp;
		
		public Server(int id, int port) {
			//set variables
			this.id = id;
			this.port = port;
			this.lock = 0;
			this.dataItem = 0;
			//create a LogManager
			this.logger = new LogManager(this.id);
			this.latestTimestamp = 0;
			
			logger.write("The data item has been set to the initial value");
			logger.write("Data item = " + getDataItem());
			
			//try to open ServerSocket
			try {
				serverSocket = new ServerSocket(port, 0, InetAddress.getByName("127.0.0.1"));
				serverSocket.setSoTimeout(500);
				logger.write("ServerSocket open on port " + port);
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
		
		//acquires a write or read quorum based on the param
		private Socket[] acquireQuorum(int size) {
			Socket[] quorumSockets = new Socket[size -1];
			ArrayList selectedServers = new ArrayList();
			selectedServers.add(this.id);
			//for each server required for a quorum
			boolean serverConnected;
			for (int i=0; i<quorumSockets.length; i++) {				
				for (int j=1; j<=n; j++) {
					if (quorumSockets[i] != null){
						continue;
					}
					if (selectedServers.contains(j)) {
						continue;
					}
					Socket socket = new Socket();
					try {
						//connect with timeout 
						socket.connect(new InetSocketAddress("127.0.0.1", startPort + j), 500);
						if (socket.isConnected()) {
							serverConnected = true;
							quorumSockets[i] = socket;
							OutputStream os = socket.getOutputStream();
							PrintWriter out = new PrintWriter(os, true);
							logger.write("Quorum: locked server "+ j +" for quorum");
							out.println("Quorum: locked by server " + id);
							selectedServers.add(j);
						}
					} catch (SocketTimeoutException t) {
						System.out.println("Server " + id + " has timed out waiting for response form server" + j);
					} catch (IOException e) {
						e.printStackTrace();
						System.err.println("Server " + id + " has crashed trying to obtain a quorum");
						System.exit(1);
					}					
				}
			}	
			selectedServers.toString();
			return quorumSockets;
		}
		
		private int read(PrintWriter[] printWriters, BufferedReader[] bufferedReaders) {
			//todo
			//check the fucking thing of all of the fuckers
			int highestTimestamp = latestTimestamp;
			int checkTimestamp;
			int x = getDataItem();
			String received;
			try {
				for (int i=0; i<printWriters.length; i++) {
					printWriters[i].println("read");
					received = bufferedReaders[i].readLine();
					checkTimestamp = Integer.parseInt(received.substring(0,received.indexOf("-")));
					if (checkTimestamp > highestTimestamp) {
						highestTimestamp = checkTimestamp;
						x = Integer.parseInt(received.substring(received.indexOf("-")+1));
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			}
			logger.write("Transaction: read the data item");
			logger.write("DataItem: x = " + x);			
			return x;
		}
		
		private void write(PrintWriter[] printWriters, int x) {
			//todo
			//command all of them to write, the bastards them
			incrementTimestamp();
			latestTimestamp = getTimestamp();
			setDataItem(x);
			for (int i=0; i<printWriters.length; i++) {
				printWriters[i].println("write:"+latestTimestamp+"-"+x);
			}
			logger.write("Transaction: write the data item to the quorum");
		}
		
		private int set(String toBeConverted) {
			//todo
			//parse the string you fucker
			int x = Integer.parseInt(toBeConverted.substring(toBeConverted.indexOf("=")+1));
			logger.write("Transaction: set the data item to be " + x);
			logger.write("DataItem: x = " + x);
			return x;
		}
		
		public void close() {
			logger.write("Closing server");
			close = true;
		}
		
		public void run() {
			logger.write("Running and ready for operation.");
			//continue server operations until requested to close
			while (!close) {
				try {
					String received;
					//block until connection is made
					Socket socket = serverSocket.accept();

					InputStream is = socket.getInputStream();
					OutputStream os = socket.getOutputStream();
					PrintWriter out = new PrintWriter(os, true);
					BufferedReader in = new BufferedReader(new InputStreamReader(is));
					
					//standardise input, remove whitespace change to lower
					received = in.readLine();
					logger.write("received \"" + received + "\".");
					received = received.trim().toLowerCase().replaceAll(" ","");
						
					//client connection	
					if (received.startsWith("t[")) {
					logger.write("format standardised to \"" + received + "\".");
						//do transaction
						Socket[] quorum;
						
						if (received.contains("write")){
							//a write quorum is required
							//acquire quorum
							logger.write("Transaction: acquiring a write quorum");
							quorum = acquireQuorum(Qw);
						}
						else if (received.contains("read")){
							//a read quorum is required 
							//acquire quorum
							logger.write("Transaction: acquiring a read quorum");
							quorum = acquireQuorum(Qr);
						}
						else {
							//no quorum is required
							quorum = new Socket[0];
						}
						
						OutputStream[] outputStreams = new OutputStream[quorum.length];
						PrintWriter[] printWriters = new PrintWriter[quorum.length];
						InputStream[] inputStreams = new InputStream[quorum.length];
						BufferedReader[] bufferedReaders = new BufferedReader[quorum.length];
						
						for (int h = 0; h<quorum.length; h++) {
							outputStreams[h] = quorum[h].getOutputStream();
							printWriters[h] = new PrintWriter(outputStreams[h], true);
							inputStreams[h] = quorum[h].getInputStream();
							bufferedReaders[h] = new BufferedReader(new InputStreamReader(inputStreams[h]));
						}
						
						int index = received.indexOf(":") + 1;
						String trans = received.substring(index);
						String operation;
						//peel transaction string until commit
						
						int x=0;
						
						while (!trans.equals("")) {
							index = trans.indexOf(";");
							operation = trans.substring(0,index);
							trans = trans.substring(index + 1);
							
							//on begin
							if (operation.startsWith("begin")) {
								logger.write("Transaction: begin transaction \"" + trans + "\"");
								// do nothing
							}

							//on read
							if (operation.startsWith("read")) {
								//get read quorum
								//x = data item
								x = read(printWriters, bufferedReaders);
							}

							//on write
							if (operation.startsWith("write")) {
								//get write quorum
								//have all write in parallel
								//update timestamp and latestTimestamp
								//set data item
								write(printWriters, x);
							}

							//on set data
							if (operation.startsWith("x:=")) {
								//x = var
								x = set(operation);
							}

							//on commit
							if (operation.startsWith("commit")) {
								logger.write("Transaction: committing transaction and releasing quorum");
								for (int i=0; i<quorum.length; i++){
									//send close message
									printWriters[i].println("close");
									//close socket
									quorum[i].close();
								}
							}
						}
						out.println("transaction complete");
					} 					
					//server connection
					else if (received.startsWith("q")) {
						//wait on server instruction
						boolean finished = false;
						String recvd;
						while (!finished) {
							recvd = in.readLine();
							
							//send var and timestamp
							if (recvd.startsWith("read")) {
								out.println(latestTimestamp + "-" + getDataItem());
								logger.write("Quorum: read the local data item");
							}
							
							//update dataItem
							if (recvd.startsWith("write")) {
								int x = Integer.parseInt(recvd.substring(recvd.indexOf("-")+1));
								latestTimestamp = Integer.parseInt(recvd.substring(recvd.indexOf(":")+1,recvd.indexOf("-")));
								setDataItem(x);
								logger.write("Quorum: write to the local data item");
								logger.write("DataItem: x = " + x);
							}
							
							//release
							if (recvd.startsWith("close")) {
								logger.write("Quorum: released from the quorum");
								finished = true;
							}
						}
						
					}
					socket.close();	
				} catch (SocketTimeoutException s) {
					if (close == false) {
						if (checkClose()){
							logger.write("Closing server");
							close = true;
							Thread.currentThread().interrupt();
							return;
						}
					} else {
						if (checkClose()){
							logger.write("Closing server");
							close = true;
							Thread.currentThread().interrupt();
							return;
						} 
						else {
							System.out.println("status of close: " + close);
							s.printStackTrace();
						}
					}
				} catch (IOException e) {
					System.out.println("status of close: " + close);
					e.printStackTrace();
					System.exit(1);
				}
			}
		}

		private int getDataItem() {
			return dataItem;
		}
		
		private void setDataItem(int x) {
			this.dataItem = x;
		}
	}
	
	//only two clients are created, with IDs of 1 and 2
	private class Client implements Runnable{
		private int id;
		private int port;
		private ServerSocket serverSocket;
		private boolean clientClosed;
		
		public Client(int id, int port){
			this.id = id;
			this.port = port;
			clientClosed = false;
			//if trans.txt does not exist, create default
			try {
				FileReader fileReader = new FileReader("trans" + id + ".txt");
				System.out.println("<Client" + this.id + ">: trans" + id + ".txt has been found.");
			} catch (FileNotFoundException e) {
				System.out.println("<Client" + this.id + ">: trans" + id + ".txt has not been found.");
				try {
					FileWriter fileWriter = new FileWriter("trans" + id + ".txt");
					System.out.println("<Client" + this.id + ">: trans" + id + ".txt is being generated.");
					fileWriter.write("T[1,1]: begin(T1); X:=20;Write(X);Commit(T1);\n");
					fileWriter.flush();
					fileWriter.write("T[2,3]: begin(T2); Read(X);Commit(T2);\n");
					fileWriter.flush();
					fileWriter.close();
				} catch (IOException f) {
					f.printStackTrace();
				}
			}
			
			//open serverSocket on client to receive confirmation of transaction from the server
			try {
				serverSocket = new ServerSocket(port, 0, InetAddress.getByName("127.0.0.1"));
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
		
		public boolean isClosed() {
			return this.clientClosed;
		}
		
		public void run() {
			System.out.println("<Client" + id + ">: Thread" + id + " is running!");
			//read trans.txt, while there is a next line
			String transaction;
			try {
				BufferedReader fileIn = new BufferedReader(new FileReader("trans" + id + ".txt"));
				try {
					while ((transaction = fileIn.readLine()) != null) {
						System.out.println("<Client" + this.id + ">: read transaction \"" + transaction + "\".");
						int toServerIndex;
						int toServer=0;
						try {
							toServerIndex = transaction.indexOf(",");
							toServer = Integer.parseInt(transaction.substring(toServerIndex+1,toServerIndex+2));
						} catch (NumberFormatException n){
							n.printStackTrace();
							System.err.println("<Client" + this.id + ">: attempting to parse server id from transaction and failed");
							System.exit(1);
						}	
						//connect to specified server
						Socket socket = this.connect(toServer);
						InputStream is = socket.getInputStream();
						OutputStream os = socket.getOutputStream();
						//send transaction
						PrintWriter out = new PrintWriter(os, true);
						BufferedReader in = new BufferedReader(new InputStreamReader(is)); 
						out.println(transaction);
						
						//await response
						String serverResponse = in.readLine();
						
						//parse response and output
						System.out.println("<Client" + this.id + ">: received response from server " + toServer + ": \"" + serverResponse + "\"");												
						
					}
					System.out.println("<Client" + this.id + ">: reached end of transactions in trans" + id + ".txt");
				} catch (IOException e) {
					e.printStackTrace();
					System.exit(1);
				}	
			} catch (FileNotFoundException f) {
				f.printStackTrace();
				System.exit(1);
				
			}
			System.out.println("<Client" + this.id + ">: closing client");
			//Thread.currentThread().interrupt();
			clientClosed = true;
			checkClose();
		}
		
		private Socket connect(int serverID) {
			Socket socket = null;
			try {
				socket = new Socket(InetAddress.getByName("127.0.0.1"), startPort + serverID);
			} catch (IOException e) {
				System.err.println("<Client" + this.id + ">: attempted to connect to Server" + serverID + " and failed.");	
				e.printStackTrace();
				System.exit(1);
			}
			return socket;
		}
	}
	
    public static void main(String[] args){
        //check for incorrect numbers of arguments, 3 expected
		if (args.length != 3) {
            usage();
        }
		
		//parse and validate arguments
		try {
			n = Integer.parseInt(args[0]);
		} catch (NumberFormatException e ){
			System.err.println("<Number of servers>: \"" + args[0] + "\" is not an integer value");
			System.exit(1);			
		}
		if ((n<4) || (n>10)) {
			System.err.println("<Number of servers>: \"" + n + "\" is not an integer between 4 and 10");
			System.exit(1);
		}	
		
		try {
			Qw = Integer.parseInt(args[1]);
		} catch (NumberFormatException e ){
			System.err.println("<Qw>: \"" + args[1] + "\" is not an integer value");
			System.exit(1);
		}
		if (Qw <= 0) {
			System.err.println("<Qw>: \"" + Qw + "\" is not a non-zero positive integer");
			System.exit(1);
		}	
		
        try {
			Qr = Integer.parseInt(args[2]);
		} catch (NumberFormatException e ){
			System.err.println("<Qr>: \"" + args[2] + "\" is not an integer value");
			System.exit(1);
		}
		if (Qr <= 0) {
			System.err.println("<Qr>: \"" + Qr + "\" is not a non-zero positive integer");
			System.exit(1);
		}	
		
		if (!(Qw + Qr > n)) {
			System.err.println("<CS346>: invalid values of <Qw> and <Qr> for <Number of servers> = " + n + "");
			System.err.println("<CS346>: <Qr> + <Qw> must be greater than <Number of servers>");
			System.exit(1);			
		}
		
		if (!(2*Qw > n)) {
			System.err.println("<CS346>: invalid values of <Qw> for <Number of servers> = " + n + "");
			System.err.println("<CS346>: <Qw> * 2 must be greater than <Number of servers>");
			System.exit(1);			
		}
		
		System.out.println("<CS346>: received and accepting n as " + n + ", Qw as " + Qw + ", and Qr as " + Qr);

		//instantiate CS246
		CS346 cs346 = new CS346();
		
		//spawn servers
		cs346.spawnServers();
		cs346.startServers();
		
		//spawn and start clients
		cs346.spawnClients();
		
		while (cs346.checkClose() == false) {
			//wait until clients are done
			try {
				TimeUnit.SECONDS.sleep(1);
			} catch (InterruptedException i) {
				System.err.print(i.getMessage());
			}
		}
		close = true;
    }
    
	public void spawnServers() {
		System.out.println("<CS346>: spawning " + n + " servers.");
		servers = new Server[n];		
		//for 1 to <number of servers>
		for (int i=0; i<n; i++){
			//spawn servers
			int serverID = i+1;
			int serverPort = startPort + serverID;
			servers[i] = new Server(serverID, serverPort);
		}
	}

	public boolean checkClose() {
		if (clients[0].isClosed() && clients[1].isClosed()) {
			//close the program
			close = true;
			return true;
		}
		else {
			return false;
		}
	}
	
	public void startServers() {
		for (int i=0; i<n; i++){
			Thread thread = new Thread(servers[i]);
			thread.start();
		}	
	}

	public void spawnClients() {
		System.out.println("<CS346>: spawning 2 clients.");
		clients = new Client[2];
		clients[0] = new Client(1, startPort + 11);
		clients[1] = new Client(2, startPort + 12);
		Thread thread1 = new Thread(clients[0]);
		Thread thread2 = new Thread(clients[1]);
		thread1.start();
		thread2.start();
	}

	public int getTimestamp() {
		return timestamp;
	}
	
	public void incrementTimestamp() {
		timestamp += 1;
	}	
}

import java.io.IOException;
import java.io.FileNotFoundException;
import java.net.SocketTimeoutException;
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
	//serverThreads is used to start the threads
	private Thread[] serverThreads;
	private static boolean close = false;
    
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
			String startMessage = "<LogManager>: the log of Server " + serverID + " begins here.";
			try {
				fileWriter.write(startMessage);
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
				logger.write("ServerSocket open on port " + port);
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
		
		//acquires a write or read quorum based on the param
		private Socket[] quorum(int size) {
			Socket[] quorumSockets = new Socket[size -1];
			//for each server required for a quorum
			for (int i=0; i<quorumSockets.length; i++) {
				//loop until the slot is filled
				boolean serverConnected = false;
				while (!serverConnected) {
					for (int j=1; j<=n; j++) {
						if (!serverConnected) {
							if (j != id) {
								Socket socket = new Socket();
								try {
									//connect with timeout 
									socket.connect(new InetSocketAddress("127.0.0.1", startPort + j), 15);
									if (socket.isConnected()) {
										serverConnected = true;
										quorumSockets[i] = socket;
										OutputStream os = socket.getOutputStream();
										PrintWriter out = new PrintWriter(os, true);
										out.println("quorum locked by server " + id);
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
					}
				}	
			}
			return quorumSockets;
		}
		
		private int read(Socket[] quorum) {
			//todo
			//check the fucking thing of all of the fuckers
			return 0;
		}
		
		private void write(Socket quorum, int x) {
			//
			//command all of them to write, the bastards them
		}
		
		private int set(String toBeConverted) {
			//to do
			//parse the string you fucker
			return 0;
		}
		
		public void run() {
			logger.write("Running and ready for operation.");
			String received;
			//continue server operations until requested to close
			while (!close) {
				try {
					//block until connection is made
					Socket socket = serverSocket.accept();
					//lock 
						//required? if blocking perhaps not
					InputStream is = socket.getInputStream();
					OutputStream os = socket.getOutputStream();
					PrintWriter out = new PrintWriter(os, true);
					BufferedReader in = new BufferedReader(new InputStreamReader(is));
					
					//standardise input, remove whitespace change to lower
					received = in.readLine();
					// System.out.println(received);
					logger.write("received \"" + received + "\".");
					received = received.trim().toLowerCase().replaceAll(" ","");
					// System.out.println(received);
					logger.write("format standardised to \"" + received + "\".");
						
					//client connection	
					if (received.startsWith("t[")) {
						//do transaction
						if (received.contains("write")){
							//a write quorum is required
							//acquire quorum
							Socket[] quorum = quorum(qw);
						}
						else if (received.contains("read")){
							//a read quorum is required 
							//acquire quorum
							Socket[] quorum = quorum(qr);
						}
						else {
							//no quorum is required
							Socket[] quorum = new Socket[0];
						}
						
						int index = received.indexOf(":") + 1;
						String trans = received.substring(index);
						//peel transaction string until commit
						
						while (!trans.equals("")) {
							//on begin
								// do nothing
							//on read
								//get read quorum
								//x = data item
							//on write
								//get write quorum
								//have all write in parallel
								//update timestamp and latestTimestamp
								//set data item
							//on set data
								//x = var
							//on commit
								//close quorum sockets
							
						}
							
					} 					
					//server connection
					else if (received.startsWith("q")) {
						//wait on server instruction
							//update dataItem
							//send var and timestamp
							//release
						
					}	
				} catch (IOException e) {
					e.printStackTrace();
					System.exit(1);
				}
			}
			
		}
		
		public boolean lock(){
			if (isLocked()) return false;
			else {
				//lock and return
				this.lock = 1;
				return true;
			}
		}
		
		public boolean unlock(){
			if (!isLocked()) return false;
			else {
				//unlock and return
				this.lock = 0;
				return true;
			}
		}
		
		private boolean isLocked() {
			if (lock == 0)
				return false;
			else
				return true;
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
		
		public Client(int id, int port){
			this.id = id;
			this.port = port;
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
							toServerIndex = transaction.indexOf("[");
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
						System.out.println("<Client" + this.id + ">: received response from server" + toServer + ": \"" + serverResponse + "\"");												
						
						//sleep used to test if threads were concurrent or not
						//SPOILER: they are
						// try {
							// Thread.sleep(1000);
						// } catch (InterruptedException i) {
							// i.printStackTrace();
						// }
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

	public void startServers() {
		serverThreads = new Thread[n];
		for (int i=0; i<n; i++){
			serverThreads[i] = new Thread(servers[i]);
			serverThreads[i].start();
		}	
	}

	public void spawnClients() {
		System.out.println("<CS346>: spawning 2 clients.");
		Client client1 = new Client(1, startPort + 11);
		Client client2 = new Client(2, startPort + 12);
		Thread thread1 = new Thread(client1);
		Thread thread2 = new Thread(client2);
		thread1.start();
		thread2.start();
	}

	public static void close() {
		close = true;
	}
	
}

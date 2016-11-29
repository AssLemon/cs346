import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.FileWriter;
import java.io.FileReader;
import java.io.PrintWriter;
import java.io.BufferedReader;
import java.io.InputStreamReader;


public class CS346 {

    public static InputStream is;
    public static OutputStream os;
	private static int Qw;
	private static int Qr;
	private static int n;
	private Server[] servers;
    
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
	private class Server {
		private int id;
		private int port;
		private int dataItem;
		private ServerSocket serverSocket;
		private int lock;
		private LogManager logger;
		
		public Server(int id, int port) {
			//set variables
			this.id = id;
			this.port = port;
			this.dataItem = 0;
			this.lock = 0;
			//create a LogManager
			this.logger = new LogManager(this.id);
			//try to open ServerSocket
			try {
				serverSocket = new ServerSocket(port, 0, InetAddress.getByName("127.0.0.1"));
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			}	
			logger.write("ServerSocket open on port " + port);
		}
		
		//to do
		//wait until unlocked and then getDataItem()
		public int read() {
			return 0;
		}
		
		//to do
		//wait until unlocked and then setDataItem(int x)
		public boolean write(int x) {
			return false;
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
		
		public Client(int id, int port){
			this.id = id;
			this.port = port;
			//if trans.txt does not exist, create default
			try {
				FileReader fileReader = new FileReader("trans.txt");
				System.out.println("<Client" + this.id + ">: trans.txt has been found.");
			} catch (FileNotFoundException e) {
				System.out.println("<Client" + this.id + ">: trans.txt has not been found.");
				try {
					FileWriter fileWriter = new FileWriter("trans.txt");
					System.out.println("<Client" + this.id + ">: trans.txt is being generated.");
					fileWriter.write("T[1,1]: begin(T1); X:=20;Write(X);Commit(T1);\n");
					fileWriter.flush();
					fileWriter.write("T[2,3]: begin(T2); Read(X);Commit(T2);\n");
					fileWriter.flush();
					fileWriter.close();
				} catch (IOException f) {
					f.printStackTrace();
				}
			}
			//open port for comms
		}
		
		public void run() {
			System.out.println("<Client" + id + ">: Thread" + id + " is running!");
			//read trans.txt, while there is a next line
			String transaction;
			try {
				BufferedReader fileIn = new BufferedReader(new FileReader("trans.txt"));
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
						Socket socket = this.connect(9000 + toServer);
						InputStream is = socket.getInputStream();
						OutputStream os = socket.getOutputStream();
						//send transaction
						PrintWriter out = new PrintWriter(os, true);
						BufferedReader in = new BufferedReader(new InputStreamReader(is)); 
						out.println(transaction);
						
						//await response
						String serverResponse = in.readLine();
						
						//parse response and output
						System.out.println("<Client" + this.id + ">: recieved response from server" + toServer + ": \"" + serverResponse + "\"");												
						
						//sleep used to test if threads were concurrent or not
						//SPOILER: they are
						// try {
							// Thread.sleep(1000);
						// } catch (InterruptedException i) {
							// i.printStackTrace();
						// }
					}
				} catch (IOException e) {
					e.printStackTrace();
				}	
			} catch (FileNotFoundException f) {
				f.printStackTrace();
			}
		}
		
		private Socket connect(int serverID) {
			Socket socket = null;
			try {
				socket = new Socket(InetAddress.getByName("127.0.0.1"), 9000 + serverID);
			} catch (IOException e) {
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
		
		//
		cs346.spawnClients();
    }
    
	public void spawnServers() {
		System.out.println("<CS346>: spawning " + n + " servers.");
		servers = new Server[n];		
		//for 1 to <number of servers>
		for (int i=0; i<n; i++){
			//spawn servers
			int serverID = i+1;
			int serverPort = 9000 + serverID;
			servers[i] = new Server(serverID, serverPort);
		}
	}
		
	public void spawnClients() {
		System.out.println("<CS346>: spawning 2 clients.");
		Client client1 = new Client(1, 11);
		Client client2 = new Client(2, 12);
		Thread thread1 = new Thread(client1);
		Thread thread2 = new Thread(client2);
		thread1.start();
		thread2.start();
	}
}
    // private static void serverMain(String[] args) throws IOException {
	// if (args.length != 4) {
            // usage();
        // }
        
        // int serverListenPort = Integer.parseInt(args[1]); // The port that this server should listen on to accept connections from other servers
        // int clientListenPort = Integer.parseInt(args[2]); // The port that this server should listen on to accept connections from clients
        // InetSocketAddress[] otherServerAddresses = Server.parseAddresses(args[3]); // The addresses of the other servers
        // String databasePath = "test"; // The path to the database file
        
        // Server server = new Server(databasePath);
        
     
	// try {
	
	    // if (serverListenPort == 30001) {
		// System.out.println("<server> Accepting client on " + clientListenPort);
			
		// server.connectServers(otherServerAddresses);
		//server.acceptClients(clientListenPort);	
	    // }
	    // else {
		// System.out.println("<server> Accepting coordinator on " + serverListenPort);
	
		// ServerSocket serverSocket = new ServerSocket(serverListenPort);
		
		// try {
		    // Socket socket = serverSocket.accept();
		    // System.out.println("<server> Successfully connected to coordinator");
		// } finally {
		      // serverSocket.close();
		// }
	    // }
			
	    // while (true){
			
	    // }
	// } catch (IOException e) {
	    // System.err.println(e);
	    // e.printStackTrace(System.err);
	// } finally {
	     //server.close();
	      
	// }
// }
// }

/* 	 private class Server {

		// private String databasePath;

		public Server(String databasePath) throws IOException {
		  this.databasePath = databasePath;
		}

		public void acceptServers(int port) throws IOException {
		
		System.out.println("Accepting servers on port: " + port);
		  
		ServerSocket serverSocket = new ServerSocket(port); 
			Socket socket = serverSocket.accept();
		
			System.out.println("Successfully accepted servers on port: " + port);        	
		
		}
		
		public void connectServers(InetSocketAddress[] servers) throws IOException {
		
		System.out.println("Attempting to connect to other servers");
		
		Socket socket = new Socket(); // Socket to connect to first server
		Socket socket2 = new Socket(); // Socket to connect to second server
			  
		socket.connect(servers[0]); // Connect to first server 
		System.out.println("Successfully connected to server: " + servers[0]);
		socket2.connect(servers[1]); // Connect to second server
		System.out.println("Successfully connected to server: " + servers[1]);

		/*InputStream is = socket.getInputStream();	// Create input/output stream 
		OutputStream os = socket.getOutputStream();	// for first server
		InputStream is2 = socket2.getInputStream();	// Create input/output stream
		OutputStream os2 = socket2.getOutputStream();	// for second server
		iStreams = new InputStream[servers.length];
		oStreams = new OutputStream[servers.length];

		iStreams[0] = is;
		iStreams[1] = is2;	// Store the input/output streams
		oStreams[0] = os;	// in their resoective arrays
		oStreams[1] = os2;*/
		
		// }
		
		// public InetSocketAddress[] parseAddresses(String input) {
			// if (input.equals("NULL")) {
				// return new InetSocketAddress[0];
			// }
			// String[] rawAddresses = input.split(",");
			// InetSocketAddress[] addresses = new InetSocketAddress[rawAddresses.length];
			// for (int i = 0; i != rawAddresses.length; ++i) {
				// String[] addrParts = rawAddresses[i].split(":");
				// addresses[i] = new InetSocketAddress(addrParts[0], Integer.parseInt(addrParts[1]));
			// }
			// return addresses;
		// }

	// } */

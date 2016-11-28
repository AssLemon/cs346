//import dcs.os.Server;
//import dcs.os.StockList;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;


public class cs346 {

    public static InputStream is;
    public static OutputStream os;
    
    public static void usage() {
        System.out.println("Usage:");
        System.out.println("\tjava Node server <server port> <client port> <other servers> <database path>");
        System.out.println("\t\t<server port>: The port that the server listens on for other servers");
        System.out.println("\t\t<client port>: The port that the server listens on for clients");
        System.out.println("\t\t<other servers>: A comma separated list of the other server's addresses and ports");
        System.out.println("\t\t\tExamples:");
        System.out.println("\t\t\t - 'localhost:9001'");
        System.out.println("\t\t\t - 'localhost:9001,127.0.0.1:9002'");
        System.out.println("\t\t<database path>: The path to the database file, this needs to be unique per server");
        System.out.println("\t\t\tExamples:");
        System.out.println("\t\t\t - 'db1.txt'");
        System.exit(1);
    }
    
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            usage();
        }
        String program = args[0];
        switch (program) {
            case "server": serverMain(args); break;
            default: System.err.println("Unknown program type '" + program + "'"); break;
        }
    }
    
    private static void serverMain(String[] args) throws IOException {
	if (args.length != 4) {
            usage();
        }
        
        int serverListenPort = Integer.parseInt(args[1]); // The port that this server should listen on to accept connections from other servers
        int clientListenPort = Integer.parseInt(args[2]); // The port that this server should listen on to accept connections from clients
        InetSocketAddress[] otherServerAddresses = Server.parseAddresses(args[3]); // The addresses of the other servers
        String databasePath = "test"; // The path to the database file
        
        Server server = new Server(databasePath);
        
     
	try {
	
	    if (serverListenPort == 30001) {
		System.out.println("<server> Accepting client on " + clientListenPort);
			
		server.connectServers(otherServerAddresses);
		//server.acceptClients(clientListenPort);	
	    }
	    else {
		System.out.println("<server> Accepting coordinator on " + serverListenPort);
	
		ServerSocket serverSocket = new ServerSocket(serverListenPort);
		
		try {
		    Socket socket = serverSocket.accept();
		    System.out.println("<server> Successfully connected to coordinator");
		} finally {
		      serverSocket.close();
		}
	    }
			
	    while (true){
			
	    }
	} catch (IOException e) {
	    System.err.println(e);
	    e.printStackTrace(System.err);
	} finally {
	     // server.close();
	      
	}
}
}

class Server {

    private String databasePath;

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
	
    }
    
    public static InetSocketAddress[] parseAddresses(String input) {
        if (input.equals("NULL")) {
            return new InetSocketAddress[0];
        }
        String[] rawAddresses = input.split(",");
        InetSocketAddress[] addresses = new InetSocketAddress[rawAddresses.length];
        for (int i = 0; i != rawAddresses.length; ++i) {
            String[] addrParts = rawAddresses[i].split(":");
            addresses[i] = new InetSocketAddress(addrParts[0], Integer.parseInt(addrParts[1]));
        }
        return addresses;
    }

}
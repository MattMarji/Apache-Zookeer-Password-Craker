import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.net.Socket;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;

public class FileServer extends Thread{

	private static final String ZK_REQUESTS = "/requests";
	private static final String ZK_JOBS = "/jobs";
    private static final String ZK_FS = "/fs";
    private static final String ZK_WORKERS = "/workers";
    static String fileServerPath, myPath;
    static boolean isLeader = false;
    CountDownLatch fsRootCreatedLatch = new CountDownLatch(1);
    CountDownLatch fileserverLatch = new CountDownLatch(1);

	//We need to store this machine's IP and port in the znode so the clients can get that information through the zookeeper and the znodes.
	//Otherwise they won't be able to connect.

	//TODO: find pathname for this badboy
	File dictionary = new File("lowercase.rand");

	private List<String> dict = null;
	private String IP;
	private Integer port;
    //ZooKeeper resources
    private static ZkConnector zkc;
    private static ZooKeeper zk;
    public static ServerSocket SS;
    public static List<String> fileServers = null;

	public FileServer(String connection){

		this.zkc = new ZkConnector();

        try {
            // Client sends "HOST:PORT" of Zookeeper service.
            zkc.connect(connection);
        } catch(Exception e) {
            System.out.println("Zookeper connect " + e.getMessage());
        }

        // Get zk Object
        this.zk = zkc.getZooKeeper();

		try{
			this.dict = loadDictionary(dictionary);
			this.SS = new ServerSocket(0);
		} catch (Exception e){
			System.out.println(e.getMessage());
		}



		run();
	}

	public static void main(String[] args){
		FileServer FS = new FileServer(args[0]);
	}

	@Override
	public void run() {

		byte[] data = null;
		String nodeData = null, fsPath = null, forNode = null;
		Stat stat = null;

		try {
			// Check to ensure that the /fs node exists.
			try{
				System.out.println("Trying to find root node");
				stat = zk.exists(ZK_FS, new Watcher() {
	                @Override
	                public void process(WatchedEvent event) {
	                    if (event.getType() == Event.EventType.NodeCreated) {
	                        fsRootCreatedLatch.countDown();
	                    }
	                }
	            });
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}


	            if (stat == null) {
	            	try{
		                System.out.println("Going to sleep waiting for root /fs");
		                fsRootCreatedLatch.await();
		                System.out.println("root /fs exists");

					} catch (Exception e){
						System.out.println(e.getMessage());
					}

	            }

	            //As soon as the root node comes up I need to make my own znode.
	            try{

	            	InetSocketAddress sockAddr = (InetSocketAddress) SS.getLocalSocketAddress();
					//this.IP = sockAddr.getAddress().getHostAddress();
					this.IP = InetAddress.getLocalHost().getHostAddress();
					this.port = SS.getLocalPort();
					System.out.println("IP is: " + this.IP);

					//TODO: Create my zNode here
					forNode = this.IP + ":" + this.port.toString();

					data = forNode.getBytes();

					myPath = zk.create(
		                    ZK_FS + "/",
		                    data,
		                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
		                    CreateMode.EPHEMERAL_SEQUENTIAL);

					myPath = myPath.split("/")[2];
					System.out.println("My path is: " + myPath);

					//Now that I've made my znode, get all children of the root
					fileServers = zk.getChildren(ZK_FS, false);

					System.out.println("The File Servers are: " + fileServers + " line 143");

					Collections.sort(fileServers);
					fileServerPath = fileServers.get(0);

					System.out.println("The Primary FS is: " + fileServerPath);

					if(fileServerPath.equals(myPath)){
						isLeader = true;
						System.out.println("I'm the leader!");
					} else {
						isLeader = false;
						System.out.println("I am NOT the leader");
					}
	            } catch (Exception e) {
	            	System.out.println(e.getMessage());
	            }

	        // If we are the leader, we begin to listen, else we watch on the person to our left.
	       	if (!isLeader) {
	       		// Indefinite watch until we are the leader!
	            String path;
	            int myIndex;
	       		while(true) {
	       			// set a watch on the fs to our left. We only become leader if we are first in the /fs list.

	                // Setup a watch on the node that is one smaller than this.
	                try {
	                    fileServers = zk.getChildren(ZK_FS, false);
	                } catch(KeeperException e) {
	                    System.out.println(e.code());
	                } catch(Exception e) {
	                    System.out.println(e.getMessage());
	                }

	                // Order the jobtrackers...
	                Collections.sort(fileServers);

	                /*/ We know we are the jobtracker @ jobTrackerPath
	                // Get index of...
	                path = fileServerPath.split("/")[2];*/

	                myIndex = fileServers.indexOf(myPath);

	                System.out.println("File Servers: " + fileServers + " My path: " + myPath + " My index: " + myIndex + " line 190");

	                if (myIndex != -1) {
	                    // We want to watch the previous node @ myIndex-1
	                    // We know that we are a backup so we are not first in the list. Thus myIndex-1 will always work.
	                    String watchingNode = ZK_FS + "/" + fileServers.get(myIndex-1);

	                    System.out.println("We will be watching: " + watchingNode);

	                    // Setup a watch on the smaller znode.
	                    try {
	                     zk.exists(watchingNode, new Watcher() {
	                        @Override
	                        public void process(WatchedEvent event) {
	                            if (event.getType().equals(EventType.NodeDeleted)) {
	                                fileserverLatch.countDown();
	                            }
	                        }
	                    });

	                    } catch(KeeperException e) {
	                        System.out.println(e.code());
	                    } catch(Exception e) {
	                        System.out.println(e.getMessage());
	                    }
	                    // Sit and wait for a result!!
	                    try{
	                        System.out.println("Watching File Server: " + watchingNode);
	                        fileserverLatch.await();
	                        //filserverLatch = new CountDownLatch(1);

	                         // Job Tracker Down!
	                        System.out.println("File Server Down! Determine our position...");

	                        // Countdown occured.
	                        // Determine if we are the new leader
	                        try {
	                            fileServers = zk.getChildren(ZK_FS, false);
	                        } catch(KeeperException e) {
	                            System.out.println(e.code());
	                        } catch(Exception e) {
	                            System.out.println(e.getMessage());
	                        }

	                        // Order the jobtrackers...
	                        Collections.sort(fileServers);

	                        // If we are the first in the list -- we are the new leader! Set leader flag and start RequestListener Thread.

	                        if (fileServers.indexOf(myPath) == 0) {
	                            System.out.println("We are the new leader!");
	                            isLeader = true;
	                            break;
	                        } else {
	                            System.out.println("Still a backup...");
	                        }

	                        // Setup Latch again, we are still a backup!
	                        fileserverLatch = new CountDownLatch(1);

	                    } catch(Exception e) {
	                        System.out.println(e.getMessage());
	                    }

	                } else {
	                    System.exit(-1);

	                }
	       		}
	       	}




			while(true){
				//As the FS, we always need to be willing to accept new connections
				Socket workerSock = SS.accept();

				System.out.println("FileServer accepted new connection.");

				//We want a new thread to listen to each connection, so we can handle all reqs.
				new Thread(this.new WorkerHandlerThread(workerSock, dict)).start();
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}


	//This only needs to be called and run one time when the file server is started.
	public List<String> loadDictionary(File dictFile) throws FileNotFoundException{
		FileReader fr;
		BufferedReader bfr;
		String line;
		List<String> dict = new ArrayList<String>();

		if(dictFile.exists()){
			System.out.println("Dictfile exists");
			fr  = new FileReader(dictFile);
			bfr  = new BufferedReader(fr);

			//while we haven't reached the end of the file keep reading it
			//Each new line has a new word that we need to add to our dictionary.
			try {
				line = bfr.readLine();
				while(line != null) {
					dict.add(line);
					line = bfr.readLine();
				}

				//This means that we've read the entire dictionary file, and have it memory. Return.
				System.out.println("Size of dict: " + dict.size());
				return dict;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} else {
			System.out.println("Dictfile does NOT exist");
			throw new FileNotFoundException();
		}
		System.out.println("Returning null");
		return null;
	}



	public class WorkerHandlerThread implements Runnable {
		public Socket wSock;
		public String startIdx;
		public String endIdx;
		public String workerID;
		public List<String> dict;
		public List<String> partition;
		private String[] partArr;
		private String incoming;
		private String outgoing;
		private String[] splitString;

		public WorkerHandlerThread(Socket wSock, List<String> dict){
			this.wSock = wSock;
			this.dict = dict;
		}

		public void run() {

			try{
				ObjectOutputStream out = new ObjectOutputStream(wSock.getOutputStream());
				ObjectInputStream in = new ObjectInputStream(wSock.getInputStream());


				incoming = (String) in.readObject();

				System.out.println("INCOMING: " + incoming);

				splitString = incoming.split(":");

				workerID = splitString[0];
				startIdx = splitString[1];
				endIdx = splitString[2];

				System.out.println(workerID + " " + startIdx + " " + endIdx);

				partition = allocatePartition(Integer.parseInt(startIdx), Integer.parseInt(endIdx));

				partArr = new String[partition.size()];

				partArr = partition.toArray(partArr);

				out.writeObject(partArr);

				wSock.close();
			} catch (Exception e){
				System.out.println(e.getMessage());
			}

		}

		public List<String> allocatePartition(Integer start, Integer end){
			 return dict.subList(start, end);
		}



}
}

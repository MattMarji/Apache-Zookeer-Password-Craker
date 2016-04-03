import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;
import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class JobTracker extends Thread implements Watcher {


    static String ZK_REQUESTS = "/requests";
    static String ZK_JOBS = "/jobs";
    static String ZK_FS = "/fs"
    static String ZK_WORKERS = "/workers";

    static final int NUM_WORDS = 265744;

    // ZooKeeper resources
    ZkConnector zkc;
    ZooKeeper zk;

    // Request resources
    CountDownLatch childrenChangedLatch = new CountDownLatch(1);

    // JobTracker
    static String jobTrackerPath;
    static String JT_TRACKER = "/jt";
    static String JT_PRIMARY = "P";
    static String JT_BACKUP = "B";
    static CountDownLatch nodeCreatedLatch = new CountDownLatch(1);

    // Client Tracking
    Semaphore clientWait = new Semaphore(1);
    static ArrayList<String> clientList = new ArrayList<String>();
    // Job tracking --> Client ID: hash1,hash2,hash3 etc..
    static HashMap <String, ArrayList<String>> jobs = new HashMap<String, ArrayList<String>>();

    // Constructor
    public JobTracker(String connection) {

        zkc = new ZkConnector();

        try {
            // Client sends "HOST:PORT" of Zookeeper service.
            zkc.connect(connection);
        } catch(Exception e) {
            System.out.println("Zookeper connect " + e.getMessage());
        }

        // Get zk Object
        zk = zkc.getZooKeeper();

        // Setup zNodes...
        createZNodes();

    }

    public void createZNodes() {
        Stat stat;
        try {
            String trackerPath;

        // ========================== JOBTRACKER NODE SETUP ==================
            // Determine if a tracker exists. No need for a watch.
            stat = zk.exists(ZK_TRACKER, false);

            // Does not exist!
            if (stat == null) {
                // Create root node for trackers...
                zk.create(
                    ZK_TRACKER,
                    null,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);

                // Since tracker did not exist, we are first. Become Primary.
                // Path: /tracker/P
                jobTrackerPath = zk.create(
                    ZK_TRACKER + "/" + TRACKER_PRIMARY,
                    null,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL
                    );
            }

            // /tracker exists --> if it has a child already assume it has a leader, else take over as leader!

            //TO-DO: Handle election process...
            else {
                if (stat.getNumChildren() == 0) {
                   jobTrackerPath = zk.create(
                    ZK_TRACKER + "/" + TRACKER_PRIMARY,
                    null,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL
                    );
               } else {
                   jobTrackerPath = zk.create(
                    ZK_TRACKER + "/" + TRACKER_BACKUP,
                    null,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL
                    );
               }
            }
        // =============================================================

            // Create /requests
            if (zk.exists(ZK_REQUESTS, false) == null) {
                zk.create(ZK_REQUESTS,
                        null,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            }

            // Create /fs
            if (zk.exists(ZK_FS, false) == null) {
                zk.create(ZK_FS,
                        null,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            }

            // Create /jobs
            if (zk.exists(ZK_JOBS, false) == null) {
                zk.create(ZK_JOBS,
                        null,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            }

            // Create /workers
            if (zk.exists(ZK_WORKERS, false) == null) {
                zk.create(ZK_WORKERS,
                        null,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            }

        } catch (KeeperException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public class RequestListener implements Runnable {
        @Override
        public void run() {
            List<String> requests;
            Stat stat;
            byte[] data;
            String nodeData;

            while(true) {

                requests = zk.getChildren(ZK_REQUEST, new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                        if (event.getType == Event.EventType.NodeChildrenChanged) {
                            // There has been a change in the number of jobs.
                            childenChanged.countDown();
                        }
                    }
                }

                // Sit and wait for a result!!
                try{
                    childrenChanged.await();
                    requests = zk.getChildren(ZK_REQUEST, false);
                } catch(Exception e) {
                    System.out.println(e.getMessage());
                }
                );

                // COMPLETION OF REQUEST SHOULD OCCUR IN ANOTHER THREAD
                // Sort the jobs in order incase they are not.
                // FROM API DOCS: The list of children returned is not sorted and no guarantee is provided as to its natural or lexical order.
                Collections.sort(requests);
                System.out.println("requests: " + requests.toString());

                // Last request must be a new request.
                String request = requests.get(requests.size-1), result = null;
                stat = new Stat();
                data = zk.getData(request_path + request, false, stat);

                // Just to be sure!!
                if (stat != null) {
                    nodeData = byteToString(data);

                    // Determine if this is a job or status.
                    String[] resultArr = nodeData.split(":");

                    if (resultArr[2].equals("100")) {
                    	// This is a job -- add to /jobs.
                    	handleJob(resultArr[0]);
                    } else if (resultArr[2].equals("101")) {
                    	// This is a status -- get job status.
                    	handleStatus(request_path + request, resultArr[0]);
                    } else {
                    	System.out.println("Unknown Task Type!");
                    }
                }

            }
        }
    }

    // Handle a new job
    public void handleJob(String pwHash) {
        String task = String.format("%s:%s",  pwHash, "ongoing");

        // Set data string to bytes
        byte[] byteData = null;
            if(data != null) {
                byteData = task.getBytes();
            }

        // Create a new job...
        String jobPath = zk.create(
            ZK_JOBS + "/",
            byteData,
            ZooDefs.Ids.OPEN_ACL_UNSAFE,
            CreateMode.PERSISTENT_SEQUENTIAL);

        // Determine the number of workers
        Stat stat = zk.exists(ZK_WORKERS, false);
        int numWorkers = stat.getNumChildren();

        int partition_size = NUM_WORDS / numWorkers;
        int word_remainder = NUM_WORDS % numWorkers;

        // For each worker, create a task with start and end indices
        for (int i = 0; i<numWorkers; i++) {
            // Create a task under the job.
            // owner: -1, start: i*part_size, end: (i + 1)* part_size

            int startIdx = i * partition_size;
            int endIdx;

            if (i == (numWorkers-1) && (word_remainder != 0)) {
            	 endIdx = word_remainder;
            } else {
            	endIdx = ((i + 1) * partition_size) -1;
            }

            String task = String.format("%s:%s:%s", "-1", Integer.toString(startIdx), Integer.toString(endIdx));

            // Set data string to bytes
            byte[] byteData = null;
                if(data != null) {
                    byteData = task.getBytes();
                }

            zk.create(
                jobPath + "/",
                byteData,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT)
        }
    }

    // Get the status of a job
    public void handleStatus(String, requestPath, String pwHash) {

    	String nodeData;
        Byte[] data;
    	Stat stat;

    	// We have the password hash -> traverse jobs.
    	List<String> jobs = zk.getChildren(ZK_JOBS, false);

    	// jobs has a list of paths.
    	Collections.sort(jobs);
        System.out.println("jobs: " + requests.toString());

        // Traverse each job, and get the Stat.
        for (String path: jobs) {
        	stat = new Stat();
            data = zk.getData(ZK_JOBS + "/" + path, false, stat);

            // Just to be sure!!
            if (stat != null) {
                nodeData = byteToString(data);

                // Determine if this is the job we are looking for.
                String[] resultArr = nodeData.split(":");

                if (resultArr[0].equals(pwHash)) {
                	// We found the matching job! Get the status.
                    String status = resultArr[1];

                    // Setup the new data!
                    String newData = String.format("%s:%s:%s", pwHash, status, "101");

                    // Set the data in the node.
                    stat = zk.setData(
                        requestPath,
                        newData.getBytes(),
                        -1
                        );
                }

            }
        }
    }

    @Override
    public void process(WatchedEvent event) {
        // JobTracker watcher: watches if primary jt fails, makes self primary
        boolean isNodeDeleted;
        try {
            isNodeDeleted = event.getType().equals(EventType.NodeDeleted);
            String nodeName = event.getPath().split("/")[2];

            if (mode.equals(TRACKER_BACKUP) // primary failure handling
                    && isNodeDeleted
                    && nodeName.equals(TRACKER_PRIMARY)) {
                debug("detected primary failure, setting self as new primary");
                zk.delete(myPath, 0);                           // remove self as backup
                myPath = ZK_TRACKER + "/" + TRACKER_PRIMARY;    // add self as primary
                mode = TRACKER_PRIMARY;
                zk.create(myPath,
                        null,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL);

                modeSignal.countDown();
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

        JobTracker jt = new JobTracker(args[0]);

        System.out.println("We have been set as: " + jobTrackerPath);

        // If we are backup we sit and wait...
        if (jobTrackerPath == (ZK_TRACKER + "/" + ZK_BACKUP)) {

            // Setup a watch on the primary job tracker.
            zk.exists(ZK_TRACKER + "/" + ZK_PRIMARY, jt);

            // SIGNAL???
            // trackerSignal.await();
        }

        new Thread(jt.new RequestListener()).start();
    }
}

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

public class JobTracker extends Thread {


    static String ZK_REQUESTS = "/requests";
    static String ZK_JOBS = "/jobs";
    static String ZK_FS = "/fs";
    static String ZK_WORKERS = "/workers";

    static final int NUM_WORDS = 265744;

    // ZooKeeper resources
    ZkConnector zkc;
    ZooKeeper zk;

    // Request resources
    CountDownLatch childrenChangedLatch = new CountDownLatch(1);

    // JobTracker
    static String jobTrackerPath;
    static String ZK_TRACKERS = "/jt";
    static boolean isLeader = false;
    static CountDownLatch jobTrackerLatch = new CountDownLatch(1);
    static CountDownLatch nodeCreatedLatch = new CountDownLatch(1);

    // Worker resources
    static CountDownLatch workerLatch = new CountDownLatch(1);

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
        String myPath = null, firstJTPath = null;
        List<String> jobTrackers = new ArrayList<String>();
        try {
            String trackerPath;

        // ========================== JOBTRACKER NODE SETUP ==================
            // Determine if a tracker exists. No need for a watch.
            stat = zk.exists(ZK_TRACKERS, false);

            // Does not exist!
            if (stat == null) {
                // Create root node for trackers...
                zk.create(
                    ZK_TRACKERS,
                    null,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);

                // Since tracker did not exist, we are first. Become Primary.
                // Path:
                jobTrackerPath = zk.create(
                    ZK_TRACKERS + "/",
                    null,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL_SEQUENTIAL
                    );

                myPath = jobTrackerPath.split("/")[2];
                jobTrackers = zk.getChildren(ZK_TRACKERS, false);

                Collections.sort(jobTrackers);

                firstJTPath = jobTrackers.get(0);

                if(firstJTPath.equals(myPath)){
                    isLeader = true;
                }
            }

            // /tracker exists --> if it has a child already assume it has a leader, else take over as leader!

            else {
                // If there are no children, we must be the leader by default.
                if (stat.getNumChildren() == 0) {
                   jobTrackerPath = zk.create(
                    ZK_TRACKERS + "/",
                    null,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL_SEQUENTIAL
                    );

                   myPath = jobTrackerPath.split("/")[2];

                    jobTrackers = zk.getChildren(ZK_TRACKERS, false);

                    Collections.sort(jobTrackers);

                    firstJTPath = jobTrackers.get(0);

                    if(firstJTPath.equals(myPath)){
                        isLeader = true;
                    }

               } else {
                   jobTrackerPath = zk.create(
                    ZK_TRACKERS + "/",
                    null,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL_SEQUENTIAL
                    );

                   isLeader = false;
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

    // Handle a new job
    public void handleJob(String requestPath, String pwHash) {
        String task = String.format("%s:%s",  pwHash, "ongoing");

        // Set data string to bytes
        byte[] byteData = task.getBytes();

        // Create a new job...
        String jobPath = null;

        try {
            jobPath = zk.create(
            ZK_JOBS + "/",
            byteData,
            ZooDefs.Ids.OPEN_ACL_UNSAFE,
            CreateMode.PERSISTENT_SEQUENTIAL);

        } catch(KeeperException e) {
            System.out.println(e.code());
        } catch(Exception e) {
            System.out.println(e.getMessage());
        }

        // Determine the number of workers
        Stat stat = null;
        try {
            stat = zk.exists(ZK_WORKERS, false);
        } catch(KeeperException e) {
            System.out.println(e.code());
        } catch(Exception e) {
            System.out.println(e.getMessage());
        }

        int numWorkers = stat.getNumChildren();

        System.out.println("Number of workers: " + numWorkers);

        int partition_size;
        int word_remainder;

        if (numWorkers == 0) {
            partition_size = NUM_WORDS / 1;
            word_remainder = NUM_WORDS % 1;
        } else {
            partition_size = NUM_WORDS / numWorkers;
            word_remainder = NUM_WORDS % numWorkers;
        }

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

            task = String.format("%s:%s:%s", "-1", Integer.toString(startIdx), Integer.toString(endIdx));

            // Set data string to bytes
            byteData = task.getBytes();

            try {
                zk.create(
                jobPath + "/",
                byteData,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT_SEQUENTIAL);

                task = String.format("%s:%s:%s", pwHash, "ongoing", "200");
                byteData = task.getBytes();

                zk.setData(
                requestPath,
                byteData,
                -1);

                System.out.println("Writing 200 as status of: " + requestPath);
            } catch(KeeperException e) {
                System.out.println(e.code());
            } catch(Exception e) {
                System.out.println(e.getMessage());
            }

        } // end of for loop

        try {
            task = String.format("%s:%s:%s", pwHash, "ongoing", "200");
            byteData = task.getBytes();

            zk.setData(
            requestPath,
            byteData,
            -1);

            System.out.println("Writing 200 as status of: " + requestPath);
        } catch(KeeperException e) {
            System.out.println(e.code());
        } catch(Exception e) {
            System.out.println(e.getMessage());
        }
    }

    // Get the status of a job
    public void handleStatus(String requestPath, String pwHash) {

    	String nodeData;
        byte[] data = null;
    	Stat stat = null;

    	// We have the password hash -> traverse jobs.
        List<String> jobs = null;
        try {
            jobs = zk.getChildren(ZK_JOBS, false);
        } catch(KeeperException e) {
            System.out.println(e.code());
        } catch(Exception e) {
            System.out.println(e.getMessage());
        }

    	// jobs has a list of paths.
    	Collections.sort(jobs);
        System.out.println("jobs: " + jobs.toString());

        // Traverse each job, and get the Stat.
        for (String path: jobs) {

            System.out.println("Getting job node @ path: " + ZK_JOBS + "/" + path);
            try {
                data = zk.getData(ZK_JOBS + "/" + path, false, stat);
            } catch(KeeperException e) {
                System.out.println(e.code());
            } catch(Exception e) {
                System.out.println(e.getMessage());
            }

            nodeData = zkc.byteToString(data);

            System.out.println("job nodeData: " + nodeData);

            // Determine if this is the job we are looking for.
            String[] resultArr = nodeData.split(":");

            if (resultArr[0].equals(pwHash)) {
                System.out.println("Found match...");
            	// We found the matching job! Get the status.
                String status = resultArr[1];

                // Setup the new data!
                String newData = String.format("%s:%s:%s", pwHash, status, "200");

                // IMPORTANT: We need a buffer time to ensure the watch gets setup on the clientDriver before we return a status.
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    System.out.println(e.getMessage());
                }

                // Set the data in the node.
                try {
                    stat = zk.setData(
                    requestPath,
                    newData.getBytes(),
                    -1
                    );

                    return;
                } catch(KeeperException e) {
                    System.out.println(e.code());
                } catch(Exception e) {
                    System.out.println(e.getMessage());
                }
            }
        }

        String newData = String.format("%s:%s:%s", pwHash, "noMatch", "200");
        // No match in /jobs
        // Set the data in the node.
                try {
                    stat = zk.setData(
                    requestPath,
                    newData.getBytes(),
                    -1
                    );

                    return;
                } catch(KeeperException e) {
                    System.out.println(e.code());
                } catch(Exception e) {
                    System.out.println(e.getMessage());
                }

    }

    public class RequestListener implements Runnable {
        @Override
        public void run() {
            List<String> requests = null;
            List<String> requestsPrevious = new ArrayList<String>();
            Stat stat = null;
            byte[] data = null;
            String nodeData;
            String request;
            int newRequests = 0;
            String[] resultArr = null;

            while(true) {

                try {
                requests = zk.getChildren(ZK_REQUESTS, false);
                Collections.sort(requests);

                newRequests = requests.size() - requestsPrevious.size();

                    if (newRequests <= 0) {
                        continue;
                    } else {
                        System.out.println("There are " + newRequests + " new requests");
                        // There are new requests. Process them all.
                        for(int i=(requests.size() - newRequests); i<requests.size(); i++) {
                            request = requests.get(i);
                            System.out.println("i: " + i);
                            data = zk.getData(
                                ZK_REQUESTS + "/" + request,
                                false,
                                stat);

                            nodeData = new String(data, "UTF-8");

                            resultArr = nodeData.split(":");

                            System.out.println("Request is: " + request);

                             if (resultArr[2].equals("100")) {
                                System.out.println("Handling job: " + ZK_REQUESTS + "/" + request);
                            // This is a job -- add to /jobs.
                            handleJob(ZK_REQUESTS + "/" + request, resultArr[0]);
                            } else if (resultArr[2].equals("101")) {
                                System.out.println("Handling job status request: " + ZK_REQUESTS + "/" + request);
                                // This is a status -- get job status.
                                handleStatus(ZK_REQUESTS + "/" + request, resultArr[0]);
                            } else if (resultArr[2].equals("200")) {
                                System.out.println("Skipping request: " + ZK_REQUESTS + "/" + request + " because it is complete");
                                continue;
                            }
                            else {
                                System.out.println("Unknown Task Type!");
                            }
                        }
                    }
                } catch (Exception e) {
                    System.out.println("Exception trying to get /requests children");
                }
            requestsPrevious = requests;
            }
        }
    }

    public class JobTrackerListener implements Runnable {
        @Override
        public void run() {
            // Get the list of jobtrackers.
            List<String> jobTrackers = null;
            String path;

            while(true) {
                // Setup a watch on the node that is one smaller than this.
                try {
                    jobTrackers = zk.getChildren(ZK_TRACKERS, false);
                } catch(KeeperException e) {
                    System.out.println(e.code());
                } catch(Exception e) {
                    System.out.println(e.getMessage());
                }

                // Order the jobtrackers...
                Collections.sort(jobTrackers);

                // We know we are the jobtracker @ jobTrackerPath
                // Get index of...
                path = jobTrackerPath.split("/")[2];

                int myIndex = jobTrackers.indexOf(path);

                System.out.println("Trackers: " + jobTrackers + " My path: " + path + " My index: " + myIndex);

                if (myIndex != -1) {
                    // We want to watch the previous node @ myIndex-1
                    // We know that we are a backup so we are not first in the list. Thus myIndex-1 will always work.
                    String watchingNode = ZK_TRACKERS + "/" + jobTrackers.get(myIndex-1);

                    System.out.println("We will be watching: " + watchingNode);

                    // Setup a watch on the smaller znode.
                    try {
                     zk.exists(watchingNode, new Watcher() {
                        @Override
                        public void process(WatchedEvent event) {
                            if (event.getType().equals(EventType.NodeDeleted)) {
                                jobTrackerLatch.countDown();
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
                        System.out.println("Watching Job Tracker: " + watchingNode);
                        jobTrackerLatch.await();

                         // Job Tracker Down!
                        System.out.println("Job Tracker Down! Determine our position...");

                        // Countdown occured.
                        // Determine if we are the new leader
                        try {
                            jobTrackers = zk.getChildren(ZK_TRACKERS, false);
                        } catch(KeeperException e) {
                            System.out.println(e.code());
                        } catch(Exception e) {
                            System.out.println(e.getMessage());
                        }

                        // Order the jobtrackers...
                        Collections.sort(jobTrackers);

                        // If we are the first in the list -- we are the new leader! Set leader flag and start RequestListener Thread.

                        if (jobTrackers.indexOf(path) == 0) {
                            System.out.println("We are the new leader!");
                            isLeader = true;
                            break;
                        } else {
                            System.out.println("Still a backup...");
                        }

                        // Setup Latch again, we are still a backup!
                        jobTrackerLatch = new CountDownLatch(1);

                    } catch(Exception e) {
                        System.out.println(e.getMessage());
                    }

                } else {
                    System.exit(-1);

                }
            }
        }
    }

        public class WorkerListener implements Runnable {
        @Override
        public void run() {
            List<String> workersPrevious = null;
            List<String> workers = null;
            List<String> jobs = null;
            List<String> tasks = null;
            Stat stat = null;
            byte[] data = null;
            String nodeData;
            String missingWorker = null;

            while(true) {

                try {
                workersPrevious = zk.getChildren(ZK_WORKERS, new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                        if (event.getType().equals(EventType.NodeChildrenChanged)) {
                            // There has been a change in the number of workers.
                            workerLatch.countDown();
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
                    workerLatch.await();
                    workerLatch = new CountDownLatch(1);

                    workers = zk.getChildren(ZK_WORKERS, false);
                } catch(Exception e) {
                    System.out.println(e.getMessage());
                }

                // Sort the jobs in order incase they are not.
                // FROM API DOCS: The list of children returned is not sorted and no guarantee is provided as to its natural or lexical order.
                Collections.sort(workersPrevious);
                Collections.sort(workers);

                System.out.println("workersPrevious: " + workersPrevious.toString());
                System.out.println("workers: " + workers.toString());

                // If we had more workers before, we lost a worker!
               if (workersPrevious.size() > workers.size()) {
                    // Find out who we lost.
                    for (int i=0; i<workersPrevious.size(); i++ ) {
                        if (!workers.contains(workersPrevious.get(i))) {
                            // This is the worker we lost.
                            System.out.println("We lost worker: " + workersPrevious.get(i));

                            missingWorker = workersPrevious.get(i);
                            break;
                        }
                    }
               }

               if (missingWorker != null) {
                     // Go through all ongoing jobs and determine which are ongoing.
                    try {
                        jobs = zk.getChildren(ZK_JOBS, false);
                    } catch(KeeperException e) {
                        System.out.println(e.code());
                    } catch(Exception e) {
                        System.out.println(e.getMessage());
                    }

                    // Sort the jobs.
                    Collections.sort(jobs);

                   // Traverse each job, and get the Stat.
                    for (String path: jobs) {

                        System.out.println("WorkerListener: Getting job node @ path: " + ZK_JOBS + "/" + path);
                        try {
                            data = zk.getData(
                                ZK_JOBS + "/" + path,
                                false,
                                stat);
                        } catch(KeeperException e) {
                            System.out.println(e.code());
                        } catch(Exception e) {
                            System.out.println(e.getMessage());
                        }

                        nodeData = zkc.byteToString(data);

                        System.out.println("WorkerListener: job nodeData: " + nodeData);

                        // Determine if this is the job we are looking for.
                        if (nodeData.split(":")[1].equals("ongoing")) {
                            System.out.println("WorkerListener: This job is ongoing...check tasks.");

                            // Remove tasks for the matching worker by getting tasks for job and replace any workerID matching missingWorker with -1

                            try {
                                tasks = zk.getChildren(ZK_JOBS + "/" + path, false, stat);
                            } catch(KeeperException e) {
                                System.out.println(e.code());
                            } catch(Exception e) {
                                System.out.println(e.getMessage());
                            }

                            System.out.println("tasks: " + tasks.toString());

                            for (String task: tasks) {
                                // Get the data of the task.
                                try {
                                    data = zk.getData(ZK_JOBS + "/" + path +  "/" +task, false, stat);
                                } catch(KeeperException e) {
                                    System.out.println(e.code());
                                } catch(Exception e) {
                                    System.out.println(e.getMessage());
                                }

                                nodeData = zkc.byteToString(data);

                                System.out.println("WorkerListener: task nodeData: " + nodeData);

                                if(nodeData.split(":")[0].equals(missingWorker)) {
                                    System.out.println(" Worker " + missingWorker + " has died. Reset task to -1");

                                     // Setup the new data!
                                    String newData = String.format("%s:%s:%s", "-1", nodeData.split(":")[1], nodeData.split(":")[2]);


                                    System.out.println("New Data for Task: " + newData);

                                    // Set the data in the node.
                                    try {
                                        stat = zk.setData(
                                        ZK_JOBS + "/" + path + "/" + task,
                                        newData.getBytes(),
                                        -1
                                        );
                                    } catch(KeeperException e) {
                                        System.out.println(e.code());
                                    } catch(Exception e) {
                                        System.out.println(e.getMessage());
                                    }
                                }
                            } // end of for loop for traversing tasks
                        } // end of if statement if job is ongoing
                    } // end of for loop for traversing jobs

                    missingWorker = null;
                } // if missingWorker is not null
            } // while(true)
        } // run
    } // workerlistener

    public static void main(String[] args) {

        JobTracker jt = new JobTracker(args[0]);

        System.out.println("We have been set as: " + jobTrackerPath + " and isLeader is: " + isLeader);

        // We are a backup, setup a listener.
        if (!isLeader)
            new Thread(jt.new JobTrackerListener()).start();

        // Make sure we know when we are the leader...
        while (!isLeader) {
            try {
                Thread.sleep(700);
            } catch (InterruptedException e) {
                System.out.println(e);
            }

        }

        System.out.println("Starting the Worker Listener Thread");
        new Thread(jt.new WorkerListener()).start();

        System.out.println("Starting the Request Listener Thread");
        new Thread(jt.new RequestListener()).start();
    }
}

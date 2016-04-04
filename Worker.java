import java.net.Socket;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;
import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.math.BigInteger;



public class Worker {

	private static final String ZK_REQUESTS = "/requests";
	private static final String ZK_JOBS = "/jobs";
    private static final String ZK_FS = "/fs";
    private static final String ZK_WORKERS = "/workers";

    //ZooKeeper resources
    private static ZkConnector zkc;
    private static ZooKeeper zk;

    // JobTracker
    private static String jobTrackerPath;
    private static String JT_TRACKER = "/jt";
    private static String JT_PRIMARY = "P";
    private static String JT_BACKUP = "B";

    private static String startIdx = null;
    private static String endIdx = null;
    private static String myId = null;

    public static Integer fsPort = -1;
    public static String fsIP = null;

    public static String hashedPwd = null;
    public static String taskPath = null;
    public static String jobPath = null;


    private static CountDownLatch fsCreatedLatch = new CountDownLatch(1);
    private static CountDownLatch jobsCreatedLatch = new CountDownLatch(1);


    public Worker(String connection) {

        zkc = new ZkConnector();

        try {
            // Client sends "HOST:PORT" of Zookeeper service.
            zkc.connect(connection);
        } catch(Exception e) {
            System.out.println("Zookeper connect " + e.getMessage());
        }

        // Get zk Object
        zk = zkc.getZooKeeper();

        run();
    }

    public static void main(String[] args) {

    	System.out.println("Starting the Worker");
    	new Worker(args[0]);

    }

	public void run() {
		//Worker needs to know what the hashed string is, so it can try to make a match


		//Set up all my variables here.
		Integer updateLimit = -1;
		Stat stat = null;
		byte[] data;
		String  nodeData = null, clearTxtPwd = null, reqString = null, primaryFSPath = null;
		Boolean taskAcquired = false, monitorThreadStarted = false;
		String[] partition = null, resultArr;

		List<String> fileServers = null, jobs = null, tasks = null;

		Socket fSock = null;
		ObjectOutputStream out = null;
		ObjectInputStream in = null;

		myId = createZnode();

		while(true){

			while(!taskAcquired){
				taskAcquired = acquireTask(jobs, tasks, hashedPwd, myId, startIdx, endIdx, this);
			}

			//TODO: Need to set a watch on the FS znode to detect FS crashes. This is only needed if there are no fs.
			try{
				fileServers = zk.getChildren(ZK_FS, false);

				System.out.println("fileservers: " + fileServers.toString());
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}


			//If there are no FSs, then I can't get my partition of the dictionary, so I should set a watch and sleep.
			if(fileServers.size() == 0 && !monitorThreadStarted){

				System.out.println("There were no FSs up, I'm going to sleep...");

				//Set a watch on the fileservers
				try{
						fileServers = zk.getChildren(ZK_FS, new Watcher() {
			            @Override
			            public void process(WatchedEvent event) {
			                if (event.getType() == Event.EventType.NodeChildrenChanged) {
			    				fsCreatedLatch.countDown();
			                }
			            }
		            });

				} catch (Exception e) {
					System.out.println(e.getMessage());
				}

				//Go to sleep waiting for the latch to trigger, only wake up if a FS was added.
                try{
                    fsCreatedLatch.await();

                    fileServers = zk.getChildren(ZK_FS, false);

    				//Sort the fileServers
    				Collections.sort(fileServers);

    				//Isolate the primary FS here and get its info so you can connect to it.
    				primaryFSPath = fileServers.get(0);

    				System.out.println("fileservers: " + fileServers.toString());
    				System.out.println("Primary Path: " + primaryFSPath);

    				data = zk.getData(ZK_FS + "/" + primaryFSPath, false, stat);
    				try {
    					nodeData = new String(data, "UTF-8");
    				} catch (Exception e){
    					System.out.println("Failed on line 163");
    				}

    				resultArr = nodeData.split(":");

    				System.out.println("Primary FS: " + primaryFSPath);

    				//Get fsIP & fsPort so we can connect to the FS now.
    				fsIP = resultArr[0];
    				fsPort = Integer.parseInt(resultArr[1]);

                } catch(Exception e) {
                    System.out.println(e.getMessage());
                }

                new Thread(new FileServerMonitor(this, fsIP, fsPort, primaryFSPath, zk)).start();
                monitorThreadStarted = true;

			} else if(!monitorThreadStarted) {

				new Thread(new FileServerMonitor(this, fsIP, fsPort, primaryFSPath, zk)).start();
				monitorThreadStarted = true;
			}

			try {
				//I have a task. Connect to the FS and retrieve my partition, by sending my ID & my start & end indices.

				while (fsIP == null || fsPort == -1) {
					System.out.println("IN LOOP");
					Thread.sleep(1000);
				}

				System.out.println("Exited Loop");
				fSock = new Socket(fsIP, fsPort);
				out = new ObjectOutputStream(fSock.getOutputStream());
				in = new ObjectInputStream(fSock.getInputStream());


				//Set up our request for the file server here.
				reqString = String.format("%s:%s:%s", myId, startIdx, endIdx);
				out.writeObject(reqString);

				//Receive the response from the FS, which should be my partition, as an array.
				partition = (String[]) in.readObject();

				System.out.println("Partition length is: " + partition.length);

				fSock.close();

			} catch (Exception e) {
				//We need to catch a broken pipe exception or whatever when FS goes down and not let that blow me up.
				//If at any point in time the FS goes down , I need to catch the exceptions and look for the new primary and connect to them.
				System.out.println("The primary FS has gone down and its socket threw an exception");
			}


			updateLimit = partition.length/10;

			//Now we have all we need, do the work.
			clearTxtPwd = doWork(partition, hashedPwd, updateLimit, taskPath);

			//We've just finished our previous task, so we have no tasks at the moment.
			taskAcquired = false;

			if ( clearTxtPwd != null && clearTxtPwd.equals("AmzaIsAGod")) {
				continue;
			}

			//Handle the completed task appropriately, including task znode cleanup etc.
			taskCompleted(jobPath, taskPath, clearTxtPwd, jobs, tasks, hashedPwd, myId, taskAcquired);

		}
	}

	//TODO: Check that this works right.
	public static Boolean acquireTask(List<String> jobs, List<String> tasks, String hashedPwd, String myId, String startIdx, String endIdx, Worker worker){

		//System.out.println("Worker is acquiring a task.");
		String jobPath = null, taskPath = null, nodeData = null;
		byte[] data = null;
		Stat stat = null;

		String[] resultArr;

		try {
            stat = zk.exists(ZK_JOBS, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (event.getType() == Event.EventType.NodeCreated) {
                        jobsCreatedLatch.countDown();
                    }
                }
            });

            if (stat == null) {
                System.out.println("Going to sleep waiting for root /jobs");
                jobsCreatedLatch.await();
                System.out.println("root /jobs exists");
            }


        } catch (Exception e) {
            System.out.println("Job Root Exists Error");
        }

		try{
			jobs = zk.getChildren(ZK_JOBS, false);
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}

		Collections.sort(jobs);

		//Find ongoing job
		for(String job : jobs){
			jobPath = ZK_JOBS + "/" + job;
			worker.jobPath = ZK_JOBS + "/" + job;
			//System.out.println("jobpath: " + jobPath);
			//Get tasks for this job.
			try{
				tasks = zk.getChildren(jobPath, false);
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}
			if(tasks.size() != 0) {
				for(String task : tasks) {
					taskPath = jobPath + "/" + task;
					worker.taskPath = jobPath + "/" + task;
					//System.out.println("taskpath: " + taskPath);
					try{
						data = zk.getData(taskPath, false, stat);
					} catch (Exception e) {
						System.out.println(e.getMessage());
					}


					try {
						nodeData = new String(data, "UTF-8");
					} catch (Exception e){
						System.out.println("Failed on line 276 - Task disappeared");
						continue;
					}
					resultArr = nodeData.split(":");

					//Find unclaimed task and claim it.
					if(resultArr[0].equals("-1")){

						nodeData = String.format("%s:%s:%s", myId, resultArr[1], resultArr[2]);

						System.out.println("Found unclaimed task. Changing to: " + nodeData);

						//change the data in that task node to reflect that I have now claimed it and own this task.
						try {
							zk.setData(taskPath, nodeData.getBytes(), -1);
						} catch (Exception e) {
							System.out.println(e.getMessage());
						}

						worker.startIdx = resultArr[1];
						worker.endIdx = resultArr[2];

						try{
						data = zk.getData(jobPath, false, stat);

							nodeData = new String(data, "UTF-8");
						} catch (Exception e) {
							System.out.println("Failed on line 302");
						}


						resultArr = nodeData.split(":");
						worker.hashedPwd = resultArr[0];
						return true;
					}
				}
			}
		}

		return false;

	}

	public static String createZnode() {

		System.out.println("Creating znode for worker.");
		String path, myID;
		//Check that workers znode exists! Set a watch on it.

		//path = createZnode(); //ephemeral, sequential
		path = ZK_WORKERS + "/";


		try{
			path = zk.create(path,
	                null,
	                ZooDefs.Ids.OPEN_ACL_UNSAFE,
	                CreateMode.EPHEMERAL_SEQUENTIAL);
		} catch (Exception e) {
			System.out.println("Failed on line 334");
		}
		//Store it in i and then return it.

		System.out.println("WorkerID = " + path);


		return path.split("/")[2];
	}

	public static String doWork (String[] partition, String hashedPwd, Integer updateLimit, String taskPath){

		System.out.println("Starting the work on: " + hashedPwd);
		//Boolean pwdCracked = false;
		Integer i = 0;
		String pwd = null, hashResult = null, nodeData = null;
		byte[] data;
		String[] resultArr = null;
		Stat stat = null;
		Integer startIndex = -1;

		try{
			data = zk.getData(taskPath, false, stat);

			nodeData = new String(data, "UTF-8");

			resultArr = nodeData.split(":");

			startIndex = Integer.parseInt(resultArr[1]);

		} catch (Exception e){
			System.out.println("Failed on line 370 - My node disappeared");
					pwd = "AmzaIsAGod";
		}

		while(i < partition.length && pwd == null /*!pwdCracked*/){
			//Do md5 hashing & store the result to compare it with the hashed password we were given.

			hashResult = getHash(partition[i]);

			if(hashResult.equals(hashedPwd)) {
				pwd = partition[i];
				return pwd;
			}

			if(i % updateLimit == 0 && i != 0){

				try{
					System.out.println("taskpath: " + taskPath);
					data = zk.getData(taskPath, false, stat);

					nodeData = new String(data, "UTF-8");
				} catch (Exception e) {
					System.out.println("Failed on line 392 - My node disappeared");
					pwd = "AmzaIsAGod";
					break;
				}

				resultArr = nodeData.split(":");

				//Change the start index for the task, to update my progress.
				resultArr[1] = Integer.toString((int)(i+startIndex));

				System.out.println("startIdx is now: " + resultArr[1]);
				//nodeData = resultArr.toString(); Not sure how this would work

				nodeData = resultArr[0] + ":" + resultArr[1] + ":" + resultArr[2];
				//Update the task znode startIdx here.

				try{
					zk.setData(taskPath, nodeData.getBytes(), -1);
				} catch (Exception e) {
					System.out.println(e.getMessage());
				}
			}

			i++;
		}


		return pwd;
	}

    public static String getHash(String word) {

        String hash = null;
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            BigInteger hashint = new BigInteger(1, md5.digest(word.getBytes()));
            hash = hashint.toString(16);
            while (hash.length() < 32) hash = "0" + hash;
        } catch (NoSuchAlgorithmException nsae) {
            // ignore
        }
        return hash;
    }

	public static void taskCompleted(String jobPath, String taskPath, String clearTxtPwd, List<String> jobs, List<String> tasks, String hashedPwd, String myId, Boolean taskAcquired) {

		System.out.println("Task was completed for job: " + hashedPwd);
		String nodeData;
		String[] jobResult;
		byte[] jobData = null;
		List<String> jobTasks;
		Stat stat = null;

		//This means we cracked it, notify all others by modifying the job to complete and storing the result in it.
		if(clearTxtPwd != null){

			// Setup the new data!
			String newData = String.format("%s:%s", hashedPwd, clearTxtPwd);

            // Set the data in the job node to show that we cracked it.
			try{
				zk.setData(jobPath, newData.getBytes(),-1);
	            //Delete my task
	            zk.delete(taskPath, -1);
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}



		} else {
			//Delete your task znode
			try{
				zk.delete(taskPath, -1);
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}

			//Need to check if the job that your task was child of has any children left
			//if there are children left, then check to see if there are unclaimed subtasks and claim one
			try{

					System.out.println("jobpath in taskcompleted: " + jobPath);

					jobTasks = zk.getChildren(jobPath, false);

					//System.out.println("jobTasks: " + jobTasks.toString());


				if(jobTasks.size() == 0) {
					//If there are no children (you were the last one, and you didn't crack the password, then set the job's status to failed

					System.out.println("Job: " + jobPath + " has 0 tasks left");

					jobData = zk.getData(jobPath, false, stat);

					nodeData = new String(jobData, "UTF-8");

					jobResult = nodeData.split(":");

					if(jobResult[1].equals("ongoing")){
						nodeData = String.format("%s:%s", hashedPwd, "failed");
						zk.setData(jobPath, nodeData.getBytes(), -1);
					}
				}
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}

		}
	}



	public class FileServerMonitor implements Runnable {

		public static final String ZK_FS = "/fs";

		public String fsIP;
		public String primaryPath;
		public String nodeData;
		public String[] resultArr;
		public Integer fsPort;
		public ZooKeeper zk;
		public List<String> fileServers;
		public byte[] data;
		public CountDownLatch primaryDiedLatch = new CountDownLatch(1);
		public CountDownLatch backupExistsLatch = new CountDownLatch(1);
		public Stat stat;
		public Worker worker;

		public FileServerMonitor(Worker worker, String fsIP, Integer fsPort, String primaryPath, ZooKeeper zk){
			this.worker = worker;
			this.fsIP = fsIP;
			this.primaryPath = primaryPath;
			this.fsPort = fsPort;
			this.zk = zk;

			System.out.println("There's a new FS Monitor in town.");
		}

		@Override
		public void run() {
			while(true){
				try {
					fileServers = zk.getChildren(ZK_FS, false);

				} catch (Exception e) {
					System.out.println(e.getMessage());
				}


				//Sort the fileServers
				Collections.sort(fileServers);

				//Isolate the primary FS here and get its info so you can connect to it.
				primaryPath = fileServers.get(0);

				//TODO: need to set a watch on the primary FS here.
				try{
					data = zk.getData(ZK_FS + "/" + primaryPath, new Watcher() {
						@Override
			            public void process(WatchedEvent event) {
				            //If the primary went down
							if(event.getType() == Event.EventType.NodeDeleted){

								System.out.println("The Primary FS died.");

								//Wait for a new primary to take the place of the old one
								try {
									Thread.sleep(3000);
								} catch(Exception e) {
									System.out.println(e.getMessage());
								}


								primaryDiedLatch.countDown();

							}
						}
					}, stat);
				} catch (Exception e) {
					System.out.println(e.getMessage());
				}
				try{

					nodeData = new String(data, "UTF-8");
					resultArr = nodeData.split(":");


					fsIP = resultArr[0];
					worker.fsIP = resultArr[0];
					fsPort = Integer.parseInt(resultArr[1]);
					worker.fsPort = Integer.parseInt(resultArr[1]);

					System.out.println("ip: " + worker.fsIP + " port: " + worker.fsPort);


					primaryDiedLatch.await();
					primaryDiedLatch = new CountDownLatch(1);

					fileServers = zk.getChildren(ZK_FS, false);

						//Check that some children actually exist, is there a FS who will become primary?
						if(fileServers.size() > 0){

							Collections.sort(fileServers);

							primaryPath = fileServers.get(0);


							data = zk.getData(ZK_FS + "/" + primaryPath, false, stat);
							try {
								nodeData = new String(data, "UTF-8");
							} catch (Exception e){
								System.out.println("Failed on line 598");
							}

							resultArr = nodeData.split(":");

							System.out.println("The new Primary fs is: " + primaryPath);

							//Get fsIP & fsPort so we can connect to the FS now.
							worker.fsIP = resultArr[0];
							worker.fsPort = Integer.parseInt(resultArr[1]);

							} else {

							System.out.println("There were no other FSs waiting, going to watch the root node and sleep... ");
							fileServers = zk.getChildren(ZK_FS, new Watcher() {

								@Override
					            public void process(WatchedEvent event) {
					                if (event.getType() == Event.EventType.NodeChildrenChanged) {
					    				backupExistsLatch.countDown();
					                }
					            }

							});

							//Go to sleep waiting for the latch to trigger, only wake up if a FS was added.
					        try{
					            backupExistsLatch.await();
					            backupExistsLatch = new CountDownLatch(1);

					            fileServers = zk.getChildren(ZK_FS, false);

								//Sort the fileServers
								Collections.sort(fileServers);

								//Isolate the primary FS here and get its info so you can connect to it.
								primaryPath = fileServers.get(0);

								data = zk.getData(ZK_FS + "/" + primaryPath, false, stat);

								nodeData = new String(data, "UTF-8");
								resultArr = nodeData.split(":");

								System.out.println("The new Primary FS is: " + primaryPath);

								//Get fsIP & fsPort so we can connect to the FS now.
								worker.fsIP = resultArr[0];
								worker.fsPort = Integer.parseInt(resultArr[1]);

					        } catch(Exception e) {
					            System.out.println(e.getMessage());
					        }

					}

				} catch (Exception e){
					System.out.println(e.getMessage());
				}
			}

		}
}

}

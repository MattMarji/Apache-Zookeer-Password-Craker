import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import java.util.concurrent.CountDownLatch;
import java.util.List;
import java.io.Console;
import java.io.IOException;

/*
The purpose of the Client Driver is as follows:
1) The clientDriver provides the user with a command line to complete job requests and queries.

2) The clientDriver creates jobs

3) The clientDriver 'transparently' moves to the primary job tracker if one exists.
*/

public class ClientDriver {

    static String ZK_REQUESTS = "/requests";
    static String ZK_TRACKERS = "/jt";
    static String request_path;

    CountDownLatch jtCreatedLatch = new CountDownLatch(1);
    CountDownLatch jtRootCreatedLatch = new CountDownLatch(1);
    CountDownLatch nodeCreatedLatch = new CountDownLatch(1);
    ZkConnector zkc;
    ZooKeeper zk;

    public ClientDriver(String connection) {

        zkc = new ZkConnector();

        try {
            // Client sends "HOST:PORT" of Zookeeper service.
            zkc.connect(connection);
        } catch(Exception e) {
            System.out.println("Zookeper connect " + e.getMessage());
        }

        // Get zk Object
        zk = zkc.getZooKeeper();
    }

    public void sendTask(TaskPacket task) {
        String data = task.toString();
        Stat stat = null;
        List<String> jobTrackers = null;

        // First ensure that a job tracker exists...
        try {
            Thread.sleep(3000);

            stat = zk.exists(ZK_TRACKERS, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (event.getType() == Event.EventType.NodeCreated) {
                        jtRootCreatedLatch.countDown();
                    }
                }
            });

            if (stat == null) {
                System.out.println("Going to sleep waiting for root /jt");
                jtRootCreatedLatch.await();
                System.out.println("root /jt exists");
            }


        } catch (Exception e) {
            System.out.println("Job Tracker Exists Error");
        }

        try {
            jobTrackers = zk.getChildren(ZK_TRACKERS, false);

            System.out.println("Size of jobtrackers: " + jobTrackers.size());
            if (jobTrackers.size() == 0) {
                jobTrackers = zk.getChildren(ZK_TRACKERS, new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                        if (event.getType() == Event.EventType.NodeChildrenChanged) {
                            jtCreatedLatch.countDown();
                        }
                    }
                });
                // Go to sleep waiting for a job tracker
                System.out.println("Going to sleep waiting for job tracker.");
                jtCreatedLatch.await();
                System.out.println("We awake. There is a job tracker");

            }

            Thread.sleep(3000);
            Code ret = createRequestPath(data);

            if (ret != Code.OK) {
                System.out.println("Something is wrong... RET: " + ret);
                return;
            }

            else if(ret == Code.OK) {
                System.out.println("Successfully created the job: " + request_path);
            }

        } catch (Exception e) {
            System.out.println("Error getting children of JT node");
        }

    }

    // Create this override so that we can get the PATH for the job and keep track of it for later determining the result and deleting the node!
    private KeeperException.Code createRequestPath(String data) {
        try {
            byte[] byteData = null;
            if(data != null) {
                byteData = data.getBytes();
            }
            request_path = zk.create(ZK_REQUESTS + "/",
                    byteData,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT_SEQUENTIAL);

        } catch(KeeperException e) {
            return e.code();
        } catch(Exception e) {
            return KeeperException.Code.SYSTEMERROR;
        }
        return KeeperException.Code.OK;
    }

    public void getStatus() {
        String path = request_path;

        byte[] data;
        String result = null, status = null, outString = null;
        Stat stat = null;
        String[] resultArr= null;

        try {

            // See ZkConnector -- we watch the path for changes...
            zkc.listenToPath(path);

            // We have a result.
            // No need to watch, just get the information.
            data = zk.getData(path, false, stat);
            result = zkc.byteToString(data);

            resultArr = result.split(":");

            status = resultArr[1];

            if(status.equals("ongoing")) {
                System.out.println("Job in progress");
            }

            else if (status.equals("noMatch")) {
                System.out.println("No such job existed.");
            }

            else {
                if(status.equals("failed")){
                    outString = "Failed: Password not found";
                } else {
                    outString = "Password found: " + status;
                }

                System.out.println(outString); //Give the result to the user.
            }


            // Delete the result we received + delete the status job.
            //zk.delete(path, -1);

        } catch (KeeperException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

        TaskPacket task = null;
        Watcher watcher = null;

        ClientDriver client = new ClientDriver(args[0]);


            if(args[1].equals("job")) {
                task = new TaskPacket(args[2], "-1", TaskPacket.TASK_JOB);
                client.sendTask(task);
            }

            // If STATUS - send status packet.
            if (args[1].equals("status")) {
                task = new TaskPacket(args[2], "-1" ,TaskPacket.TASK_STATUS);
                client.sendTask(task);
                client.getStatus();
            }
        }

}

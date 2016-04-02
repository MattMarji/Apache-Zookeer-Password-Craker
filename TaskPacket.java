import java.io.Serializable;

public class TaskPacket implements Serializable {

    // PACKET TYPES
    public static final int TASK_JOB = 100;
    public static final int TASK_STATUS = 101;

    public String pwHash;
    public String status;
    public int packet_type;

    // This will be used by the client to create the initial JOB/STATUS task
    public TaskPacket (String pwHash, String status, int packet_type) {
        this.pwHash = pwHash;
        this.status = status;
        this.packet_type = packet_type;
    }

    // This will be used by the JobTracker to retrieve the TaskPacket from a
    // job node created by the client and handle the task!
    public TaskPacket (String data) {
        this.pwHash = data.split(":")[0];
        this.status = data.split(":")[1];
        this.packet_type = Integer.parseInt(data.split(":")[2]);
    }

    // This will convert the task packet into the form stored in the znode.
    public String toString() {
        String task;
        task = String.format("%s:%s:%s", pwHash, status, packet_type);
        return task;
    }
}

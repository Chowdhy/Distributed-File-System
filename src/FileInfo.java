import java.util.Arrays;
import java.util.List;

public class FileInfo {
    private final int size;
    private List<Integer> dstorePorts;
    private Status status;

    public FileInfo(int size, List<Integer> ports) {
        this.size = size;
        status = Status.STORE_IN_PROGRESS;
        dstorePorts = ports;
    }

    public int getSize() {
        return size;
    }

    public Status getStatus() {
        return status;
    }

    public List<Integer> getDstorePorts() {
        return dstorePorts;
    }

    public void setDstorePorts(List<Integer> dstorePorts) {
        this.dstorePorts = dstorePorts;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public enum Status {
        STORE_IN_PROGRESS, STORE_COMPLETE, REMOVE_IN_PROGRESS, REMOVE_COMPLETE;
    }
}

package server;

import java.util.ArrayList;
import java.util.List;

public class SlaveInfo {
    private int id;
    private long capacity;

    public SlaveInfo(int id, long capacity) {
        this.id = id;
        this.capacity = capacity;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public long getCapacity() {
        return capacity;
    }

    public void setCapacity(long capacity) {
        this.capacity = capacity;
    }


}

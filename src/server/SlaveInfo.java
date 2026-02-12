package server;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

public class SlaveInfo {
    private int id;
    private long capacity;
    private int port;
    private String host;
    private Boolean isActif;

    public SlaveInfo(int id, long capacity,int port ,String host) {
        this.id = id;
        this.capacity = capacity;
        this.port = port;
        this.host = host;
    }

    public int getId() {
        return id;
    }

    public Boolean getActif() {
        return isActif;
    }

    public void setActif(Boolean actif) {
        isActif = actif;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
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

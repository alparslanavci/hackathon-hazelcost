import java.io.Serializable;

public class HazelcostRequest implements Serializable {
    public String app;
    public Long timestamp;
    double cpuUsage;
    double memUsage;

    public HazelcostRequest(String app, Long timestamp, double cpuUsage, double memUsage) {
        this.app = app;
        this.timestamp = timestamp;
        this.cpuUsage = cpuUsage;
        this.memUsage = memUsage;
    }

    public String getApp() {
        return app;
    }

    public void setApp(String app) {
        this.app = app;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public double getCpuUsage() {
        return cpuUsage;
    }

    public void setCpuUsage(double cpuUsage) {
        this.cpuUsage = cpuUsage;
    }

    public double getMemUsage() {
        return memUsage;
    }

    public void setMemUsage(double memUsage) {
        this.memUsage = memUsage;
    }

    @Override
    public String toString() {
        return "HazelcostRequest{"
               + "app='"
               + app
               + '\''
               + ", timestamp="
               + timestamp
               + ", cpuUsage="
               + cpuUsage
               + ", memUsage="
               + memUsage
               + '}';
    }
}

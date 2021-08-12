import java.io.Serializable;

public class HazelcostResource implements Serializable {
    String app;
    double cpuUsage;
    int memUsage;

    public HazelcostResource(String app, double cpuUsage, int memUsage) {
        this.app = app;
        this.cpuUsage = cpuUsage;
        this.memUsage = memUsage;
    }
}

import java.io.Serializable;

public class HazelcostRequest implements Serializable {
    public String app;
    public Long timestamp;

    public HazelcostRequest(String app, Long timestamp) {
        this.app = app;
        this.timestamp = timestamp;
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
}

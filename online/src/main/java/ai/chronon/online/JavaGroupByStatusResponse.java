package ai.chronon.online;

import ai.chronon.online.fetcher.Fetcher;

public class JavaGroupByStatusResponse {
    public String groupByName;
    public String batchEndDate;
    public Long maxTs;

    public JavaGroupByStatusResponse(String groupByName, String batchEndDate, Long maxTs) {
        this.groupByName = groupByName;
        this.batchEndDate = batchEndDate;
        this.maxTs = maxTs;
    }

    public JavaGroupByStatusResponse(Fetcher.GroupByStatusResponse scalaResponse) {
        this.groupByName = scalaResponse.groupByName();
        this.batchEndDate = scalaResponse.batchEndDate();
        this.maxTs = scalaResponse.maxTs();
    }

    public Fetcher.GroupByStatusResponse toScala() {
        return new Fetcher.GroupByStatusResponse(groupByName, batchEndDate, maxTs);
    }
}

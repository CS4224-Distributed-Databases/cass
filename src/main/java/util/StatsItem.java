package util;

public class StatsItem {
    public Double clientNum;
    public Double numOfTransactions;
    public Double executionTime;
    public Double throughput;
    public Double avgLatency;
    public Double medianLatency;
    public Double latency95;
    public Double latency99;

    public StatsItem() {
        clientNum = null;
        numOfTransactions = null;
        executionTime = null;
        throughput = null;
        avgLatency = null;
        medianLatency = null;
        latency95 = null;
        latency99 = null;
    }
}
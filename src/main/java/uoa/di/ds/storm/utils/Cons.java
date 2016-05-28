package uoa.di.ds.storm.utils;

public class Cons {

	public static final String TLG_NAME= "topology.name";
	public static final String TLG_WORKERS= "topology.num.workers";
	public static final String TLG_MX_SPOUT= "topology.max.spout.pending";
	public static final String TLG_MSG_TIMEOUT= "topology.message.timeout.secs";
	public static final String TLG_STATS_RATE= "topology.stats.sample.rate";
	public static final String TLG_RCV_BUF= "topology.receiver.buffer.size";
	public static final String TLG_TNF_BUF= "topology.transfer.buffer.size";
	public static final String TLG_EXEC_RCV_BUF= "topology.executor.receive.buffer.size";
	public static final String TLG_EXEC_SEND_BUF= "topology.executor.send.buffer.size";
	
	public static final String TUPLE_VAR_MSG = "message";
	public static final String TUPLE_VAR_KEY = "key";
	public static final String TUPLE_VAR_CPU = "cpu";
	public static final String TUPLE_VAR_RAM = "ram";
	public static final String TUPLE_VAR_ACTIVESESSIONS = "activeSessions";
	public static final String TUPLE_VAR_UPTIME = "upTime";
	public static final String TUPLE_VAR_ID = "id";
	public static final String TUPLE_VAR_NAME = "name";
	public static final String TUPLE_VAR_SITE = "site";
	public static final String TUPLE_VAR_TEMPERATURE = "temperature";

	
	
	public static final String LOCAL_ADDRS = "127.0.0.1";
	
	public static final String DefaultSpoutName = "tcpSpout";
}

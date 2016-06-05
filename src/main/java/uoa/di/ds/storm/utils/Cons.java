package uoa.di.ds.storm.utils;

public class Cons {

	public static final String TLG_NAME = "topology.name";
	public static final String TLG_DBG = "topology.debug";
	public static final String TLG_LOCAL = "topology.local";
	public static final String TLG_WORKERS = "topology.num.workers";
	public static final String TLG_MX_SPOUT = "topology.max.spout.pending";
	public static final String TLG_MSG_TIMEOUT = "topology.message.timeout.secs";
	public static final String TLG_STATS_RATE = "topology.stats.sample.rate";
	public static final String TLG_RCV_BUF = "topology.receiver.buffer.size";
	public static final String TLG_TNF_BUF = "topology.transfer.buffer.size";
	public static final String TLG_EXEC_RCV_BUF = "topology.executor.receive.buffer.size";
	public static final String TLG_EXEC_SEND_BUF = "topology.executor.send.buffer.size";

	public static final String CASSANDRA_HOST = "cassandra.host";
	public static final String CASSANDRA_PORT = "cassandra.port";
	public static final String CASSANDRA_R_KEYSPACE = "cassandra.rules.keyspace";
	public static final String CASSANDRA_R_TABLE = "cassandra.rules.table";
	public static final String CASSANDRA_S_KEYSPACE = "cassandra.statistics.keyspace";
	public static final String CASSANDRA_A_KEYSPACE = "cassandra.alarms.keyspace";
	public static final String CASSANDRA_AM_TABLE = "cassandra.alarms.alarms_meta";       // Is that correct?
	public static final String CASSANDRA_A_TABLE = "cassandra.alarms.active_alarms";      // Is that correct?
	public static final String CASSANDRA_CLUSTERNAME = "cassandra.cluster.name";
	public static final String CASSANDRA_A_BOLT_PARALLEL = "cassandra.aggregation.bolt.parallelism";
	public static final String CASSANDRA_A_BOLT_BATCH = "cassandra.aggregation.bolt.batch";
	public static final String CASSANDRA_A_BOLT_BATCH_SIZE = "cassandra.aggregation.bolt.batch.size";
	

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
	public static final String TUPLE_VAR_EVENTTIME = "eventTime";
	public static final String TUPLE_VAR_DURATION = "duration";
	public static final String TUPLE_VAR_VALUE = "value";
	public static final String TUPLE_VAR_FIELD = "field";
	public static final String TUPLE_VAR_OPER = "operation";
	public static final String TUPLE_VAR_MO = "managed_object";
	public static final String TUPLE_VAR_NOTIF = "notification_id";
	public static final String TUPLE_VAR_ADDTEXT = "additional_text";
	public static final String TUPLE_VAR_STATE = "additional_text";

	public static final String LOCAL_ADDRS = "127.0.0.1";

	public static final String DefaultSpoutName = "tcpSpout";
	public static final String DefaultSpoutName2 = "RandomGeneratorSpout";
	public static final String DefaultBoltName = "Bolt";
	public static final String DefaultACassandraBoltName = "CassandraAggregationBolt";

}

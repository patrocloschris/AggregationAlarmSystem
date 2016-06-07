package uoa.di.ds.storm.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import uoa.di.ds.db.ConnectionManager;
import uoa.di.ds.storm.utils.Cons;

public class AlarmsCassandraBolt extends BaseRichBolt{

	private static final Logger LOG = LoggerFactory.getLogger(AlarmsCassandraBolt.class);

	private static final long serialVersionUID = 1L;
	
    private Map _conf= null;
    private TopologyContext _context = null;
    private OutputCollector _collector = null;
    private String cassandraHostname = null; 
    private String clusterName=null;
    private boolean batchMode = false;
    private int batchSize = 10;
	private ArrayList<Tuple> tupleList ;
	private Session session;
	private String keyspace;
	private String table;
	
    public AlarmsCassandraBolt(String keyspace,String table) {
    	this.tupleList = new ArrayList<Tuple>();
    	this.keyspace = keyspace;
    	this.table = table;
    	LOG.info("Creating bolt for Keyspace=[{}] and Table=[{}]",keyspace,table);

    }
    
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
	      this._conf = stormConf;
	      this._context = context;
	      this._collector = collector;
    	  ConnectionManager.init(cassandraHostname,clusterName);
    	  session = ConnectionManager.getInstance().getCluster().connect(keyspace);
    	  LOG.info("Preparing bolt....Connection for on DB=[{}]",session.getLoggedKeyspace());
	}

	@Override
	public void execute(Tuple input) {
		if(batchMode){
			tupleList.add(input);
			if(batchSize == tupleList.size()){
				insertBatchToCassandra( tupleList);
				tupleList.clear();
			}
		} else {
			insertToCassandra(input);
		}
		_collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}
	
	public void withHostName(String name){
		this.cassandraHostname = name;
	}
	
	public void withClusterName(String clusterName){
		this.clusterName = clusterName;
	}

	public void withBatchMode(boolean mode){
		this.batchMode = mode;
	}

	public void withBatchSize(int size){
		this.batchSize = size;
	}
	
	private void insertToCassandra(Tuple tuple){

		Statement statement = null;
		if(tuple.getValueByField(Cons.TUPLE_VAR_STATE).equals("active")){
			statement = QueryBuilder.insertInto(table)
			        .value("managed_object",tuple.getValueByField(Cons.TUPLE_VAR_MO))
			        .value("notification_id", tuple.getValueByField(Cons.TUPLE_VAR_NOTIF))
			        .value("additional_text", tuple.getValueByField(Cons.TUPLE_VAR_ADDTEXT))
			        .value("eventTime", tuple.getValueByField(Cons.TUPLE_VAR_EVENTTIME));
			session.execute(statement);		
			LOG.info("Insert into DB tuple=[{}]",tuple);

		}else{
			session.execute("Delete From "+table+" where managed_object='"+tuple.getValueByField(Cons.TUPLE_VAR_MO)+"' and notification_id="+tuple.getValueByField(Cons.TUPLE_VAR_NOTIF));
			LOG.info("Insert into DB tuple=[{}]",tuple);
		}
	}

	private void insertBatchToCassandra(List<Tuple> tuples){
		
		BatchStatement batch = new BatchStatement();

		for(Tuple tuple: tuples){
			Statement statement = QueryBuilder.insertInto(table)
			        .value("managed_object",tuple.getValueByField(Cons.TUPLE_VAR_MO))
			        .value("notification_id", tuple.getValueByField(Cons.TUPLE_VAR_NOTIF))
			        .value("additional_text", tuple.getValueByField(Cons.TUPLE_VAR_ADDTEXT))
			        .value("state", tuple.getValueByField(Cons.TUPLE_VAR_STATE))
			        .value("eventTime", tuple.getValueByField(Cons.TUPLE_VAR_EVENTTIME));
			batch.add(statement);
		}
		session.execute(batch);
		LOG.info("Insert into DB multi tuples");
	}
	


}

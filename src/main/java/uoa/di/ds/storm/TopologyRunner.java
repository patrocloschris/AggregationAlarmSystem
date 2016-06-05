package uoa.di.ds.storm;

import java.util.ArrayList;

import org.apache.commons.configuration.Configuration;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import uoa.di.ds.db.ConnectionManager;
import uoa.di.ds.storm.bolt.AggregationBolt;
import uoa.di.ds.storm.bolt.AggregationCassandraBolt;
import uoa.di.ds.storm.spout.RandomEventGeneratorSpout;
import uoa.di.ds.storm.utils.Cons;
import uoa.di.ds.storm.utils.configuration.TopologyConfig;

public class TopologyRunner {

	private static final Logger LOG = LoggerFactory.getLogger(TopologyRunner.class);

	public static void main(String args[]) {

		TopologySettings settings = TopologyConfig.validateInputParameters(args);
		if (settings == null) {
			LOG.error("Missmatching in input parameters");
			return;
		}

		LOG.info("Starting topology initialization...");
		Configuration config = TopologyConfig.readConfigurationFile(settings.getConfigPath());
		if (config == null) {
			LOG.error("Cannot read configuration file=[]", settings.getConfigPath());
			return;
		}

		/* Setup topology configuration*/
		String topologyName = config.getString(Cons.TLG_NAME);
		Config stormConfig = TopologyConfig.constructStormConfig(config);
		TopologyBuilder builder = new TopologyBuilder();

		/* Initialize Event Spout into to topology */
		LOG.info("Adding Spout =["+Cons.DefaultSpoutName2+"]");
//		TCPSpout tcpSpout = new TCPSpout(Cons.LOCAL_ADDRS, settings.getSport());
		RandomEventGeneratorSpout randomGeneratorSpout = new RandomEventGeneratorSpout(10);

		builder.setSpout(Cons.DefaultSpoutName2, randomGeneratorSpout,1);
		
		/*Open a connection to cassandra to retrieve rules*/
		ConnectionManager.init(config.getString(Cons.CASSANDRA_HOST),config.getString(Cons.CASSANDRA_CLUSTERNAME,null));
		Session session = ConnectionManager.getInstance().getCluster().connect(config.getString(Cons.CASSANDRA_R_KEYSPACE));
		ResultSet results = session.execute("SELECT * FROM " + config.getString(Cons.CASSANDRA_R_TABLE));

		/*For every rule generate a bolt*/
		ArrayList<String> streams = new ArrayList<String>();
		for (Row row : results) {
			String field = row.getString("field") == null ? "field" : row.getString("field");
			String operation = row.getString("operation") == null ? "operation" : row.getString("operation");
			int duration = row.getInt("duration") == 0 ? 10 : row.getInt("duration");
			int nbolts = row.getInt("nbolts") == 0 ? 2 : row.getInt("nbolts");

			AggregationBolt bolt = new AggregationBolt(field, operation, duration);
			String boldID = Cons.DefaultBoltName.concat(".").concat(field).concat("_").concat(operation);
			streams.add(boldID);
			LOG.info("Adding stream =["+boldID+"]");
			builder.setBolt(boldID,bolt,nbolts).shuffleGrouping(Cons.DefaultSpoutName2);
		}
		
		/*Connecting CassandraAggregationBolt*/
		AggregationCassandraBolt cassandraBolt = new AggregationCassandraBolt(config.getString(Cons.CASSANDRA_S_KEYSPACE));
		cassandraBolt.withHostName(config.getString(Cons.CASSANDRA_HOST));
		cassandraBolt.withClusterName(config.getString(Cons.CASSANDRA_CLUSTERNAME));
		cassandraBolt.withBatchMode(config.getBoolean(Cons.CASSANDRA_A_BOLT_BATCH));
		cassandraBolt.withBatchSize(config.getInt(Cons.CASSANDRA_A_BOLT_BATCH_SIZE,1));
		LOG.info("Adding CassandraAggregation Bolt");
		BoltDeclarer declarer = builder.setBolt(Cons.DefaultACassandraBoltName,cassandraBolt,config.getInt(Cons.CASSANDRA_A_BOLT_PARALLEL,2));
		for(String stream : streams){
			declarer.shuffleGrouping(stream);
		}
		
		
		LOG.info("Starting topology deployment...");
		LOG.info("Local mode set to: " + config.getBoolean(Cons.TLG_LOCAL));

		if (config.getBoolean(Cons.TLG_LOCAL)) {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(topologyName, stormConfig, builder.createTopology());
		} else {
			try {
				StormSubmitter.submitTopology(topologyName, stormConfig, builder.createTopology());
			} catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
				LOG.error("Error during topology submittion.\nError=[{}]",e.getMessage());
			}
		}

	}

}

package uoa.di.ds.storm;


import org.apache.commons.configuration.Configuration;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uoa.di.ds.storm.utils.Cons;
import uoa.di.ds.storm.utils.configuration.TopologyConfig;

public class TopologyRunner {

    private static final Logger LOG = LoggerFactory.getLogger(TopologyRunner.class);
	
	public static void main(String args[]){
	
		TopologySettings settings = TopologyConfig.validateInputParameters(args);
		if(settings == null){
			LOG.error("Missmatching in input parameters");
			return;
		}

		LOG.info("Starting topology initialization...");	    
	    Configuration config = TopologyConfig.readConfigurationFile(settings.configPath);
	    if(config==null){
			LOG.error("Cannot read configuration file=[]",settings.configPath);
			return;
	    }
	    
	    String topologyName = config.getString(Cons.TLG_NAME);

	    Config stormConfig = TopologyConfig.constructStormConfig(config, settings);

	    
        TopologyBuilder builder = new TopologyBuilder();
        /*
         * TODO setup topology pipeline
         */
		LOG.info("Starting topology deployment...");	    
        LOG.info("Local mode set to: " + settings.isLocalMode());
        
        if (settings.isLocalMode()) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topologyName, stormConfig, builder.createTopology());
        } else {
            try {
				StormSubmitter.submitTopology(topologyName, stormConfig, builder.createTopology());
			} catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }

	
	}
	
	
}

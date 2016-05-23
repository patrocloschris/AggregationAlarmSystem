package uoa.di.ds.storm.utils.configuration;

import java.io.File;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.storm.Config;

import com.beust.jcommander.JCommander;

import uoa.di.ds.storm.TopologySettings;
import uoa.di.ds.storm.utils.Cons;

public class TopologyConfig {

	/* Validate input parameters */
	public static TopologySettings validateInputParameters(String args[]) {
		TopologySettings params = new TopologySettings();
		JCommander jc = new JCommander(params, args);
		jc.setProgramName("DistributedSystems");
		if (params.isHelp()) {
			jc.usage();
			return null;
		}
		return params;
	}

	/*read key-value configuration file*/
	public static Configuration readConfigurationFile(String configPath) {
		Configuration config = null;
		try {
			File f = new File(configPath);
			if (f.exists() && !f.isDirectory()) {
				config = new PropertiesConfiguration(configPath);
			}
		} catch (ConfigurationException e) {
			e.printStackTrace();
		}
		return config;
	}

	
	public static Config constructStormConfig(Configuration config,TopologySettings settings){
	    Config stormConfig = new Config();
	    stormConfig.setDebug(settings.isDebug());
	    stormConfig.setNumWorkers(config.getInt(Cons.TLG_WORKERS, 1));
	    stormConfig.setMaxSpoutPending(config.getInt(Cons.TLG_MX_SPOUT, 1024));
	    stormConfig.setMessageTimeoutSecs(config.getInt(Cons.TLG_MSG_TIMEOUT, 30));
	    stormConfig.setStatsSampleRate(config.getDouble(Cons.TLG_STATS_RATE, 0.05));
	    stormConfig.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, config.getInt(Cons.TLG_TNF_BUF, 1024)); 
	    stormConfig.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE,
                config.getInt(Cons.TLG_EXEC_RCV_BUF, 1024)); 
	    stormConfig.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, config.getInt(Cons.TLG_EXEC_SEND_BUF, 1024));   

	    if(settings.isLocalMode()){
	    	stormConfig.setMaxTaskParallelism(1);
	    }
	    return stormConfig;

	}
	
	
}

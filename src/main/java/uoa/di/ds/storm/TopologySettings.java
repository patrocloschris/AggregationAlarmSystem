package uoa.di.ds.storm;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

@Parameters(separators = "=")
public class TopologySettings {

    @Parameter(names = "-help", help = true)
    protected boolean help = false;

    @Parameter(names = "-local_mode", description = "Run the topology on local or remote mode")
    protected boolean localMode = true;

    @Parameter(names = "-test_mode", description = "Test mode")
    protected boolean testMode = true;

    @Parameter(names = "-config_path", description = "Configuration path")
    protected String configPath="/home/chrispat/Documents/workspace/DistributedSystems/src/main/resources/topology.conf";

    @Parameter(names = "-debug", description = "Debug mode")
    protected boolean debug = true;

    @Parameter(names = "-keyspace", description = "Cassandra rule's keyspace")
    protected String keyspace = "rules";


    @Parameter(names = "-table", description = "Cassandra rule's table")
    protected String table = "aggregation_rules";

    @Parameter(names = "-host", description = "Cassandra's hostname")
    protected String chost = "192.168.1.69";

    @Parameter(names = "-sport", description = "Spout's Port")
    protected int sport = 6606;
    
    public boolean isHelp() {
        return help;
    }

    public boolean isLocalMode() {
        return localMode;
    }

    public boolean isTestMode() {
        return testMode;
    }

    public String getConfigPath() {
        return configPath;
    }

    public boolean isDebug() {
        return debug;
    }

    public String getKeyspace() {
		return keyspace;
	}

	public String getTable() {
		return table;
	}

	public String getChost() {
		return chost;
	}

	public int getSport() {
		return sport;
	}

	//TODO update toString()
	@Override
    public String toString() {
        return "TopologySettings [help=" + help + ", localMode=" + localMode + ", testMode=" + testMode
                + ", configPath=" + configPath + ", debug=" + debug + "]";
    }
	
}

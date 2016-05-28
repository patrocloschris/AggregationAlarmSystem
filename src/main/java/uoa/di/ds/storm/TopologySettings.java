package uoa.di.ds.storm;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

@Parameters(separators = "=")
public class TopologySettings {

    @Parameter(names = "-help", help = true)
    protected boolean help = false;

    @Parameter(names = "-local_mode", description = "Run the topology on local or remote mode",required=true)
    protected boolean localMode = true;

    @Parameter(names = "-test_mode", description = "Test mode",required=true)
    protected boolean testMode = false;

    @Parameter(names = "-config_path", description = "Configuration path",required=true)
    protected String configPath;

    @Parameter(names = "-debug", description = "Debug mode", required = false)
    protected boolean debug = false;

    @Parameter(names = "-keyspace", description = "Cassandra rule's keyspace", required= true)
    protected String keyspace = "";


    @Parameter(names = "-table", description = "Cassandra rule's table", required= true)
    protected String table = "";

    @Parameter(names = "-host", description = "Cassandra's hostname", required= true)
    protected String chost = "";

    @Parameter(names = "-sport", description = "Spout's Port", required= true)
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

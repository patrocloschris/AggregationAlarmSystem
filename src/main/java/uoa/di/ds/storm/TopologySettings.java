package uoa.di.ds.storm;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

@Parameters(separators = "=")
public class TopologySettings {

    @Parameter(names = "--help", help = true)
    protected boolean help = false;

    @Parameter(names = "-local_mode", description = "Run the topology on local or remote mode", arity = 1)
    protected boolean localMode = true;

    @Parameter(names = "-test_mode", description = "Test mode", arity = 1)
    protected boolean testMode = false;

    @Parameter(names = "-config_path", required = true, description = "Configuration path", arity = 1)
    protected String configPath;

    @Parameter(names = "-debug", description = "Debug mode", arity = 1)
    protected boolean debug = false;

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

    @Override
    public String toString() {
        return "TopologySettings [help=" + help + ", localMode=" + localMode + ", testMode=" + testMode
                + ", configPath=" + configPath + ", debug=" + debug + "]";
    }
	
}

package uoa.di.ds.storm;

import com.beust.jcommander.Parameter;

/*
 * Input parameter domain class
 */
public class TopologySettings {

    @Parameter(names = "--help", help = true)
    private boolean help = false;

    @Parameter(names = "--configPath", description = "Configuration path",required=true)
    private String configPath=null;

	public boolean isHelp() {
		return help;
	}

	public String getConfigPath() {
		return configPath;
	}

	@Override
    public String toString() {
        return "TopologySettings [help=" + help + ", configPath=" + configPath +"]";
    }
	
}

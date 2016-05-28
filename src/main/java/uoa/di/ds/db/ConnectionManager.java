package uoa.di.ds.db;

import com.datastax.driver.core.Cluster;

public class ConnectionManager {
	
	private static ConnectionManager instance = null;
	private Cluster cluster = null;

	private ConnectionManager(String host){
		cluster = Cluster.builder().addContactPoint(host).build();
	}
	
	public static ConnectionManager getInstance(){
		return instance;
	}
	
	public static void init(String host){
		instance = new ConnectionManager(host);
	}
	
	public Cluster getCluster(){
		return cluster;
	}

}

package uoa.di.ds.db;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class ConnectionManager {
	
	private static ConnectionManager instance = null;
	private Cluster cluster = null;
	private Session session = null;

	
	private ConnectionManager(String host, String keyspace){
		cluster = Cluster.builder().addContactPoint(host).build();
		session = cluster.connect(keyspace);
	}
	
	public static ConnectionManager getInstance(){
		return instance;
	}
	
	public static void init(String host, String keyspace){
		instance = new ConnectionManager(host, keyspace);
	}
	
	public Cluster getCluster(){
		return cluster;
	}

	public Session getSession(){
		return session;
	}
}

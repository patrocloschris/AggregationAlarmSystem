package uoa.di.ds.storm.utils.network;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*A simple socket client*/
public class SocketClient {

	private static final Logger LOG = LoggerFactory.getLogger(SocketClient.class);
	private Socket tcpSocket = null;
	private BufferedReader d = null;

	public SocketClient(String hostname, int port) {
		try {
			tcpSocket = new Socket(hostname, port);
			d = new BufferedReader(new InputStreamReader(tcpSocket.getInputStream()));
		} catch (UnknownHostException e) {
			System.err.println("Don't know about host: hostname");
		} catch (IOException e) {
			System.err.println("Couldn't get I/O for the connection to: hostname");
		}
	}
	
	public String getNextResponse(){
		try {
			return d.readLine();
		} catch (IOException e) {
			LOG.error("Cannot read next response. Returning null",e);
			return null;
		}
	}
	
	public void closeClient(){
		try {
			d.close();
			tcpSocket.close();
		} catch (IOException e) {
			LOG.error("Cannot close open Streams and Sockets",e);
		}
	}
}

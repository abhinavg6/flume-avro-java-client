package com.sapient.flumeclient;

/**
 * Main class to send events to flume agent, by using a simple flume avro client
 * 
 * @author abhinavg6
 *
 */
public class FlumeEventSender {

	public static void main(String args[]) {
		ConsCompFlumeClient conscompClient = new ConsCompFlumeClient();

		// Initialize flume client resources
		conscompClient.init("localhost", 42222);

		// Send all the data to flume agent
		System.out.println("Starting flume event generator");
		conscompClient.sendDataToFlume();

		// Clean-up flume client resources
		System.out.println("All events sent, cleaning up");
		conscompClient.cleanUp();
	}

}
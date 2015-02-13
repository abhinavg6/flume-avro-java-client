package com.sapient.flumeclient;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;

/**
 * A simple event sender to a flume agent (as a avro source). The events are
 * sourced from a consumer complaints dataset.
 * 
 * @author abhinavg6
 *
 */
public class ConsCompFlumeClient {

	private RpcClient client;
	private String hostname;
	private int port;
	private Iterable<CSVRecord> csvRecords;

	// Initialize connection to Flume agent, and the event source file
	public void init(String hostname, int port) {
		// Setup the RPC connection
		this.hostname = hostname;
		this.port = port;
		this.client = RpcClientFactory.getDefaultInstance(hostname, port);

		try {
			InputStream inStream = this.getClass().getClassLoader()
					.getResourceAsStream("cons_comp_data.csv");
			Reader in = new InputStreamReader(inStream);
			csvRecords = CSVFormat.DEFAULT.withSkipHeaderRecord(true).parse(in);
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}
	}

	// Send records from source file as events to flume agent
	public void sendDataToFlume() {
		CSVRecord csvRecord = null;
		Iterator<CSVRecord> csvRecordItr = csvRecords.iterator();
		while (csvRecordItr.hasNext()) {
			try {
				csvRecord = csvRecordItr.next();
				String eventStr = csvRecord.toString();
				System.out.println(eventStr);
				Event event = EventBuilder.withBody(eventStr,
						Charset.forName("UTF-8"));
				client.append(event);
			} catch (NoSuchElementException e) {
				System.out.println(e.getMessage());
			} catch (EventDeliveryException e) {
				// clean up and recreate the client
				client.close();
				client = null;
				client = RpcClientFactory.getDefaultInstance(hostname, port);
			}
		}
	}

	public void cleanUp() {
		// Close the RPC connection
		client.close();
	}

}

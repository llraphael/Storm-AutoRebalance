package com.llraphael.storm.dynscheduler;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.thrift7.TException;
import org.apache.thrift7.protocol.TBinaryProtocol;
import org.apache.thrift7.transport.TFramedTransport;
import org.apache.thrift7.transport.TSocket;
import org.apache.thrift7.transport.TTransportException;

import backtype.storm.generated.BoltStats;
import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.ExecutorSpecificStats;
import backtype.storm.generated.ExecutorStats;
import backtype.storm.generated.ExecutorSummary;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.Nimbus.Client;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.generated.TopologySummary;

public class TopologyComponentInfo {
	private TSocket socket;
	private TFramedTransport transport;
	private TBinaryProtocol protocol;
	private Client client;
	private ClusterSummary summary;
	private Map<String, Map<String, String>> storedBoltsInfo;
	private String timeWindow;
	
	public TopologyComponentInfo(String ip) {
		this.socket = new TSocket(ip, 6627);
		this.transport = new TFramedTransport(socket);
		this.protocol = new TBinaryProtocol(transport);
		this.client = new Client(protocol);
		try {
			transport.open();
			summary = client.getClusterInfo();
		} catch (TTransportException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		}
	}
	
	public boolean setTimeWindow(int tw) {
		String tW = Integer.toString(tw);
		if(tW.equals("600") || tW.equals("10800") || tW.equals("86400")) {
			timeWindow = tW;
			return true;
		} else {
			System.out.println("The value of time window is illegal, choose among: 600, 10800 and 86400");
			return false;
		}
	}

	// Get all the bolts information for the topology, mainly on executed tuple number, execute latency and
	// the number of executors each bolt has.
	public Map<String, Map<String, String>> getAllBoltsInfo(String topologyName) {
		Map<String, Map<String, String>> boltsInfo= new HashMap<String, Map<String, String>>();
		
		// Check if time window has been decided
		if(timeWindow == null) {
			System.out.println("Please choose time window: 600, 10800 and 86400 (s)");
			return boltsInfo;
		}
		
		TopologyInfo topInfo = getTopologyInfobyName(topologyName);
		
		// Determine if 'topInfo' is effective.
		if(!topInfo.is_set_name()) {
			System.out.println("The topology does not exist!");
			return boltsInfo;
		}
	
		Iterator<ExecutorSummary> executorStatusItr = topInfo.get_executors_iterator();
		while(executorStatusItr.hasNext()) {
			//Get the executor
			ExecutorSummary executorSummary = executorStatusItr.next();
			if(!executorSummary.is_set_executor_info() || !executorSummary.is_set_stats()) {
				return boltsInfo;
			}
			
			// Get the newly uptime for the components, which would be the same for all
			if(!boltsInfo.containsKey("up_time")) {
				String upTime = Integer.toString(executorSummary.get_uptime_secs());
				Map<String, String> temp = new HashMap<String, String>();
				temp.put("up_time", upTime);
				boltsInfo.put("static_time", temp);
			}
			ExecutorStats execStats = executorSummary.get_stats();
			
			if(!execStats.is_set_specific()) {
				return boltsInfo;
			}
			ExecutorSpecificStats execSpecStats = execStats.get_specific();
			String componentId = executorSummary.get_component_id();
			
			// Check if the component is a bolt and the date of system generated acker bolt will not be collected
			if(execSpecStats.is_set_bolt() && !componentId.equals("__acker")) {
				BoltStats boltStats = execSpecStats.get_bolt();
				
				// Get the number of executed tuples and average latency for this executor
				Long executedNum = getBoltStatLongValueFromMap(boltStats.get_executed(), timeWindow);
				Long ackedNum = getBoltStatLongValueFromMap(boltStats.get_acked(), timeWindow); 
				Double executeLatency = getBoltStatDoubleValueFromMap(boltStats.get_execute_ms_avg(), timeWindow);
				
				// If the executor information is not ready, just return an empty table
				if(executedNum == null || executeLatency == null) {
					return new HashMap<String, Map<String, String>>();
				}
				
				// If this executor belongs to a new bolt, just put the stats
				Map<String, String> currentStat = new HashMap<String, String>();
				if(!boltsInfo.containsKey(componentId)) {
					currentStat.put("num_of_executed", Long.toString(executedNum));
					currentStat.put("execute_latency", Double.toString(executeLatency));
					currentStat.put("num_of_executors", Integer.toString(1));
					currentStat.put("num_of_acked", Long.toString(ackedNum));
					boltsInfo.put(componentId, currentStat);
				} else {
					// if the executor belongs to a existing bolt, calculate the new total tuple number and avg_latency for
					// the bolt
					Map<String, String> currentBoltInfo = boltsInfo.get(componentId);
					Long newExecuted = Long.valueOf(currentBoltInfo.get("num_of_executed")) + executedNum;
					Long newAcked = Long.valueOf(currentBoltInfo.get("num_of_acked")) + ackedNum;
					Double newAvgLatency = (Double.valueOf(currentBoltInfo.get("execute_latency")) * Integer.parseInt(currentBoltInfo.get("num_of_executors")) 
								            + executeLatency) / (Integer.parseInt(currentBoltInfo.get("num_of_executors")) + 1);
					currentStat.put("num_of_executed", Long.toString(newExecuted));
					currentStat.put("num_of_acked", Long.toString(newAcked));
					currentStat.put("execute_latency", Double.toString(newAvgLatency));
					currentStat.put("num_of_executors", Integer.toString(Integer.parseInt(currentBoltInfo.get("num_of_executors")) + 1));
					boltsInfo.put(componentId, currentStat);
				}
			}
		}
		storedBoltsInfo = boltsInfo;
		return boltsInfo;
	}
	
	public Map<String, String> getBoltInfoByName(String boltName, String topName) {
		Map<String, String> boltInfo;
		getAllBoltsInfo(topName);
		
		boltInfo = storedBoltsInfo.get(boltName);
		if(boltInfo == null) {
			System.out.println("No Information on bolt '" + boltName + "' now. Wrong component name or not available.");
		} 
		return boltInfo;
	}

	void finish() {
		transport.close();
	}
	
	/* Get TopologyInfo data structure using topology name */
	public TopologyInfo getTopologyInfobyName(String targetName) {
		TopologyInfo topInfo = new TopologyInfo();
		Iterator<TopologySummary> topologiesIterator = summary.get_topologies_iterator();
		while(topologiesIterator.hasNext()) {
			// Check the name of the current topology
			TopologySummary topology = topologiesIterator.next();
			if(targetName.equals(topology.get_name())) {
				try {
					topInfo = client.getTopologyInfo(topology.get_id());
				} catch (NotAliveException e) {
					e.printStackTrace();
				} catch (TException e) {
					e.printStackTrace();
				}
				break;
			} 
		}
		return topInfo;
	}
	
	/* method to parse double Map values returned from some of BoltStats method */
	public static Double getBoltStatDoubleValueFromMap(Map<String, Map<GlobalStreamId, Double>> map, String statName) {
		Double statValue = 0.0;
		Map<GlobalStreamId, Double> boltStatMap = map.get(statName);
		Set<GlobalStreamId> key = boltStatMap.keySet();
		if(key.size() > 0) {
			Iterator<GlobalStreamId> itr = key.iterator();
			statValue = boltStatMap.get(itr.next());
		}
		return statValue;
	}
	
	/* method to parse long Map values returned from some of BoltStats method */
	public static Long getBoltStatLongValueFromMap(Map<String, Map<GlobalStreamId, Long>> map, String statName) {
		if(map != null) {
			Long statValue = null;
			Map<GlobalStreamId, Long> boltStatMap = map.get(statName);
			Set<GlobalStreamId> key = boltStatMap.keySet();
			if(key.size() > 0) {
				Iterator<GlobalStreamId> itr = key.iterator();
				statValue = boltStatMap.get(itr.next());
			}
			return statValue;
		}
		return 0L;
	}
	
}

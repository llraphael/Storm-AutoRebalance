package com.cloudamize.storm.dynscheduler;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.thrift7.TException;

import backtype.storm.generated.Nimbus.Client;
import backtype.storm.generated.RebalanceOptions;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

public class DynamicScheduler {

	// Calculate the capacity of each bolt. timeWindow is in terms of second.
	public static Map<String, Double> getCapacity(Map<String, Map<String, String>> boltsInfo, int timeWindow) {
		Map<String, Double> capacityResult = new HashMap<String, Double>();
		int activateTime = Integer.parseInt(boltsInfo.get("static_time").get("up_time"));
		
		int timeBase = activateTime > timeWindow ? timeWindow : activateTime;
		boltsInfo.remove("static_time");
		
		for(Map.Entry<String, Map<String, String>> entry1 : boltsInfo.entrySet()) {
			String boltName = entry1.getKey();
			Map<String, String> currentBolt = entry1.getValue();
			
			// Get #executed and #execute_latency for the current bolt
			Long numOfExecuted = null;
			Double avgExecuteLatency = null;
			for(Map.Entry<String, String> entry2 : currentBolt.entrySet()) {
				if(entry2.getKey().equals("num_of_executed"))
					numOfExecuted = Long.valueOf(entry2.getValue());
				if(entry2.getKey().equals("execute_latency"))
					avgExecuteLatency = Double.valueOf(entry2.getValue());
			}
			
			// Calculate the capacity for current bolt (actually for each executor)
			if(numOfExecuted == null || avgExecuteLatency == null) {
				System.out.println("Bolts info incomplete. Capacity can not be calculated!");
				return capacityResult;
			} else {
				int numOfExecutors = Integer.parseInt(currentBolt.get("num_of_executors"));
				// Capacity formula
				Double cap = numOfExecuted * avgExecuteLatency / (timeBase * 1000) / numOfExecutors; 
				capacityResult.put(boltName, cap);
			}
		}
		
		// Iterate through the bolts Info
		System.out.println();
		System.out.println("*****Time Window: " + timeBase + "s*****");
		for(Map.Entry<String, Map<String, String>> entry1 : boltsInfo.entrySet()) {
			System.out.println("Bolt Name: " + entry1.getKey());
			Map<String, String> oneBoltInfo = entry1.getValue();
			for(Map.Entry<String, String> entry2 : oneBoltInfo.entrySet()) {
				System.out.println(entry2.getKey() + ": " + entry2.getValue());
			}
		}

		for(Map.Entry<String, Double> entry : capacityResult.entrySet()) {
			System.out.println("Bolt Name: " + entry.getKey() + ". Capacity: " + entry.getValue());
		}
		
		return capacityResult;
	}
	
	
	public static void main(String[] args) throws InterruptedException {
        
		
		ScheduledExecutorService timeManager = Executors.newScheduledThreadPool(1);
		AutoRebalancer balancer = new AutoRebalancer("matlab"); 
		
		// Time interval between rebalance should not be too short, which would cause
		// the failure of reading executor status. A minimum value should be 120s.
		ScheduledFuture<?> taskHandler = timeManager.scheduleWithFixedDelay(balancer, 10, 20, TimeUnit.SECONDS);
		
		timeManager.schedule(new Runnable() {
			public void run() {
				System.out.println("Auto Balancing program stopped");
				taskHandler.cancel(true);
				timeManager.shutdown();}
		}, 200, TimeUnit.SECONDS);
		
		System.out.println("Last line reached!"); 
		
		/*
		TopologyComponentInfo info = new TopologyComponentInfo("192.168.69.79");
		info.setTimeWindow(600);
		Map<String, String> finalBoltStats = info.getBoltInfoByName("finalbolt", "matlab");
		if(finalBoltStats != null) {
			System.out.println("Acked number: " + finalBoltStats.get("num_of_acked"));
			System.out.println("Executed number: " + finalBoltStats.get("num_of_executed"));
			System.out.println("Latency: " + finalBoltStats.get("execute_latency"));
		} 
		*/
	}

	
}

class AutoRebalancer implements Runnable {
	
	String topologyName;
	String nimbusIP = "192.168.69.79";
	int timeWindow = 600;
	
	AutoRebalancer(String topName) {
		this.topologyName = topName;
	}
	
	AutoRebalancer(String topName, int timeWindow) {
		this.topologyName = topName;
		this.timeWindow = timeWindow;
	}
	
	@Override
	public void run() {
		
		TopologyComponentInfo componentsInfo = new TopologyComponentInfo(nimbusIP);

		// Set time window for the topology info
		componentsInfo.setTimeWindow(timeWindow);
		
		// Get bolts Info
		Map<String, Map<String, String>> boltsInfo = componentsInfo.getAllBoltsInfo(topologyName);

		if(boltsInfo.isEmpty()) {
			System.out.println("Bolts information are not available this time! Will check later.");
			return;
		}

		// Get capacity
		Map<String, Double> capacity = DynamicScheduler.getCapacity(boltsInfo, timeWindow);

		// Capacity Threshold
		double upThreshold = 0.9, lowThreshold = 0.2;
		
		// The new decided number of executors that should be assigned each bolt.
		Map<String, Integer> newAssignment = new HashMap<String, Integer>();
		
		// Indicate that if there is actually a change in the number of newly decided executor 
		// numbers for each bolt.
		int changeNumber = 0;
		
		// Check each bolt's capacity and determine the new number of executors for each bolt
		for(Map.Entry<String, Double> entry : capacity.entrySet()) {
			Integer oldNumber = Integer.parseInt(boltsInfo.get(entry.getKey()).get("num_of_executors"));
			Double boltCap = capacity.get(entry.getKey());

			if(boltCap > upThreshold) {
				newAssignment.put(entry.getKey(), oldNumber+1);
				changeNumber++;
			} else if(boltCap < lowThreshold && oldNumber > 1) {
				//If the number of executor is 1, it can not be reduced further
				newAssignment.put(entry.getKey(), oldNumber-1);
				changeNumber++;
			} else {
				newAssignment.put(entry.getKey(), oldNumber);
			}
		}

		// Rebalance the topology
		if(changeNumber > 0) {
			Map conf = Utils.readStormConfig();
			Client client = NimbusClient.getConfiguredClient(conf).getClient();
			RebalanceOptions reOptions = new RebalanceOptions();
			reOptions.set_num_executors(newAssignment);
			reOptions.set_wait_secs(10);
			try {
				client.send_rebalance(topologyName, reOptions);
				System.out.println("Topology " + topologyName + " is being rebalanced, new number of executors for each bolt:");
				for(Map.Entry<String, Integer> entry : newAssignment.entrySet()) {
					System.out.println(entry.getKey() + ": " + entry.getValue());
				}
			} catch (TException e) {
				e.printStackTrace();
			}
		} else {
			System.out.println("There is no need to rebalance \"" + topologyName + "\"");
		}
		
		componentsInfo.finish();
	}
}


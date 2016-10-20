package org.mlgb.storm.back_propagation.service;

public class LatencySimulator {
	public static void simulate(int latencyInMillis){
		try {
			beLatent(latencyInMillis);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	private static void beLatent(int latency) throws InterruptedException {
		// TODO Auto-generated method stub
		Thread.sleep(latency);
	}

}

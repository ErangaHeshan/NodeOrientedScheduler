package org.apache.storm.scheduler;


import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.shade.org.json.simple.JSONObject;
import org.apache.storm.shade.org.json.simple.parser.JSONParser;
import org.apache.storm.shade.org.json.simple.parser.ParseException;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Eranga Heshan on 11/28/16.
 */

public class NodeBasedCustomScheduler implements IScheduler {
    private static final String NODE = "node";
    private List<WorkerSlot> availableSlots;

    @Override
    public void prepare(Map conf) {}

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        Collection<TopologyDetails> topologyDetails = topologies.getTopologies();
        Collection<SupervisorDetails> supervisorDetails = cluster.getSupervisors().values();
        Map<String, SupervisorDetails> supervisors = new HashMap<String, SupervisorDetails>();
        JSONParser parser = new JSONParser();
        for(SupervisorDetails s : supervisorDetails){
            Map<String, String> metadata = (Map<String, String>)s.getSchedulerMeta();
            if(metadata.get(NODE) != null){
                supervisors.put((String)metadata.get(NODE), s);
            }
        }

        for(TopologyDetails t : topologyDetails){
            //if(cluster.needsScheduling(t)) continue;
            StormTopology topology = t.getTopology();
            Map<String, Bolt> bolts = topology.get_bolts();
            Map<String, SpoutSpec> spouts = topology.get_spouts();
            try{


                for(String name : bolts.keySet()){
                    Bolt bolt = bolts.get(name);
                    JSONObject conf = (JSONObject)parser.parse(bolt.get_common().get_json_conf());
                    if(conf.get(NODE) != null && supervisors.get(conf.get(NODE)) != null){
                        String node = (String)conf.get(NODE);
                        SupervisorDetails supervisor = supervisors.get(node);
                        List<WorkerSlot> availableSlots = cluster.getAvailableSlots(supervisor);
                        List<ExecutorDetails> executors = cluster.getNeedsSchedulingComponentToExecutors(t).get(name);
                        if(!availableSlots.isEmpty() && executors != null){
                            cluster.assign(availableSlots.get(0), t.getId(), executors);
                        }
                    }
                }
                for(String name : spouts.keySet()){
                    SpoutSpec spout = spouts.get(name);
                    JSONObject conf = (JSONObject)parser.parse(spout.get_common().get_json_conf());
                    if(conf.get(NODE) != null && supervisors.get(conf.get(NODE)) != null){
                        String node = (String)conf.get(NODE);
                        SupervisorDetails supervisor = supervisors.get(node);
                        List<WorkerSlot> availableSlots = cluster.getAvailableSlots(supervisor);
                        List<ExecutorDetails> executors = cluster.getNeedsSchedulingComponentToExecutors(t).get(name);
                        if(!availableSlots.isEmpty() && executors != null){
                            cluster.assign(availableSlots.get(0), t.getId(), executors);
                        }
                    }
                }
            }catch(ParseException pe){
                pe.printStackTrace();
            }
        }
    }
}

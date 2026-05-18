package com.test.LoadBalancerDemo.streamProcessor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.test.LoadBalancerDemo.streamProcessor.boltDetails.*;
import com.test.LoadBalancerDemo.streamProcessor.configs.ApplicationSettings;
import com.test.LoadBalancerDemo.streamProcessor.metrics.BoltLatencyDetails;
import com.test.LoadBalancerDemo.streamProcessor.metrics.CandidateEvaluationMetrics;
import com.test.LoadBalancerDemo.streamProcessor.metrics.RemovalCandidate;
import com.test.LoadBalancerDemo.streamProcessor.requests.RequestToAddAReplica;
import com.test.LoadBalancerDemo.streamProcessor.requests.RequestToRemoveReplica;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.eclipse.paho.client.mqttv3.*;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import static com.test.LoadBalancerDemo.streamProcessor.BottleneckSorter.sortByPriority;

public class LoadBalancer {
    public final String filePath = "message_processing_times.csv";
    public IMqttClient mqttClient;
    public IMqttClient mqttClientForPorts;
    public IMqttClient mqttClientForGBs;
    public IMqttClient messagePublisher;
    public IMqttClient startTimeListener;
    public IMqttClient emitRateListener;
    public IMqttClient bNSignalsListener;
    public IMqttClient endTimeListener;
    public HashMap<String, BoltInfo> boltRecords;
    //the key is bolt name and the value is name of all DS bolts
    public HashMap<String, List<String>> topology;
    //the key is nameOfOriginalBolt like b2 and the value is the name of replicas
    public HashMap<String, List<String>> replicas;
    public int numberOfOperatorsInTopology = 0;
    public ObjectMapper objectMapper;
    public ApplicationSettings applicationSettings;
    public HashMap<String, BoltLatencyDetails> latencyRecordsFromUpStreamBolts;
    public HashMap<String, BoltLatencyDetails> latencyRecordsFromDownStreamBolts;
    // public HashMap<String, List<Integer>> listOfFreeServerPorts;
    public ConcurrentHashMap<String, BlockingQueue<Integer>> listOfFreeServerPorts;
    public HashMap<String, MessageProcessingTimeUnit> messageStartTimeBatch;
    public HashMap<String, Long> messageEndTimeBatch;
    public List<LatencyInfo> latencyInfoList;
    //the key is vm name
    public HashMap<String, BandwidthInfo> bandwidthTable;
    public ExecutorService executorService;
    public BufferedWriter writer;
    public FileWriter writerForBWCsvFile;
    public FileWriter writerForLatencyFile;
    //the key is vm name
    public HashMap<String, RemainingBandwidthInfoOfVm> tableOfRBWOfVms;
    public ExecutorService threadPool;
    public int metricPrecision = (int) Math.pow(10, 3); // 10^3 for 3 decimal places
    public ScheduledExecutorService rBWExecutor;
    public ScheduledExecutorService globalAdaptationExecutor;

    public int emitRate = 0;
    public File cpuUsageCsvFile;
    public File remainingBWCsvFile;
    public File latencyCsvFile;
    public int cycle = 0;
    /* public Set<String> listOfVmsWithInBWBottleNeck;
     public Set<String> listOfVmsWithOutBWBottleNeck;
     public Set<String> listOfVmsWithCPUBottleNeck;
     */
    public Multimap<String, BottleneckDetails> listOfVmsWithBottleNeck;
    public Set<String> listOfComponentsWithBWBottleNeck = new HashSet<>();


    public HashMap<String, BottleneckDetails> vmsWithCPU_BN = new HashMap<>();
    public Set<String> listOfUnderUsedBolts = new HashSet<>();
    public ExecutorService executorForUnderUtilization;
    public ExecutorService executorForGlobalAdaptation;
    public ExecutorService fileWriterExecutorForEndTimeBatch = Executors.newSingleThreadExecutor(); // Separate thread for file writing
    public ExecutorService fileWriterExecutorForStartTimeBatch = Executors.newSingleThreadExecutor(); // Separate thread for file writing
    public int numberOfReadyOperators = 0;
    public int flushCountAfterStop = 0;
    public ScheduledExecutorService schedulerForRemainderDataAfterStop = Executors.newScheduledThreadPool(1);
    public HashSet<String> opsSentUpstreamAck = new HashSet<>();
    public boolean programStarted = false;
    //the key is hostName and value is the latest cycle in which CPU BN happened
    public HashMap<String, Queue<Integer>> recordsOfCPUBN;
    public int globalAdaptationInterval = 0;


    public LoadBalancer() {
        boltRecords = new HashMap<String, BoltInfo>();
        topology = new HashMap<String, List<String>>();
        objectMapper = new ObjectMapper();
        replicas = new HashMap<String, List<String>>();
        latencyRecordsFromUpStreamBolts = new HashMap<String, BoltLatencyDetails>();
        latencyRecordsFromDownStreamBolts = new HashMap<String, BoltLatencyDetails>();
        //  listOfFreeServerPorts = new HashMap<String, List<Integer>>();
        loadLatencyFile();
        executorService = Executors.newFixedThreadPool(10);
        messageStartTimeBatch = new HashMap<String, MessageProcessingTimeUnit>();
        messageEndTimeBatch = new HashMap<String, Long>();

       /* listOfVmsWithInBWBottleNeck = new HashSet<>();
        listOfVmsWithOutBWBottleNeck = new HashSet<>();
        listOfVmsWithCPUBottleNeck = new HashSet<>();
        */
        listOfVmsWithBottleNeck = ArrayListMultimap.create();
        // Write the header to the file
        // Schedule periodic flushing of the HashMap to the file

        //read bandwidth file
        loadEdgeDeviceBandwidthInfo();
        threadPool = Executors.newFixedThreadPool(40);
        recordsOfCPUBN = new HashMap<>();
        listOfFreeServerPorts = new ConcurrentHashMap<>();
    }

    public void subscribeToBNSignals(String topic) {
        bNSignalsListener.setCallback(new MqttCallback() {

            @Override
            public void connectionLost(Throwable throwable) {
                System.out.println("The connection of load balancer to broker is broken");
                throwable.printStackTrace();
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) {
                String messagePayload = new String(message.getPayload());
                System.out.println("A message arrived with the topic = " + topic + " and the payload is " + messagePayload);
                if (topic.equals("underUtilization")) {
                    System.out.println("A message arrived with the topic = " + topic + " and the payload is " + messagePayload);
                    if (messagePayload.startsWith("b") || !boltRecords.get(messagePayload).getNameOfClassLoadedInside().equals("none")) {
                        if (topology.containsKey(messagePayload)) {
                            listOfUnderUsedBolts.add(messagePayload);
                        }
                    }
                }
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {
            }
        });

        try {
            bNSignalsListener.subscribe(topic, 1);
            System.out.println("Subscribed to topic= " + topic);
        } catch (MqttException e) {
            System.out.println("Error subscribing to " + topic + " topic");
            throw new RuntimeException(e);
        }
    }


    public int[] getLastTwoCyclesWithCPUBN(String hostName) {
        Queue<Integer> queue = recordsOfCPUBN.get(hostName);
        if (queue == null || queue.size() < 2) {
            return null; // Not enough data
        }
        int[] cycles = new int[2];
        Iterator<Integer> it = queue.iterator();
        cycles[0] = it.next(); // Older cycle
        cycles[1] = it.next(); // Newer cycle
        return cycles;
    }

    public void addCycleNumberToRecordsOfCPUBN(String hostName, int cycleNumber) {
        Queue<Integer> queue = recordsOfCPUBN.computeIfAbsent(hostName, k -> new LinkedList<>());
        if (queue.size() == 2) {
            queue.poll(); // Remove the oldest cycle number
        }
        queue.add(cycleNumber); // Add the new cycle number
    }

    public void manageUnderUtilizationSignal() {
        System.out.println("just entered manageUnderUtilizationSignal");
        Iterator<String> iterator = listOfUnderUsedBolts.iterator();
        while (iterator.hasNext()) {
            String underUsedOpName = iterator.next();
            try {
                if (boltRecords.get(underUsedOpName) == null ||
                        "none".equals(boltRecords.get(underUsedOpName).getNameOfClassLoadedInside())) {
                    System.out.println("Bolt " + underUsedOpName + " no longer exists, skipping");
                    iterator.remove();
                    continue;
                }
                List<String> listOfAllBoltsWithSameFunctionality = findAllBoltNamesWithSameFunctionality(underUsedOpName);
                String nameOfBoltToBeDeleted = null;
                HashMap<String, RemovalCandidate> listOfCandidatesForRemoval = checkIfAReplicaMustBeRemoved(listOfAllBoltsWithSameFunctionality);

                if (listOfCandidatesForRemoval != null && !listOfCandidatesForRemoval.isEmpty()) {
                    System.out.println("listOfCandidatesForRemoval= " + listOfCandidatesForRemoval.toString());
                    nameOfBoltToBeDeleted = identifyAReplicaToBeDeleted(listOfCandidatesForRemoval);
                    if (nameOfBoltToBeDeleted != null) {
                        System.out.println("nameOfBoltToBeDeleted = " + nameOfBoltToBeDeleted);
                        removeAReplica(new RequestToRemoveReplica(findNameOfOriginalBoltByReplicaName(nameOfBoltToBeDeleted), nameOfBoltToBeDeleted));
                    }
                }
            } catch (Exception e) {
                System.err.println("Error processing under-utilized bolt: " + underUsedOpName);
                e.printStackTrace();
            }

            iterator.remove(); // Safe removal
        }
    }

    public boolean manageReplicaCreationBasedOnRemainingInBandwidth(String hostName) {

        //double check for updated information
        if (1 - tableOfRBWOfVms.get(hostName).getRemainingInBandwidthPercentage() > applicationSettings.getMax_throughput_threshold()) {
            System.out.println("there is a bottleNeck in terms of in_bandwidth in vm = " + hostName);
            String nameOfResourceIntensiveBolt = findBoltWithHighestInBandwidthUsage(hostName);
            System.out.println("name Of resource intensive bolt: " + nameOfResourceIntensiveBolt);
/*

            List<String> listOfBoltsWithSameFunctionality = findAllBoltNamesWithSameFunctionality(nameOfResourceIntensiveBolt);
            for (String s : listOfBoltsWithSameFunctionality) {
                String hostNameOfOp = findHostNameOfOperator(s);
                double rinBWP= tableOfRBWOfVms.get(hostNameOfOp).getRemainingInBandwidthPercentage();
                if(rinBWP>= 0.7){
                    System.out.println("new method returned TRUUUEE");
                    boolean replicationResult = createReplicaBasedOnBestCandidate(nameOfResourceIntensiveBolt, " InBW");
                    return replicationResult;
                }
            }
*/

            if (nameOfResourceIntensiveBolt != null) {
                boolean result = isAverageRemainingInBandwidthBelowThreshold(nameOfResourceIntensiveBolt);
                System.out.println("isAverageRemainingInBandwidthBelowThreshold returns " + result);
                if (result) {
                    boolean replicationResult= createReplicaBasedOnBestCandidate(nameOfResourceIntensiveBolt, " InBW");
                    return replicationResult;
                } else return false;
            }
        }
        return false;
    }

    public boolean manageReplicaCreationBasedOnRemainingOutBandwidth(String hostName) {
        if (1 - tableOfRBWOfVms.get(hostName).getRemainingOutBandwidthPercentage() > applicationSettings.getMax_throughput_threshold()) {
            System.out.println("there is a bottleNeck in terms of out_bandwidth in vm = " + hostName);
            String nameOfResourceIntensiveBolt = findBoltWithHighestOutBandwidthUsage(hostName);
            System.out.println("name Of resource intensive bolt: " + nameOfResourceIntensiveBolt);
            if (nameOfResourceIntensiveBolt != null) {
                boolean result = isAverageRemainingOutBandwidthBelowThreshold(nameOfResourceIntensiveBolt);
                System.out.println("isAverageRemainingOutBandwidthBelowThreshold returns " + result);
                if (result) {
                    boolean replicationResult = createReplicaBasedOnBestCandidate(nameOfResourceIntensiveBolt, " OutBW");
                    return replicationResult;
                } else return false;
            }
        }
        return false;
    }

    public boolean manageReplicaCreationBasedOnCPU(String hostName) {
        double averageCPUUsageOfVM_updated = getAverageCPuUsageOfAllBoltsOnANode(hostName);
        if (averageCPUUsageOfVM_updated < applicationSettings.getMax_cpu_threshold()) {
            return false;
        }
        List<BoltInfo> allBoltsOnTheHost = getBoltsRunningOnHost(hostName);
        if (allBoltsOnTheHost != null) {
            allBoltsOnTheHost = sortAListOfBoltInfoAccordingToCPUUsageAtBoltLevel(allBoltsOnTheHost);
        }
        if (allBoltsOnTheHost != null) {
            for (int i = allBoltsOnTheHost.size() - 1; i >= 0; i--) {

                System.out.println("name Of cpu-congested bolt for replication : " + allBoltsOnTheHost.get(i).getComponentName());

                boolean result = isAverageCPUAboveThreshold(allBoltsOnTheHost.get(i).getComponentName());
                System.out.println("checkIfANewReplicaIsRequired returns " + result);
                if (result) {
                    boolean replicationResult = createReplicaBasedOnBestCandidate(allBoltsOnTheHost.get(i).getComponentName(), " CPU");
                    if (replicationResult) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    //false means the replication not happened
//true means a replica created
    public boolean createReplicaBasedOnBestCandidate(String nameOfResourceIntensiveBolt, String cause) {
        HashMap<String, CandidateEvaluationMetrics> listOfAllCandidates = identifyCandidatesForSpanningAReplica(nameOfResourceIntensiveBolt);
        if (listOfAllCandidates != null && listOfAllCandidates.size() > 0) {
            String chosenBoltToBeAReplica = calculateScoreForAllCandidatesAndSelectTheBestCandidate(listOfAllCandidates);
            String originalBolName = findOriginalBoltNameRelatedToAComponentName(nameOfResourceIntensiveBolt);
            addANewReplica(new RequestToAddAReplica(boltRecords.get(nameOfResourceIntensiveBolt).getNameOfClassLoadedInside(), chosenBoltToBeAReplica, originalBolName), cause);
            return true;
        } else {
            System.out.println("there is no candidate");
            return false;
        }
    }


    public void connectToBroker() {
        //create a new IMqttClient synchronous instance:
        //The server endpoint we're using is a public MQTT broker hosted
        // by the Paho project, which allows anyone with an internet connection to test clients without
        // the need of any authentication

        System.out.println("load balancer is connecting to the broker");

        try {
            mqttClient = new MqttClient("tcp://192.168.122.98:1883", "LoadBalancer1");
            mqttClientForPorts = new MqttClient("tcp://192.168.122.98:1883", "LoadBalancer2");
            mqttClientForGBs = new MqttClient("tcp://192.168.122.98:1883", "LoadBalancer3");
            messagePublisher = new MqttClient("tcp://192.168.122.98:1883", "LoadBalancer4");
            startTimeListener = new MqttClient("tcp://192.168.122.98:1883", "LoadBalancer5");
            endTimeListener = new MqttClient("tcp://192.168.122.98:1883", "LoadBalancer6");
            emitRateListener = new MqttClient("tcp://192.168.122.98:1883", "LoadBalancer7");
            bNSignalsListener = new MqttClient("tcp://192.168.122.98:1883", "LoadBalancer8");

        } catch (MqttException e) {
            System.out.println("there is a problem in creating a mqtt client");
            throw new RuntimeException(e);
        }
        MqttConnectOptions connectOptions = new MqttConnectOptions();
        connectOptions.setCleanSession(false);
        connectOptions.setAutomaticReconnect(true);
        connectOptions.setKeepAliveInterval(300);
        connectOptions.setConnectionTimeout(300);
        try {
            mqttClient.connect(connectOptions);
            mqttClientForPorts.connect(connectOptions);
            mqttClientForGBs.connect(connectOptions);
            messagePublisher.connect(connectOptions);
            startTimeListener.connect(connectOptions);
            endTimeListener.connect(connectOptions);
            emitRateListener.connect(connectOptions);
            bNSignalsListener.connect(connectOptions);
        } catch (MqttException e) {
            System.out.println("there is a problem in connection between load balancer and broker");
            throw new RuntimeException(e);
        }
    }

    public void publishAMessage(String topic, String payload) {
        MqttMessage mqttMessage = new MqttMessage();
        mqttMessage.setQos(2);
        mqttMessage.setRetained(false);
        mqttMessage.setPayload(payload.getBytes(StandardCharsets.UTF_8));

        try {
            messagePublisher.publish(topic, mqttMessage);
            System.out.println("the message with topic= " + topic + " and payload= " + payload + " was successfully published");
        } catch (MqttException e) {
            System.out.println("there is a problem in publishing messages in the load balancer");
            throw new RuntimeException(e);
        }
    }

    public void subscribeToEmitRate(String topic) {
        emitRateListener.setCallback(new MqttCallback() {

            @Override
            public void connectionLost(Throwable throwable) {
                System.out.println("The connection of load balancer to broker is broken");
                throwable.printStackTrace();
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) {
                String messagePayload = new String(message.getPayload());
                System.out.println("A message arrived with the topic = " + topic + " and the payload is " + messagePayload);
                if (topic.endsWith("/ready")) {
                    numberOfReadyOperators++;
                } else if (topic.endsWith("upstreamAck")) {
                    opsSentUpstreamAck.add(topic.split("/")[0]);
                    System.out.println("upstream ack so far: " + opsSentUpstreamAck);
                } else if (topic.endsWith("/emitRate")) {
                    emitRate = Integer.parseInt(messagePayload);
                    System.out.println("emitRate= " + emitRate);
                    if (cycle > 0 && emitRate == 0) {

                        synchronized (messageStartTimeBatch) {
                            if (!messageStartTimeBatch.isEmpty()) {
                                // Create a snapshot and clear the batch
                                // Submit the batch to be written asynchronously
                                writeStartTimeToFile(messageStartTimeBatch);
                                messageStartTimeBatch.clear();
                            }
                        }


                        synchronized (messageEndTimeBatch) {
                            if (!messageEndTimeBatch.isEmpty()) {
                                // Create a snapshot and clear the batch
                                // Submit the batch to be written asynchronously
                                writeEndTimeToFile(messageEndTimeBatch);
                                messageEndTimeBatch.clear();
                            }
                        }

                    }
                }

            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {
            }
        });

        try {
            emitRateListener.subscribe(topic);
            System.out.println("Subscribed to topic= " + topic);
        } catch (MqttException e) {
            System.out.println("Error subscribing to " + topic + " topic");
            throw new RuntimeException(e);
        }
    }

    public void subscribeToProcessingTime_start(String topic) {
        startTimeListener.setCallback(new MqttCallback() {
            String[] parts;

            @Override
            public void connectionLost(Throwable throwable) {
                System.out.println("The connection of load balancer to broker is broken");
                throwable.printStackTrace();
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) {
                String messagePayload = new String(message.getPayload());
                //   System.out.println("A message arrived with the topic = " + topic + " and the payload is " + messagePayload);
                if (topic.endsWith("/processingTime/start")) {
                    parts = messagePayload.split("/");
                    if (emitRate != 0) {
                        messageStartTimeBatch.put(parts[0], new MessageProcessingTimeUnit(Long.parseLong(parts[1]), Long.parseLong(parts[2]), emitRate));
                        if (messageStartTimeBatch.size() >= 100) {
                            writeStartTimeToFile(messageStartTimeBatch);
                            messageStartTimeBatch.clear();
                        }
                    } else {
                        synchronized (messageStartTimeBatch) {
                            messageStartTimeBatch.put(parts[0], new MessageProcessingTimeUnit(Long.parseLong(parts[1]), Long.parseLong(parts[2]), emitRate));
                            writeStartTimeToFile(messageStartTimeBatch);
                            messageStartTimeBatch.clear();
                        }
                    }
                }
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {
            }
        });

        try {
            startTimeListener.subscribe(topic);
            System.out.println("Subscribed to topic= " + topic);
        } catch (MqttException e) {
            System.out.println("Error subscribing to " + topic + " topic");
            throw new RuntimeException(e);
        }
    }

    public void writeStartTimeToFile(HashMap<String, MessageProcessingTimeUnit> batch) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(latencyCsvFile, true))) {
            for (var entry : batch.entrySet()) {
                // Write in the desired format: messageId,//for endTime,startTime,cycle,EmitRate,//For Event
                writer.write(entry.getKey() + ",," + entry.getValue().getStartingTime() + "," + entry.getValue().getCycle() + "," + entry.getValue().getEmitRate() + ",");
                writer.newLine();
            }
            System.out.println("start time Batch has written to file.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void subscribeToProcessingTime_end(String topic) {

        endTimeListener.setCallback(new MqttCallback() {
            String[] parts;

            @Override
            public void connectionLost(Throwable throwable) {
                System.out.println("The connection of load balancer to broker is broken");
                throwable.printStackTrace();
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) {
                String messagePayload = new String(message.getPayload());
                //  System.out.println("A message arrived with the topic = " + topic + " and the payload is " + messagePayload);

                if (topic.endsWith("/processingTime/end")) {
                    parts = messagePayload.split("/");
                    if (emitRate != 0) {
                        messageEndTimeBatch.put(parts[0], Long.parseLong(parts[1]));


                        if (messageEndTimeBatch.size() >= 100) {
                            writeEndTimeToFile(messageEndTimeBatch);
                            messageEndTimeBatch.clear();
                        }
                    } else {
                        synchronized (messageEndTimeBatch) {
                            messageEndTimeBatch.put(parts[0], Long.parseLong(parts[1]));
                            writeEndTimeToFile(messageEndTimeBatch);
                            messageEndTimeBatch.clear();
                        }
                    }
                }
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {
            }
        });

        try {
            endTimeListener.subscribe(topic);
            System.out.println("Subscribed to topic= " + topic);
        } catch (MqttException e) {
            System.out.println("Error subscribing to " + topic + " topic");
            throw new RuntimeException(e);
        }
    }

    public void writeEndTimeToFile(HashMap<String, Long> batch) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(latencyCsvFile, true))) {
            for (var entry : batch.entrySet()) {
                // Write in the desired format: messageId, endTime,//for startTime,//for cycle,for EmitRate,For Event
                writer.write(entry.getKey() + "," + entry.getValue() + ",,,,");
                writer.newLine();
            }
            System.out.println("end time Batch has written to file.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void subscribeToGenericBoltInitializationTopic(String topic) {
        mqttClientForGBs.setCallback(new MqttCallback() {
            @Override
            public void connectionLost(Throwable throwable) {
                System.out.println("The connection of mqttClientForGBs in load balancer to broker is broken");
                throwable.printStackTrace();
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) {
                String messagePayload = new String(message.getPayload());
                System.out.println("A message for /classLoaded arrived: " + messagePayload);
                if (topic.endsWith("/classLoaded")) {
                    System.out.println("A message arrived with the topic = " + topic + " and the payload is " + messagePayload);
                    String nameOfBolt = topic.substring(0, topic.indexOf("/"));
                    synchronized (boltRecords.get(nameOfBolt)) {
                        boltRecords.get(nameOfBolt).setNameOfClassLoadedInside(messagePayload);
                        boltRecords.get(nameOfBolt).notifyAll();
                    }
                }
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {
            }
        });

        try {
            mqttClientForGBs.subscribe(topic, 1);
            System.out.println("Subscribed to topic= " + topic);
        } catch (MqttException e) {
            System.out.println("Error subscribing to /classLoaded topic");
            throw new RuntimeException(e);
        }
    }

    public List<BoltInfo> informDSBltsOfNewReplica(String nameOfOriginalBolt, String replicaName) {
        List<String> listOfDSBoltsName = new ArrayList<String>();
        listOfDSBoltsName = topology.get(nameOfOriginalBolt);

        List<BoltInfo> dsBolts = new ArrayList<BoltInfo>();
        for (String name : listOfDSBoltsName) {
            BoltInfo boltInfo = boltRecords.get(name);
            // listOfFreeServerPorts.put(name, new LinkedBlockingQueue<>());
            listOfFreeServerPorts.computeIfAbsent(name, k -> new LinkedBlockingQueue<>());
            System.out.println("before sending newUpstreamGb, listOfFreeServerPorts=" + listOfFreeServerPorts.toString());
            publishAMessage(name + "/newUpstreamGb", replicaName + "/" + boltRecords.get(replicaName).getHostName());
            dsBolts.add(boltInfo);
        }

        for (BoltInfo boltInfo : dsBolts) {
       /*     synchronized (freePortsLock) {
                System.out.println("got the lock");
                while (listOfFreeServerPorts.isEmpty() || !listOfFreeServerPorts.containsKey(boltInfo.getComponentName()) || listOfFreeServerPorts.get(boltInfo.getComponentName()).isEmpty()) {
                    try {
                        System.out.println("Waiting for free port for: " + boltInfo.getComponentName());
                        freePortsLock.wait(); // ← Releases lock, allows callback to run
                        System.out.println("Woke up, checking ports again");
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return dsBolts;
                    }
                }

        */
            try {
                // Get the queue and wait for a port
                System.out.println("after try, listOfFreeServerPorts=" + listOfFreeServerPorts.toString());

                BlockingQueue<Integer> queue = listOfFreeServerPorts.get(boltInfo.getComponentName());
                System.out.println("after recalculating queue, queue.size=" + queue.size());

                System.out.println("after recalculating queue, queue=" + queue.toString());

                System.out.println("MAIN  queue@" +
                        System.identityHashCode(queue) + "  waiting for " + boltInfo.getComponentName());

                //  Integer port = queue.poll(10, TimeUnit.SECONDS);
                Integer port = queue.take();

                System.out.println("after take, port= " + port);
                System.out.println("Got port " + port + " for " + boltInfo.getComponentName());
                boltInfo.setServerPort(port);


            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return dsBolts;
            }

            // Proceed when the condition is met
            //  boltInfo.setServerPort(listOfFreeServerPorts.get(boltInfo.getComponentName()).get(0));
            //  listOfFreeServerPorts.get(boltInfo.getComponentName()).remove(0);

        }
        // for (String name : listOfDSBoltsName) {
        //      listOfFreeServerPorts.remove(name);
        //   }

        return dsBolts;

    }

    public void subscribeToFreeServerPortTopic(String topic) {
        mqttClientForPorts.setCallback(new MqttCallback() {
            @Override
            public void connectionLost(Throwable throwable) {
                System.out.println("The connection of load balancer to broker is broken");
                throwable.printStackTrace();
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) {
                System.out.println("A message for arrived: GOOOOOOOOOOODDDDDDDDDDDDDDDDDDDDDDDDDD NNNNNNNEEEEEEEEEEEWWWWWWWWWWS");

                System.out.println("A message for /freeServerPort arrived: " + message.getPayload().toString());
                String messagePayload = new String(message.getPayload());
                // Check if the topic is for free server ports
                if (topic.endsWith("freeServerPort")) {

                    // Validate topic format (e.g., "boltName/freeServerPort")
                    int slashIndex = topic.indexOf("/");
                    if (slashIndex == -1) {
                        System.err.println("Invalid topic format: " + topic);
                        return;
                    }
                    String nameOfBolt = topic.substring(0, slashIndex);

                    // Validate and parse the port number
                    try {
                        int port = Integer.parseInt(messagePayload);
                        // Add the port to the list, creating a new list if absent
                      /*  synchronized (freePortsLock) {
                            listOfFreeServerPorts.computeIfAbsent(nameOfBolt, k -> new ArrayList<>()).add(port);
                            freePortsLock.notifyAll();
                            System.out.println("Added port " + port + " for " + nameOfBolt);
                        }

                       */
                        //  BlockingQueue<Integer> queue = listOfFreeServerPorts.get(nameOfBolt);
                        BlockingQueue<Integer> queue = listOfFreeServerPorts.computeIfAbsent(nameOfBolt, k -> new LinkedBlockingQueue<>());
                        System.out.println("CALLBACK queue@" +
                                System.identityHashCode(queue) + "  for " + nameOfBolt);
                        queue.offer(port);
                        System.out.println("Added port " + port + " for " + nameOfBolt);

                    } catch (NumberFormatException e) {
                        System.err.println("Invalid port number: " + messagePayload);
                        return;
                    }
                    // Notify waiting threads that a port is available
                }

            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {
                // Handle completed delivery
            }
        });

        try {
            mqttClientForPorts.subscribe(topic);
            System.out.println("Subscribed to topic=" + topic);
        } catch (MqttException e) {
            System.out.println("Error subscribing to /freeServerPort topic");
            throw new RuntimeException(e);
        }
    }

    public void storeVMBandwidthInfoInCSVFile(int cycleNumber) {
        threadPool.submit(() -> {

            try {
                for (String vmName : tableOfRBWOfVms.keySet()) {
                    RemainingBandwidthInfoOfVm bandwidthInfo = tableOfRBWOfVms.get(vmName);
                    writerForBWCsvFile.append(vmName).append(",").append(String.valueOf(bandwidthInfo.getRemainingInBandwidth()))
                            .append(",").append(String.valueOf(bandwidthInfo.getRemainingOutBandwidth()))
                            .append(",").append(String.valueOf(bandwidthInfo.getRemainingInBandwidthPercentage()))
                            .append(",").append(String.valueOf(bandwidthInfo.getRemainingOutBandwidthPercentage()))
                            .append(",").append(String.valueOf(cycleNumber))
                            .append(",").append(String.valueOf(emitRate)).append(",").append("").append("\n");
                }
                writerForBWCsvFile.flush();
                //System.out.println("table of bandwidth info has been saved to the file");

            } catch (IOException e) {
                e.printStackTrace();
            }
        });

    }

    public void storeVMCPUUsageInCSVFile(String vmName, double cpuUsage, int cycleCount, int emitRate) {
        threadPool.submit(() -> {
            //File csvFile = new File("cpuUsage.csv");
            boolean isFileEmpty = cpuUsageCsvFile.length() == 0;  // Check if file is empty

            try (FileWriter writerCPU = new FileWriter(cpuUsageCsvFile, true)) { // Append mode enabled
                // Write header only if the file doesn't exist
                if (isFileEmpty) {
                    writerCPU.append("VM Name,CPU usage,cycleCount,EmitRate,Event\n");
                }

                // Write data

                writerCPU.append(vmName).append(",")
                        .append(String.valueOf(cpuUsage))
                        .append(",").append(String.valueOf(cycleCount))
                        .append(",").append(String.valueOf(emitRate))
                        .append(",").append("")
                        .append("\n");

                System.out.println("cpu usage info has been saved to the file");
                writerCPU.flush();

            } catch (IOException e) {
                e.printStackTrace();
            }
        });

    }


    public void subscribeToATopic(String topic) {

        mqttClient.setCallback(new MqttCallback() {
            @Override
            public void connectionLost(Throwable throwable) {
                System.out.println("The connection of load balancer to broker is broken");
                throwable.printStackTrace();
            }

            @Override
            public void messageArrived(String topic, MqttMessage mqttMessage) {
                String messagePayload = mqttMessage.toString();
                //System.out.println("A message arrived with the topic = " + topic);
                System.out.println("A message arrived with the topic = " + topic + " and the payload is " + messagePayload);

                if (topic.endsWith("initialInfo")) {

                    BoltInfo updatedBoltInfo = new BoltInfo();
                    try {
                        updatedBoltInfo = objectMapper.readValue(messagePayload, BoltInfo.class);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                    boltRecords.put(updatedBoltInfo.getComponentName(), updatedBoltInfo);
                    storeVMCPUUsageInCSVFile(updatedBoltInfo.getHostName(), updatedBoltInfo.getMetrics().getCpu(), 0, 0);

                } else if (topic.endsWith("metrics")) {
                    String[] parts = topic.split("/");

                    // Extract the componentName and count
                    String componentName = parts[0];  // "b2"
                    int count = cycle;

                    Metrics metrics = new Metrics();
                    try {
                        metrics = objectMapper.readValue(messagePayload, Metrics.class);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                    boltRecords.get(componentName).getMetrics().setCpu(metrics.getCpu());
                    boltRecords.get(componentName).getMetrics().setCpu_previous_cycle(metrics.getCpu_previous_cycle());
                    boltRecords.get(componentName).getMetrics().setCpuAtBoltLevel(metrics.getCpuAtBoltLevel());
                    boltRecords.get(componentName).getMetrics().setIn_throughput(metrics.getIn_throughput());
                    boltRecords.get(componentName).getMetrics().setOut_throughput(metrics.getOut_throughput());
                    storeVMCPUUsageInCSVFile(boltRecords.get(componentName).getHostName(), metrics.getCpu(), count, emitRate);

                } else if (topic.endsWith("topologyUpdate")) {
                    messagePayload = messagePayload.replace("[", "");
                    messagePayload = messagePayload.replace("]", "");
                    messagePayload = messagePayload.replace(" ", "");
                    String componentName = topic.substring(0, topic.indexOf("/"));
                    topology.put(componentName, Arrays.asList(messagePayload.split(",")));
                    System.out.println("the topology is: " + topology);
                } else if (topic.equals("applicationSettings")) {
                    //in order to be done only once
                    if (applicationSettings == null) {
                        try {
                            applicationSettings = new ApplicationSettings();
                            applicationSettings = objectMapper.readValue(messagePayload, ApplicationSettings.class);
                            numberOfOperatorsInTopology = applicationSettings.getNumberOfBoltsInTheSystem() + applicationSettings.getNumberOfSinksInTheSystem()
                                    + applicationSettings.getNumberOfSpoutsInTheSystem()
                                    + applicationSettings.getNumberOfGenericBoltsInTheSystem();
                            emitRate = Integer.parseInt(applicationSettings.getInitialEmitRate());
                            globalAdaptationInterval = applicationSettings.globalAdaptation_interval_in_second / applicationSettings.metric_collection_interval_in_second;
                            System.out.println("globalAdaptationInterval per cycle is " + globalAdaptationInterval);
                            System.out.println("emitRate after receiving applicationSettings= " + emitRate);
                            cpuUsageCsvFile = new File("/home/as00750/cpuUsage_" + applicationSettings.getMood() + ".csv");


                            remainingBWCsvFile = new File("/home/as00750/remainingBW_" + applicationSettings.getMood() + ".csv");
                            writerForBWCsvFile = new FileWriter(remainingBWCsvFile, true);
                            writerForBWCsvFile.append("VM Name,Remaining In Bandwidth,Remaining Out Bandwidth,Remaining In Bandwidth Percentage,Remaining Out Bandwidth Percentage,cycle,EmitRate,Event\n");
                            writerForBWCsvFile.flush();

                            latencyCsvFile = new File("/home/as00750/executionTime_" + applicationSettings.getMood() + ".csv");
                            writerForLatencyFile = new FileWriter(latencyCsvFile, true);
                            writer = new BufferedWriter(writerForLatencyFile);
                            writer.write("MessageID,EndTime,StartTime,cycle,EmitRate,Event\n");
                            writer.flush();  // Ensure the header is written out immediately


                            //save the applicationSettings in a file
                            saveApplicationSettingsInAFile(applicationSettings.toString());


                            //initiate a thread to let each op know about their upstream ops
                            ScheduledExecutorService executorServiceForUpStreamOps = Executors.newSingleThreadScheduledExecutor();
                            int numberOfOpsInSystem = applicationSettings.getNumberOfBoltsInTheSystem() +
                                    applicationSettings.getNumberOfSpoutsInTheSystem() +
                                    applicationSettings.getNumberOfGenericBoltsInTheSystem() +
                                    applicationSettings.getNumberOfSinksInTheSystem();
                            System.out.println("numberOfOpsInSystem= " + numberOfOpsInSystem);
                            Runnable task = new Runnable() {
                                @Override
                                public void run() {
                                    while (programStarted == false) {
                                        System.out.println("going to prepare the thread for upstream ops");
                                        while (boltRecords.size() != numberOfOpsInSystem) {
                                            System.out.println("boltRecord.size=" + boltRecords.size());
                                            try {
                                                Thread.sleep(5000);
                                            } catch (InterruptedException e) {
                                                throw new RuntimeException(e);
                                            }
                                        }


                                        sendOpsTheirUpstreams();

                                        while (numberOfReadyOperators < numberOfOpsInSystem) {
                                            try {
                                                Thread.sleep(5000);
                                            } catch (InterruptedException e) {
                                                throw new RuntimeException(e);
                                            }

                                        }
                                        System.out.println("now all ops are ready");
                                        try {
                                            Thread.sleep(5000);
                                        } catch (InterruptedException e) {
                                            throw new RuntimeException(e);
                                        }
                                        if (opsSentUpstreamAck.size() == applicationSettings.getNumberOfBoltsInTheSystem() + applicationSettings.getNumberOfSinksInTheSystem()) {
                                            publishAMessage("start", "1");
                                            programStarted = true;
                                        } else {
                                            sendOpsTheirUpstreams();
                                        }
                                    }

                                    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
                                    // Define the task to increment the value
                                    Runnable incrementTask = () -> {
                                        cycle++;
                                        System.out.println("cycle: " + cycle);
                                    };
                                    // Schedule the task at a fixed rate of 30 seconds
                                    scheduler.scheduleAtFixedRate(incrementTask, 0, applicationSettings.getMetric_collection_interval_in_second(), TimeUnit.SECONDS);
                                    calculateRemainingBandwidthOfVms(15, applicationSettings.getMetric_collection_interval_in_second());

                                    executorForGlobalAdaptation = Executors.newSingleThreadExecutor();
                                    executorForUnderUtilization = Executors.newSingleThreadExecutor();
                                }
                            };
                            executorServiceForUpStreamOps.schedule(task, 0, TimeUnit.SECONDS);

                        } catch (JsonMappingException e) {
                            throw new RuntimeException(e);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                } else if (topic.endsWith("bottleNeck/CPU")) {
                    System.out.println("A message arrived with the topic = " + topic + " and the payload is " + messagePayload);
                    String[] parts = topic.split("/");
                    String boltName = parts[0];
                    String hostName = parts[1];
                    System.out.println("boltName= " + boltName + ", hostName= " + hostName);
                    addCycleNumberToRecordsOfCPUBN(hostName, cycle);

                    //Set<String> listOfComponentsWithCPUBottleNeck = new HashSet<>();
                    // listOfComponentsWithCPUBottleNeck.add(parts[0]);
                    if (!boltRecords.get(boltName).getNameOfClassLoadedInside().equals("none")) {
                        BottleneckDetails bottleneckDetail = new BottleneckDetails(hostName, "CPU", getAverageCPuUsageOfAllBoltsOnANode(hostName));
                        vmsWithCPU_BN.put(boltName, bottleneckDetail);
                    }

                }


            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
            }
        });
        try {
            mqttClient.subscribe(topic);
            System.out.println("the LB has subscribed to topic = " + topic);
        } catch (MqttException e) {
            throw new RuntimeException(e);
        }

    }


    public void saveApplicationSettingsInAFile(String applicationSettingsInStringFormat) {
        String filePath = "applicationSettings_" + applicationSettings.getMood() + ".csv"; // The path where the CSV file will be created


        try {
            // Create a new File object
            File file = new File(filePath);

            // Check if the file already exists
            if (!file.exists()) {
                // Create the file if it does not exist
                file.createNewFile();
                System.out.println("File created: " + filePath);
            }
            String[] pairs = applicationSettingsInStringFormat.split(", ");

            // Write to the file using FileWriter
            try (FileWriter writerTemp = new FileWriter(file)) {
                writerTemp.write("Key,Value\n");

                // Write each key-value pair as a new row
                for (String pair : pairs) {
                    String[] keyValue = pair.split("=", 2); // Split key and value
                    String key = keyValue[0].trim();
                    String value = keyValue.length > 1 ? keyValue[1].trim() : "";
                    writerTemp.write(key + "=" + value + "\n");
                }
                System.out.println("ApplicationSetting has been written to CSV file successfully.");
            }
        } catch (IOException e) {
            System.err.println("An error occurred while creating or writing to the file.");
            e.printStackTrace();
        }

    }

    public HashMap<String, RemovalCandidate> checkIfAReplicaMustBeRemoved(List<String> listOfReplicas) {
        if (listOfReplicas == null || listOfReplicas.isEmpty()) {
            return null;
        }
        //cpu wise
        double currentCPUUsage = 0.0;
        double averageCPUOfReplicas = 0.0;
        double averageCPUOfReplicasInPreviousCycle = 0.0;
        double totalCPULimit = 0;
        double cpuLimit = 0;
        for (String s : listOfReplicas) {
            cpuLimit = boltRecords.get(s).getMetrics().getCpuLimit();
            currentCPUUsage += boltRecords.get(s).getMetrics().getCpu() * cpuLimit;
            averageCPUOfReplicasInPreviousCycle += boltRecords.get(s).getMetrics().getCpu_previous_cycle() * cpuLimit;
            totalCPULimit += cpuLimit;
        }

        averageCPUOfReplicas = currentCPUUsage / totalCPULimit;
        averageCPUOfReplicasInPreviousCycle = averageCPUOfReplicasInPreviousCycle / totalCPULimit;
        System.out.println("averageCPUOfReplicas=" + averageCPUOfReplicas);
        System.out.println("averageCPUOfReplicasInPreviousCycle=" + averageCPUOfReplicasInPreviousCycle);

        if (averageCPUOfReplicas <= applicationSettings.getMin_cpu_threshold() &&
                averageCPUOfReplicasInPreviousCycle <= applicationSettings.getMin_cpu_threshold()) {
            return null;
        }

        //bandwidth-wise
        if (listOfReplicas != null && listOfReplicas.size() > 1) {
            HashMap<String, RemovalCandidate> listOfCandidates = new HashMap<String, RemovalCandidate>();

            double remainingInBWAfterRemovingAReplica;
            double remainingOutBWAfterRemovingAReplica;
            double sumInBW;
            double sumOutBW;
            double remainingCPULimitAfterRemoval;
            double avgCPUUsageAfterRemoval;
            double maxAcceptableCPUAfterRemoval = 0.35;
            Set<String> setOfVmsAfterRemovingReplica = new HashSet<>();


            for (int i = 0; i < listOfReplicas.size(); i++) {
                String candidateName;
                setOfVmsAfterRemovingReplica.clear();
                if (listOfReplicas.get(i).startsWith("gb")) {
                    remainingInBWAfterRemovingAReplica = 0.0;
                    remainingOutBWAfterRemovingAReplica = 0.0;
                    sumInBW = 0;
                    sumOutBW = 0;
                    candidateName = listOfReplicas.get(i);
                    System.out.println("candidateName= " + candidateName);
                    for (int j = 0; j < listOfReplicas.size(); j++) {
                        if (i != j) {
                            setOfVmsAfterRemovingReplica.add(boltRecords.get(listOfReplicas.get(j)).getHostName());
                        }
                    }
                    System.out.println("setOfVmsAfterRemovingReplica= " + setOfVmsAfterRemovingReplica);
                    for (String vmName : setOfVmsAfterRemovingReplica) {
                        System.out.println("vmName= " + vmName);
                        remainingInBWAfterRemovingAReplica += tableOfRBWOfVms.get(vmName).getRemainingInBandwidth();
                        remainingOutBWAfterRemovingAReplica += tableOfRBWOfVms.get(vmName).getRemainingOutBandwidth();
                        sumInBW += bandwidthTable.get(vmName).getInBandwidth();
                        sumOutBW += bandwidthTable.get(vmName).getOutBandwidth();

                    }
                    System.out.println("remainingInBWAfterRemovingAReplica= " + remainingInBWAfterRemovingAReplica);
                    System.out.println("remainingOutBWAfterRemovingAReplica= " + remainingOutBWAfterRemovingAReplica);
                    System.out.println("sumInBW= " + sumInBW);
                    System.out.println("sumOutBW= " + sumOutBW);

                    remainingInBWAfterRemovingAReplica = remainingInBWAfterRemovingAReplica - boltRecords.get(candidateName).getMetrics().getIn_throughput();
                    remainingOutBWAfterRemovingAReplica = remainingOutBWAfterRemovingAReplica - boltRecords.get(candidateName).getMetrics().getOut_throughput();
                    System.out.println("remainingInBWAfterRemovingAReplica after subtracting the throughput= " + remainingInBWAfterRemovingAReplica);
                    System.out.println("remainingOutBWAfterRemovingAReplica after subtracting the throughput= " + remainingOutBWAfterRemovingAReplica);


                    remainingCPULimitAfterRemoval = totalCPULimit - boltRecords.get(candidateName).getMetrics().getCpuLimit();
                    avgCPUUsageAfterRemoval = currentCPUUsage / remainingCPULimitAfterRemoval;
                    System.out.println("remainingCPULimitAfterRemoval= " + remainingCPULimitAfterRemoval);
                    System.out.println("avgCPUUsageAfterRemoval= " + avgCPUUsageAfterRemoval);


                    if (remainingInBWAfterRemovingAReplica / sumInBW > applicationSettings.getMin_RBW() &&
                            remainingOutBWAfterRemovingAReplica / sumOutBW > applicationSettings.getMin_RBW() &&
                            avgCPUUsageAfterRemoval <= maxAcceptableCPUAfterRemoval
                    ) {
                        System.out.println("it wont be any bottleneck after removal since remainingInBWAfterRemovingAReplica / sumInBW =" + remainingInBWAfterRemovingAReplica / sumInBW + ", remainingOutBWAfterRemovingAReplica / sumOutBW= " + remainingOutBWAfterRemovingAReplica / sumOutBW);
                        listOfCandidates.put(candidateName, new RemovalCandidate(remainingInBWAfterRemovingAReplica, remainingOutBWAfterRemovingAReplica, boltRecords.get(candidateName).getMetrics().getCpu()));
                    }
                }
            }
            System.out.println("listOfCandidates= " + listOfCandidates.toString());
            return listOfCandidates;
        } else return null;

    }

    public String identifyAReplicaToBeDeleted(HashMap<String, RemovalCandidate> listOfCandidates) {
        String nameOfReplicaToBeDeleted = null;
        double maxCPUUsage = listOfCandidates.values().stream().mapToDouble(RemovalCandidate::getCpu).max().getAsDouble();

        double maxInBw = listOfCandidates.values().stream().mapToDouble(RemovalCandidate::getRemainingInBW).max().getAsDouble();
        double maxOutBw = listOfCandidates.values().stream().mapToDouble(RemovalCandidate::getRemainingOutBW).max().getAsDouble();

        for (RemovalCandidate candidate : listOfCandidates.values()) {

            double normalizedInBW = candidate.getRemainingInBW() / maxInBw;
            double normalizedCPUUsage = 1 - (candidate.getCpu() / maxCPUUsage);
            double normalizedOutBW = candidate.getRemainingOutBW() / maxOutBw;

            double score = (normalizedInBW * applicationSettings.getWeight_for_inBW_of_target()) + (normalizedOutBW * applicationSettings.getWeight_for_outBW_of_target()) +
                    (normalizedCPUUsage * applicationSettings.getWeight_for_cpu());

            candidate.setScore(score);
        }
        List<Map.Entry<String, RemovalCandidate>> sortedCandidates = new ArrayList<>(listOfCandidates.entrySet());
        sortedCandidates.sort((e1, e2) -> Double.compare(e2.getValue().getScore(), e1.getValue().getScore()));
        if (!sortedCandidates.isEmpty()) {
            nameOfReplicaToBeDeleted = sortedCandidates.get(0).getKey();
            //System.out.println("nameOfReplicaToBeDeleted= " + nameOfReplicaToBeDeleted);
        }
        return nameOfReplicaToBeDeleted;
    }


    public List<BoltInfo> sortAListOfBoltInfoAccordingToCPUUsageAtBoltLevel(List<BoltInfo> listOfBolts) {
        //  System.out.println("going to perform sort list of bolts with same functionality according to cpu at bolt level," +
        //         "listOfBolts=" + listOfBolts);
        Iterator<BoltInfo> iterator = listOfBolts.iterator();
        while (iterator.hasNext()) {
            BoltInfo bolt = iterator.next();
            if (bolt.getNameOfClassLoadedInside().equals("none")) {
                iterator.remove();
            }
        }
        //System.out.println("after for we have, listOfBolts= " + listOfBolts);

        if (listOfBolts.size() == 1) {
            return listOfBolts;
        } else if (listOfBolts.size() > 1) {
            listOfBolts.sort(Comparator.comparingDouble(
                    bolt -> bolt.getMetrics().getCpuAtBoltLevel()
            ));
            //System.out.println("listOfBolts sorted= " + listOfBolts);
            return listOfBolts;
        }
        return null;

    }

    public String identifyCPUIntensiveBolt(String hostName) {
        List<BoltInfo> boltsOnSameNode = getBoltsRunningOnHost(hostName);
        if (boltsOnSameNode != null) {
            float maxCPUUsagePercentageAtBoltLevel = 0.0F;
            String boltName = null;
            for (BoltInfo bolt : boltsOnSameNode) {
                if (bolt.getMetrics().getCpuAtBoltLevel() >= maxCPUUsagePercentageAtBoltLevel && !bolt.getComponentName().startsWith("s")) {
                    if (bolt.getNameOfClassLoadedInside().equals("none")) {
                        continue;
                    }
                    maxCPUUsagePercentageAtBoltLevel = bolt.getMetrics().getCpuAtBoltLevel();
                    boltName = bolt.getComponentName();
                }
            }
            return boltName;
        } else return null;
    }

    public String findBoltWithHighestInBandwidthUsage(String hostName) {
        List<BoltInfo> boltsOnSameNode = getBoltsRunningOnHost(hostName);
        if (boltsOnSameNode != null) {
            float maxInThroughPutPercentageAtBoltLevel = 0.0F;
            String boltName = null;
            for (BoltInfo bolt : boltsOnSameNode) {
                if (bolt.getNameOfClassLoadedInside().equals("none")) {
                    continue;
                }
                if (bolt.getMetrics().getIn_throughput() >= maxInThroughPutPercentageAtBoltLevel) {
                    maxInThroughPutPercentageAtBoltLevel = bolt.getMetrics().getIn_throughput();
                    boltName = bolt.getComponentName();
                }
            }
            return boltName;
        } else return null;
    }

    public String findBoltWithHighestOutBandwidthUsage(String hostName) {
        List<BoltInfo> boltsOnSameNode = getBoltsRunningOnHost(hostName);
        if (boltsOnSameNode != null) {
            float maxOutThroughPutPercentageAtBoltLevel = 0.0F;
            String boltName = null;
            for (BoltInfo bolt : boltsOnSameNode) {
                if (bolt.getNameOfClassLoadedInside().equals("none")) {
                    continue;
                }
                if (bolt.getMetrics().getOut_throughput() >= maxOutThroughPutPercentageAtBoltLevel) {
                    maxOutThroughPutPercentageAtBoltLevel = bolt.getMetrics().getOut_throughput();
                    boltName = bolt.getComponentName();
                }
            }
            return boltName;
        } else return null;
    }

    public List<BoltInfo> getBoltsRunningOnHost(String hostName) {
        List<BoltInfo> boltsOnSameNode = new ArrayList<BoltInfo>();
        for (Map.Entry<String, BoltInfo> entry : boltRecords.entrySet()) {
            if (entry.getValue().getHostName().equals(hostName) && !entry.getValue().getComponentName().startsWith("s")) {
                boltsOnSameNode.add(entry.getValue());
            }
        }
        // System.out.println("list of bolts on the specified host name" + boltsOnSameNode.toString());
        return boltsOnSameNode;
    }

    public List<String> findAllBoltNamesWithSameFunctionality(String boltName) {
        System.out.println("going to find all bolt names which loaded same class");
        String nameOfClassLoadedInside = boltRecords.get(boltName).getNameOfClassLoadedInside();
        //System.out.println("nameOfClassLoadedInside= " + nameOfClassLoadedInside);
        if (!nameOfClassLoadedInside.equals("none")) {
            List<String> listOfBoltNamesWithSameFunctionality = new ArrayList<String>();

            for (Map.Entry<String, BoltInfo> entry : boltRecords.entrySet()) {
                if (entry.getValue().getNameOfClassLoadedInside().equals(nameOfClassLoadedInside)) {
                    listOfBoltNamesWithSameFunctionality.add(entry.getKey());
                }
            }
            System.out.println("listOfBoltNamesWithSameFunctionality is= " + listOfBoltNamesWithSameFunctionality);
            return listOfBoltNamesWithSameFunctionality;
        } else return null;
    }

    public void loadEdgeDeviceBandwidthInfo() {
        bandwidthTable = new HashMap<String, BandwidthInfo>();
        tableOfRBWOfVms = new HashMap<String, RemainingBandwidthInfoOfVm>();
        try {
            Resource resource = new ClassPathResource("edgeDeviceBandwidthInfo.csv");
            InputStream inputStream = resource.getInputStream();
            Reader in = new InputStreamReader(inputStream);
            CSVParser csvParser = new CSVParser(in, CSVFormat.DEFAULT.withFirstRecordAsHeader());
            for (CSVRecord record : csvParser) {
                String vmName = record.get("vm");
                float inBandwidth = Float.parseFloat(record.get("in_bandwidth"));
                float outBandwidth = Float.parseFloat(record.get("out_bandwidth"));
                bandwidthTable.put(vmName, new BandwidthInfo(inBandwidth, outBandwidth));
                tableOfRBWOfVms.put(vmName, new RemainingBandwidthInfoOfVm(inBandwidth, outBandwidth, 100, 100));
            }
        } catch (IOException e) {
            throw new RuntimeException("Error reading CSV file", e);
        }
        System.out.println("bandwidth table = " + bandwidthTable.toString());
        System.out.println("throughput table = " + tableOfRBWOfVms.toString());
    }

    public void loadLatencyFile() {
        latencyInfoList = new ArrayList<LatencyInfo>();
        try {
            Resource resource = new ClassPathResource("latencyFile.csv");
            InputStream inputStream = resource.getInputStream();
            Reader in = new InputStreamReader(inputStream);
            CSVParser csvParser = new CSVParser(in, CSVFormat.DEFAULT.withFirstRecordAsHeader());
            for (CSVRecord record : csvParser) {
                String source = record.get("source");
                String dest = record.get("dest");
                double latency = Double.parseDouble(record.get("latency"));
                latencyInfoList.add(new LatencyInfo(source, dest, latency));
            }
        } catch (IOException e) {
            throw new RuntimeException("Error reading CSV file", e);
        }
    }


    public List<String> findUpStreamBoltsByOriginalBoltName(String originalBoltName) {
        List<String> names = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : topology.entrySet()) {
            if (entry.getValue().contains(originalBoltName)) {
                names.add(entry.getKey());
            }
        }
        System.out.println("the upStream bolt of the original bolt is= " + names.toString());
        return names;
    }

    public List<String> retrieveNameOfDSBoltsByOriginalBoltName(String neighborName) {
        return topology.get(neighborName);
    }

    public double findLatenciesBetweenBolts(String boltName1, String boltName2) {
        String hostName1 = boltRecords.get(boltName1).getHostName();
        String hostName2 = boltRecords.get(boltName2).getHostName();
        for (LatencyInfo latencyInfo : latencyInfoList) {
            if ((latencyInfo.source.equals(hostName1) && latencyInfo.dest.equals(hostName2)) || (latencyInfo.source.equals(hostName2) && latencyInfo.dest.equals(hostName1))) {
                return latencyInfo.latency;
            }
        }
        return 0.0;
    }

    public String findNameOfOriginalBoltByReplicaName(String replicaName) {
        if (replicas != null) {
            for (Map.Entry<String, List<String>> entry : replicas.entrySet()) {
                if (entry.getValue().contains(replicaName)) {
                    return entry.getKey();
                }
            }
        }
        return null;
    }


    public boolean isAverageCPUAboveThreshold(String componentName) {
        double averageCPUOfReplicas = 0.0;
        double averageCPUOfReplicasInPreviousCycle = 0.0;
        double totalCPUlimit = 0;
        double cpuLimit = 0;

        List<String> listOfBoltsWithSameFunctionality = findAllBoltNamesWithSameFunctionality(componentName);

        for (String s : listOfBoltsWithSameFunctionality) {
            System.out.println("cpuLimit= " + cpuLimit);
            cpuLimit = boltRecords.get(s).getMetrics().getCpuLimit();
            averageCPUOfReplicas += boltRecords.get(s).getMetrics().getCpu() * cpuLimit;
            System.out.println("cpuLimit*cpu= " + averageCPUOfReplicas);
            averageCPUOfReplicasInPreviousCycle += boltRecords.get(s).getMetrics().getCpu_previous_cycle() * cpuLimit;
            System.out.println("for previous cycle, cpuLimit*cpu= " + averageCPUOfReplicas);
            totalCPUlimit += cpuLimit;
        }

        System.out.println("totalCPULimit available for all replicas=" + totalCPUlimit);
        averageCPUOfReplicas = averageCPUOfReplicas / totalCPUlimit;
        averageCPUOfReplicasInPreviousCycle = averageCPUOfReplicasInPreviousCycle / totalCPUlimit;
        System.out.println("averageCPUOfReplicas = " + averageCPUOfReplicas);
        if (averageCPUOfReplicas >= applicationSettings.getThresholdForMaxAverageCpuUsageOfAllReplicas()
                && averageCPUOfReplicasInPreviousCycle >= applicationSettings.getThresholdForMaxAverageCpuUsageOfAllReplicas()
        ) {
            return true;
        } else return false;

    }

    public boolean isAverageRemainingInBandwidthBelowThreshold(String componentName) {
        List<String> listOfBoltsWithSameFunctionality = new ArrayList<String>();
        double averageRemainingInBandwidthOfAllReplicas = 0.0;
        double sumRInBW = 0.0;
        double sumInBW = 0.0;

        listOfBoltsWithSameFunctionality = findAllBoltNamesWithSameFunctionality(componentName);
        for (String s : listOfBoltsWithSameFunctionality) {
            String hostNameOfOp = findHostNameOfOperator(s);
            sumRInBW += tableOfRBWOfVms.get(hostNameOfOp).getRemainingInBandwidth();
            sumInBW += bandwidthTable.get(hostNameOfOp).getInBandwidth();
        }
        averageRemainingInBandwidthOfAllReplicas = sumRInBW / sumInBW;
        System.out.println("averageRemainingInBandwidthOfAllReplicas= " + averageRemainingInBandwidthOfAllReplicas);
        if (averageRemainingInBandwidthOfAllReplicas < applicationSettings.getReplicasMinAvgRemainingBandwidthThreshold()) {
            return true;
        } else return false;
    }

    public boolean isAverageRemainingOutBandwidthBelowThreshold(String componentName) {
        List<String> listOfBoltsWithSameFunctionality = new ArrayList<String>();
        double averageRemainingOutBandwidthOfAllReplicas = 0.0;
        double sumOfROutBW = 0.0;
        double sumOfOutBW = 0.0;
        listOfBoltsWithSameFunctionality = findAllBoltNamesWithSameFunctionality(componentName);
        for (String s : listOfBoltsWithSameFunctionality) {
            String hostNameOfOp = findHostNameOfOperator(s);
            sumOfROutBW += tableOfRBWOfVms.get(hostNameOfOp).getRemainingOutBandwidth();
            sumOfOutBW += bandwidthTable.get(hostNameOfOp).getOutBandwidth();

        }
        averageRemainingOutBandwidthOfAllReplicas = sumOfROutBW / sumOfOutBW;
        System.out.println("averageRemainingOutBandwidthOfAllReplicas= " + averageRemainingOutBandwidthOfAllReplicas);

        if (averageRemainingOutBandwidthOfAllReplicas < applicationSettings.getReplicasMinAvgRemainingBandwidthThreshold()) {
            return true;
        } else return false;
    }


    public String findOriginalBoltNameRelatedToAComponentName(String componentName) {
        if (componentName.startsWith("gb_")) {
            for (Map.Entry<String, List<String>> entry : replicas.entrySet()) {
                if (entry.getValue().contains(componentName)) {
                    return entry.getKey();
                }
            }
        }
        return componentName;
    }

    public HashMap<String, CandidateEvaluationMetrics> identifyCandidatesForSpanningAReplica(String componentNameOfBoltToBeReplicated) {
        System.out.println("Going to identify candidate for spanning a new replica");
        //System.out.println("the bolt records are=" + boltRecords.toString());
        //by considering only cpu and latency and remain_in_bandwidth_of_vm
        // having concurrent bolts on a same device is ok
        String originalBoltName = findOriginalBoltNameRelatedToAComponentName(componentNameOfBoltToBeReplicated);
        System.out.println("the original bolt name =" + originalBoltName);
        List<String> upStreamBolts = findUpStreamBoltsByOriginalBoltName(originalBoltName);
        // System.out.println("the upStreamBolts =" + upStreamBolts.toString());
        HashMap<String, CandidateEvaluationMetrics> listOfAllCandidates = new HashMap<String, CandidateEvaluationMetrics>();
        //the key is the candidate bolt name
        List<String> listOfBoltsWithSameFunctionality = findAllBoltNamesWithSameFunctionality(componentNameOfBoltToBeReplicated);
        Set<String> setOfNodesRunSameFunctionality = new HashSet<>();
        for (String bolt : listOfBoltsWithSameFunctionality) {
            setOfNodesRunSameFunctionality.add(boltRecords.get(bolt).getHostName());
        }

        for (Map.Entry<String, BoltInfo> entry : boltRecords.entrySet()) {
            //     System.out.println("entry.getValue().getComponentName()=" + entry.getValue().getComponentName());
            //     System.out.println("entry.getValue().getNameOfClassLoadedInside()=" + entry.getValue().getNameOfClassLoadedInside());
            //   System.out.println("entry.getValue().getMetrics().getCpu()=" + entry.getValue().getMetrics().getCpu());
            //    System.out.println("entry.getValue().getMetrics().getRemaining_in_bandwidth_of_vm=" + entry.getValue().getMetrics().getRemaining_in_bandwidth_of_vm());

            if (entry.getValue().getNameOfClassLoadedInside().equals("none") && setOfNodesRunSameFunctionality.stream().noneMatch(entry.getValue().getHostName()::equals) && entry.getValue().getMetrics().getCpu() < applicationSettings.getMax_cpu_threshold() && entry.getValue().getMetrics().getRemaining_in_bandwidth_of_vm() > applicationSettings.getMin_remaining_bandwidth_threshold()) {

                String candidateName = entry.getValue().getComponentName();
                //System.out.println("candidate name=" + candidateName);
                double averageLatencyToAllDownStreamBolts = 0.0;
                double averageLatencyToAllUpStreamBolts = 0.0;

                for (String upStreamBoltName : upStreamBolts) {
                    double latency = findLatenciesBetweenBolts(upStreamBoltName, candidateName);
                    averageLatencyToAllUpStreamBolts += latency;
                }
                averageLatencyToAllUpStreamBolts = averageLatencyToAllUpStreamBolts / upStreamBolts.size();
                //       System.out.println("averageLatencyToAllUpStreamBolts is " + averageLatencyToAllUpStreamBolts);

                List<String> listOfDownStreamBolts = retrieveNameOfDSBoltsByOriginalBoltName(originalBoltName);
                //        System.out.println("listOfDownStreamBolts is " + listOfDownStreamBolts.toString());
                for (String downstreamBoltName : listOfDownStreamBolts) {
                    double latency = findLatenciesBetweenBolts(downstreamBoltName, candidateName);
                    averageLatencyToAllDownStreamBolts += latency;
                }
                averageLatencyToAllDownStreamBolts = averageLatencyToAllDownStreamBolts / listOfDownStreamBolts.size();
                //         System.out.println("averageLatencyToAllDownStreamBolts is " + averageLatencyToAllDownStreamBolts);

                CandidateEvaluationMetrics candidateEvaluationMetrics = new CandidateEvaluationMetrics();
                candidateEvaluationMetrics.setAverageConnectionLatencyToAllUpStreamBolts(averageLatencyToAllUpStreamBolts);
                candidateEvaluationMetrics.setAverageConnectionLatencyToAllDownStreamBolts(averageLatencyToAllDownStreamBolts);
                candidateEvaluationMetrics.setCpuUsage(entry.getValue().getMetrics().getCpu());
                candidateEvaluationMetrics.setRemainingInBandwidth(entry.getValue().getMetrics().getRemaining_in_bandwidth_of_vm());
                candidateEvaluationMetrics.setRemainingOutBandwidth(entry.getValue().getMetrics().getRemaining_out_bandwidth_of_vm());
                listOfAllCandidates.put(candidateName, candidateEvaluationMetrics);
            }
        }
        //  System.out.println("listOfAllCandidates is: " + listOfAllCandidates.toString());
        return listOfAllCandidates;
    }

    public String calculateScoreForAllCandidatesAndSelectTheBestCandidate(HashMap<String, CandidateEvaluationMetrics> listOfAllCandidates) {


        // 1. Check if list is null or empty
        if (listOfAllCandidates == null || listOfAllCandidates.isEmpty()) {
            System.out.println("Candidate list is empty or null.");
            return null;
        }

        // 2. Identify min and max values for each metric
        double cpuMin = Double.POSITIVE_INFINITY, cpuMax = Double.NEGATIVE_INFINITY;
        double latencyMin = Double.POSITIVE_INFINITY, latencyMax = Double.NEGATIVE_INFINITY;
        double inBWMin = Double.POSITIVE_INFINITY, inBWMax = Double.NEGATIVE_INFINITY;
        double outBWMin = Double.POSITIVE_INFINITY, outBWMax = Double.NEGATIVE_INFINITY;

        for (CandidateEvaluationMetrics candidate : listOfAllCandidates.values()) {
            double cpuUsage = candidate.getCpuUsage();
            double latencySum = candidate.getAverageConnectionLatencyToAllDownStreamBolts()
                    + candidate.getAverageConnectionLatencyToAllUpStreamBolts();
            double inBW = candidate.getRemainingInBandwidth();
            double outBW = candidate.getRemainingOutBandwidth();

            // Update CPU min/max
            cpuMin = Math.min(cpuMin, cpuUsage);
            cpuMax = Math.max(cpuMax, cpuUsage);

            // Update Latency min/max
            latencyMin = Math.min(latencyMin, latencySum);
            latencyMax = Math.max(latencyMax, latencySum);

            // Update InBW min/max
            inBWMin = Math.min(inBWMin, inBW);
            inBWMax = Math.max(inBWMax, inBW);

            // Update OutBW min/max
            outBWMin = Math.min(outBWMin, outBW);
            outBWMax = Math.max(outBWMax, outBW);
        }

        // 3. Prepare half-weights for inbound and outbound bandwidth
        //    Suppose getWeight_for_in_bandwidth_of_target() was our original total BW weight.
        double inBwWeight = applicationSettings.getWeight_for_inBW_of_target();   // half for inbound
        double outBwWeight = applicationSettings.getWeight_for_outBW_of_target();  // half for outbound

        // 4. Normalize each candidate’s metrics + compute SAW score
        for (CandidateEvaluationMetrics candidate : listOfAllCandidates.values()) {
            double cpuUsage = candidate.getCpuUsage();
            double latencySum = candidate.getAverageConnectionLatencyToAllDownStreamBolts()
                    + candidate.getAverageConnectionLatencyToAllUpStreamBolts();
            double inBW = candidate.getRemainingInBandwidth();
            double outBW = candidate.getRemainingOutBandwidth();

            // ---- CPU (cost) ----
            // (cpuMax - cpu) / (cpuMax - cpuMin)
            double normalizedCPU;
            if (cpuMax == cpuMin) {
                // all candidates have identical CPU usage
                normalizedCPU = 1.0;
            } else {
                normalizedCPU = (cpuMax - cpuUsage) / (cpuMax - cpuMin);
            }

            // ---- Latency (cost) ----
            // (latencyMax - latency) / (latencyMax - latencyMin)
            double normalizedLatency;
            if (latencyMax == latencyMin) {
                // all candidates have identical latency
                normalizedLatency = 1.0;
            } else {
                normalizedLatency = (latencyMax - latencySum) / (latencyMax - latencyMin);
            }

            // ---- Inbound Bandwidth (beneficial) ----
            // (inBW - inBWMin) / (inBWMax - inBWMin)
            double normalizedInBW;
            if (inBWMax == inBWMin) {
                // all candidates have identical inBW
                normalizedInBW = 1.0;
            } else {
                normalizedInBW = (inBW - inBWMin) / (inBWMax - inBWMin);
            }

            // ---- Outbound Bandwidth (beneficial) ----
            // (outBW - outBWMin) / (outBWMax - outBWMin)
            double normalizedOutBW;
            if (outBWMax == outBWMin) {
                // all candidates have identical outBW
                normalizedOutBW = 1.0;
            } else {
                normalizedOutBW = (outBW - outBWMin) / (outBWMax - outBWMin);
            }

            // 5. Weighted SAW score
            //    We still have:
            //    - weight_for_cpu() for CPU
            //    - weight_for_latency() for latency
            //    - inBwWeight for inbound BW
            //    - outBwWeight for outbound BW
            double score = (normalizedCPU * applicationSettings.getWeight_for_cpu())
                    + (normalizedLatency * applicationSettings.getWeight_for_latency())
                    + (normalizedInBW * inBwWeight)
                    + (normalizedOutBW * outBwWeight);

            candidate.setScore(score);
        }

        // 6. Find the candidate with the highest score
        CandidateEvaluationMetrics chosenCandidate = null;
        double highestScore = Double.NEGATIVE_INFINITY;
        String chosenKey = null;

        for (Map.Entry<String, CandidateEvaluationMetrics> entry : listOfAllCandidates.entrySet()) {
            double candidateScore = entry.getValue().getScore();
            if (candidateScore > highestScore) {
                highestScore = candidateScore;
                chosenCandidate = entry.getValue();
                chosenKey = entry.getKey(); // store the key
            }
        }

        // 7. Log and return the chosen candidate’s key
        if (chosenCandidate != null) {
            // System.out.println("Chosen candidate: " + chosenCandidate + " with score: " + highestScore);
            return chosenKey;
        } else {
            System.out.println("No candidate found.");
            return null;
        }


       /* if (listOfAllCandidates == null) {
            System.out.println("list of candidates are empty");
            return null;
        }
        //going to normalize stuff
        double maxLatencySum = listOfAllCandidates.values().stream().mapToDouble(c -> c.averageConnectionLatencyToAllDownStreamBolts + c.averageConnectionLatencyToAllUpStreamBolts).max().getAsDouble();
        //    System.out.println("maxLatencySum is " + maxLatencySum);

        double maxCPUUsage = listOfAllCandidates.values().stream().mapToDouble(c -> c.cpuUsage).max().getAsDouble();
        //     System.out.println("maxCPUUsage is " + maxCPUUsage);
        double maxRemainingBandwidth = listOfAllCandidates.values().stream().mapToDouble(c -> c.getRemainingInBandwidth()).max().getAsDouble();

        //  float maxRemainingBandwidthAsFloat = (float) maxRemainingBandwidth;
        for (CandidateEvaluationMetrics candidate : listOfAllCandidates.values()) {
            double latencySum = candidate.getAverageConnectionLatencyToAllDownStreamBolts() + candidate.getAverageConnectionLatencyToAllUpStreamBolts();

            double normalizedLatencySum = (maxLatencySum + 1) - latencySum;
            double normalizedCPUUsage = (maxCPUUsage + 1) - candidate.getCpuUsage();
            double normalizedRemainingBandwidth = candidate.getRemainingInBandwidth() / (maxRemainingBandwidth + 1);

            candidate.setScore((normalizedLatencySum * applicationSettings.getWeight_for_latency()) + (normalizedRemainingBandwidth * applicationSettings.getWeight_for_in_bandwidth_of_target()) + normalizedCPUUsage * applicationSettings.getWeight_for_cpu());
        }

        CandidateEvaluationMetrics chosenCandidate = null;
        double highestScore = Double.NEGATIVE_INFINITY;

        for (CandidateEvaluationMetrics potential_candidate : listOfAllCandidates.values()) {
            if (potential_candidate.score > highestScore) {
                chosenCandidate = potential_candidate;
                highestScore = potential_candidate.score;
            }
        }
        if (chosenCandidate != null) {
            System.out.println("The chosen candidate is: " + chosenCandidate + " with score: " + highestScore);
        } else {
            System.out.println("No candidates found.");
        }
        return getKeyFromValue(listOfAllCandidates, chosenCandidate);

        */
    }

    public <K, V> K getKeyFromValue(HashMap<K, V> map, V value) {
        for (Map.Entry<K, V> entry : map.entrySet()) {
            if (entry.getValue().equals(value)) {
                return entry.getKey();
            }
        }
        return null; // Return null if the value is not found
    }


    public void removeAReplica(RequestToRemoveReplica request) {
        int cycleCount = cycle;
        String nameOfReplica = request.getBoltNameToDelete();

        String originalBoltName = request.getOriginalBoltName();
        List<String> upStreamBolts = findUpStreamBoltsByOriginalBoltName(originalBoltName);
        if (boltRecords.get(nameOfReplica) == null ||
                "none".equals(boltRecords.get(nameOfReplica).getNameOfClassLoadedInside())) {
            System.out.println("Bolt " + nameOfReplica + " no longer exists, skipping");
            return;
        }
        for (String nameOfUpStreamBolt : upStreamBolts) {
            publishAMessage(nameOfUpStreamBolt + "/delete", nameOfReplica);
            topology.get(nameOfUpStreamBolt).remove(nameOfReplica);
        }

        System.out.println("list of replicas is = " + replicas.toString());
        publishAMessage(nameOfReplica + "/stop", "");
        topology.remove(nameOfReplica);
        while (!boltRecords.get(nameOfReplica).getNameOfClassLoadedInside().equals("none")) {
        }
        boltRecords.get(nameOfReplica).setNameOfClassLoadedInside("none");
        System.out.println("now bolt record of the removed replica is = " + boltRecords.get(nameOfReplica));


        if (replicas.containsKey(originalBoltName)) {
            replicas.get(originalBoltName).remove(nameOfReplica);
        }
        if (replicas.get(originalBoltName).size() == 0) {
            replicas.remove(originalBoltName);
        }
        System.out.println(" the topology at the end is= " + topology.toString());
        insertTopologyUpdateInBWFile(remainingBWCsvFile, cycle, " × ", nameOfReplica, originalBoltName, " ");
        insertTopologyUpdateInCPUFile(cpuUsageCsvFile, cycleCount, " × ", nameOfReplica, originalBoltName, " ");
        insertTopologyUpdateInExecutionTimeFile(latencyCsvFile, cycleCount, " × ", nameOfReplica, originalBoltName, " ");
    }

    public void addANewReplica(RequestToAddAReplica request, String cause) {
        int cycleCount = cycle;
        String nameOfReplica = null;
        String originalBoltName = request.getOriginalBoltName();

        if (request.getNewReplica() != null) {
            nameOfReplica = request.getNewReplica();

            List<BoltInfo> temp = informDSBltsOfNewReplica(originalBoltName, nameOfReplica);

            //   String messageToSend;
            //    messageToSend = request.getNameOfClassLoadedInside() + "/" + temp.toString();
            try {

                publishAMessage(nameOfReplica + "/" + "classToLoad", request.getNameOfClassLoadedInside() + "/" + objectMapper.writeValueAsString(temp));
                topology.put(nameOfReplica, topology.get(originalBoltName));
                System.out.println("topology is: " + topology.toString());
            } catch (JsonProcessingException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }

            if (replicas.containsKey(originalBoltName)) {
                replicas.get(originalBoltName).add(nameOfReplica);
            } else {
                ArrayList listOfReplicas = new ArrayList<String>();
                listOfReplicas.add(nameOfReplica);
                replicas.put(originalBoltName, listOfReplicas);
            }
            System.out.println("replicas is " + replicas.toString());

            //we also need to find upstream bolts of original bolt and let them know of new replica
            // so that they can add it to the list of DS
            synchronized (boltRecords.get(nameOfReplica)) {
                while (!boltRecords.get(nameOfReplica).nameOfClassLoadedInside.equals(request.getNameOfClassLoadedInside())) {

                    try {
                        boltRecords.get(nameOfReplica).wait();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            List<String> upStreamBolts = findUpStreamBoltsByOriginalBoltName(originalBoltName);
            listOfFreeServerPorts.put(nameOfReplica, new LinkedBlockingQueue<>());

            for (String nameOfUpStreamBolt : upStreamBolts) {
                publishAMessage(nameOfReplica + "/newUpstreamGb", nameOfUpStreamBolt + "/" + boltRecords.get(nameOfUpStreamBolt).getHostName());
              /*  synchronized (freePortsLock) {
                    while (!(listOfFreeServerPorts.containsKey(nameOfReplica)) || listOfFreeServerPorts.get(nameOfReplica).size() == 0) {
                        try {
                            freePortsLock.wait();  // Wait until the condition changes
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                            throw new RuntimeException(e);
                        }
                    }

               */
                BlockingQueue<Integer> queue = listOfFreeServerPorts.get(nameOfReplica);
                try {
                    Integer port = queue.poll(10, TimeUnit.SECONDS);

                    if (port == null) {
                        throw new RuntimeException("Timeout waiting for port from: " + nameOfReplica);
                    }
                    BoltInfo b = boltRecords.get(nameOfReplica);
                    b.setServerPort(port);

                    // BoltInfo b = boltRecords.get(nameOfReplica);
                    //    b.setServerPort(listOfFreeServerPorts.get(nameOfReplica).get(0));

                    publishAMessage(nameOfUpStreamBolt + "/add", objectMapper.writeValueAsString(b));
                    List<String> modifiableList = new ArrayList<>(topology.get(nameOfUpStreamBolt));
                    modifiableList.add(nameOfReplica);
                    topology.put(nameOfUpStreamBolt, modifiableList);
                    System.out.println("topology is: " + topology.toString());

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }

                //listOfFreeServerPorts.get(nameOfReplica).remove(0);

            }
        }
        listOfFreeServerPorts.remove(nameOfReplica);

        insertTopologyUpdateInBWFile(remainingBWCsvFile, cycle, "->", nameOfReplica, originalBoltName, cause);
        insertTopologyUpdateInCPUFile(cpuUsageCsvFile, cycleCount, "->", nameOfReplica, originalBoltName, cause);
        insertTopologyUpdateInExecutionTimeFile(latencyCsvFile, cycleCount, "->", nameOfReplica, originalBoltName, cause);
    }


    public void insertTopologyUpdateInBWFile(File file, int cycleNumber, String topologyUpdateText, String nameOfGenericBolt, String originalBoltName, String cause) {
        try {
            FileWriter writer_local = new FileWriter(file, true);

            writer_local.append("") //vmname
                    .append(",").append("")  //in BW
                    .append(",").append("")   //out BW
                    .append(",").append("")   //in BW %
                    .append(",").append("")    //out BW %
                    .append(",").append(String.valueOf(cycleNumber))    //cycle
                    .append(",").append("")    //emitRate
                    .append(",").append(String.format(nameOfGenericBolt + topologyUpdateText + originalBoltName + cause)).append("\n");
            writer_local.flush();
            writer_local.close();
            System.out.println("Event now is recorded to bandwidth file");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void insertTopologyUpdateInCPUFile(File file, int cycleCount, String topologyUpdateText, String nameOfGenericBolt, String originalBoltName, String cause) {
        threadPool.submit(() -> {
            //File csvFile = new File("cpuUsage.csv");

            try (FileWriter writerCPU_local = new FileWriter(file, true)) { // Append mode enabled


                writerCPU_local.append("") //vmName
                        .append(",").append("") //cpu Usage
                        .append(",").append(String.valueOf(cycleCount)) //cycle count
                        .append(",").append("")  //emit rate
                        .append(",").append(String.format(nameOfGenericBolt + topologyUpdateText + originalBoltName + cause)).append("\n");


                writerCPU_local.flush();
                System.out.println("Event now is recorded to CPU file");
                writerCPU_local.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    public void insertTopologyUpdateInExecutionTimeFile(File file, int cycleCount, String topologyUpdateText, String nameOfGenericBolt, String originalBoltName, String cause) {
        threadPool.submit(() -> {
            //File csvFile = new File("cpuUsage.csv");

            try (FileWriter writerExecutionTime_local = new FileWriter(file, true)) { // Append mode enabled


                writerExecutionTime_local.append("") //messageId
                        .append(",").append("") //start time
                        .append(",").append("") //execution time
                        .append(",").append(String.valueOf(cycleCount))  //cycle
                        .append(",").append("")  //emit rate
                        .append(",").append(String.format(nameOfGenericBolt + topologyUpdateText + originalBoltName + cause))
                        .append("\n");


                writerExecutionTime_local.flush();
                System.out.println("Event now is recorded to execution time file");
                writerExecutionTime_local.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    public void calculateRemainingBandwidthOfVms(int initialDelay, int period) {
        rBWExecutor = Executors.newSingleThreadScheduledExecutor();
        Runnable task = new Runnable() {

            float vmOutBW;
            float vmInBW;

            @Override
            public void run() {
                BottleneckDetails specificInBWBN = null;
                BottleneckDetails specificOutBWBN = null;
                System.out.println("going to calculate remaining bandwidth of Vms");

                if (boltRecords.size() == 0 || applicationSettings == null || boltRecords.size() < numberOfOperatorsInTopology || writerForBWCsvFile == null) {
                    System.out.println("boltRecords.size()=" + boltRecords.size());
                    System.out.println("numberOfOperatorsInTopology=" + numberOfOperatorsInTopology);
                    System.out.println("Condition not met, skipping this run.");
                    return;
                } else {
                    listOfComponentsWithBWBottleNeck.clear();
                    for (String vmName : tableOfRBWOfVms.keySet()) {
                        // System.out.println("BIBBBBB, in tableOfRemainingBandwidthOfVms, vm name ="+vmName);
                        float in_th = 0;
                        float out_th = 0;
                        float currentRInBW = 0;
                        float currentROutBW = 0;
                        for (BoltInfo infoOfOperator : boltRecords.values()) {
                            if (infoOfOperator.getHostName().equals(vmName)) {
                                in_th += infoOfOperator.getMetrics().getIn_throughput();
                                out_th += infoOfOperator.getMetrics().getOut_throughput();
                            }
                        }

                        vmOutBW = bandwidthTable.get(vmName).getOutBandwidth();
                        vmInBW = bandwidthTable.get(vmName).getInBandwidth();
                        //should be performed after all vms got their bandwidth updated

                        currentROutBW = vmOutBW - out_th;
                        currentRInBW = vmInBW - in_th;

                        if (currentROutBW < 0) {
                            currentROutBW = 0;
                        }
                        if (currentRInBW < 0) {
                            currentRInBW = 0;
                        }
                        tableOfRBWOfVms.get(vmName).setRemainingOutBandwidth(currentROutBW);
                        tableOfRBWOfVms.get(vmName).setRemainingInBandwidth(currentRInBW);
                        tableOfRBWOfVms.get(vmName).setRemainingInBandwidthPercentage((Math.round((currentRInBW / vmInBW) * metricPrecision) / (float) metricPrecision));
                        tableOfRBWOfVms.get(vmName).setRemainingOutBandwidthPercentage((Math.round((currentROutBW / vmOutBW) * metricPrecision) / (float) metricPrecision));


                        if (cycle > 3 && 1 - tableOfRBWOfVms.get(vmName).getRemainingInBandwidthPercentage() >= applicationSettings.getMax_throughput_threshold()) {
                            System.out.println(vmName + " is using inBW more than a threshold, RInBWPercentage is=" + tableOfRBWOfVms.get(vmName).getRemainingInBandwidthPercentage());

                            List<BoltInfo> listOfBolts = getBoltsRunningOnHost(vmName);
                            // System.out.println("listOfBolts on the " + vmName + " is " + listOfBolts);
                            for (BoltInfo boltInfo : listOfBolts) {
                                listOfComponentsWithBWBottleNeck.add(boltInfo.getComponentName());
                            }
                            specificInBWBN = new BottleneckDetails(vmName, "inBW", 1 - tableOfRBWOfVms.get(vmName).getRemainingInBandwidthPercentage());
                            listOfVmsWithBottleNeck.put(vmName, specificInBWBN);
                        }
                        if (cycle > 3 && 1 - tableOfRBWOfVms.get(vmName).getRemainingOutBandwidthPercentage() >= applicationSettings.getMax_throughput_threshold()) {
                            System.out.println(vmName + " is using outBW more than a threshold, ROutBWPercentage=" + tableOfRBWOfVms.get(vmName).getRemainingOutBandwidthPercentage());
                            List<BoltInfo> listOfBolts2 = getBoltsRunningOnHost(vmName);
                            //  System.out.println("listOfBolts on the " + vmName + " is " + listOfBolts2);
                            for (BoltInfo boltInfo : listOfBolts2) {
                                listOfComponentsWithBWBottleNeck.add(boltInfo.getComponentName());
                            }
                            specificOutBWBN = new BottleneckDetails(vmName, "outBW", 1 - tableOfRBWOfVms.get(vmName).getRemainingOutBandwidthPercentage());
                            listOfVmsWithBottleNeck.put(vmName, specificOutBWBN);

                        }

                    }

                    for (Map.Entry<String, RemainingBandwidthInfoOfVm> a : tableOfRBWOfVms.entrySet()) {
                        publishAMessage(a.getKey() + "/RemainingBWOfVM/" + cycle, String.valueOf(a.getValue().getRemainingInBandwidth()) + "/" + String.valueOf(a.getValue().getRemainingInBandwidthPercentage()) + "/" +
                                String.valueOf(a.getValue().getRemainingOutBandwidth()) + "/" + String.valueOf(a.getValue().getRemainingOutBandwidthPercentage()));
                        List<BoltInfo> listOfBolts = getBoltsRunningOnHost(a.getKey());
                        for (BoltInfo boltInfo : listOfBolts) {
                            publishAMessage(boltInfo.getComponentName() + "/RemainingBWOfVM/" + cycle, String.valueOf(a.getValue().getRemainingInBandwidth()) + "/" + String.valueOf(a.getValue().getRemainingInBandwidthPercentage()) + "/" +
                                    String.valueOf(a.getValue().getRemainingOutBandwidth()) + "/" + String.valueOf(a.getValue().getRemainingOutBandwidthPercentage()));
                        }
                    }


                    // System.out.println("BIBBBBB, tableOfRemainingBandwidthOfVms: " + tableOfRemainingBandwidthOfVms.toString());

                    //going to update bolt records
                    for (BoltInfo infoOfOperator : boltRecords.values()) {
                        String hostName = infoOfOperator.getHostName();
                        infoOfOperator.getMetrics().setRemaining_in_bandwidth_of_vm(tableOfRBWOfVms.get(hostName).getRemainingInBandwidth());
                        infoOfOperator.getMetrics().setRemaining_out_bandwidth_of_vm(tableOfRBWOfVms.get(hostName).getRemainingOutBandwidth());

                        infoOfOperator.getMetrics().setRemaining_in_bandwidth_of_vm_in_percentage(tableOfRBWOfVms.get(hostName).getRemainingInBandwidthPercentage());
                        infoOfOperator.getMetrics().setRemaining_out_bandwidth_of_vm_in_percentage(tableOfRBWOfVms.get(hostName).getRemainingOutBandwidthPercentage());

                    }

                    //manage BNs in terms of CPU
                    if (cycle > 3 && !vmsWithCPU_BN.isEmpty()) {
                        try {
                            System.out.println("cycle>3 and going to manage CPU congested bolts");
                            System.out.println("vmsWithCPU_BN=" + vmsWithCPU_BN.toString());


                            Iterator<Map.Entry<String, BottleneckDetails>> iterator = vmsWithCPU_BN.entrySet().iterator();
                            while (iterator.hasNext()) {
                                Map.Entry<String, BottleneckDetails> entry = iterator.next();
                                System.out.println("before problematic part that used to throw NP, entry: " + entry.getKey());
                                int replicaCount = findAllBoltNamesWithSameFunctionality(entry.getKey()).size();
                                System.out.println("replicaCount=" + replicaCount);
                                if (cycle % globalAdaptationInterval == 0) {

                                    listOfVmsWithBottleNeck.put(entry.getValue().getVmName(), entry.getValue());

                                } else {
                                    if (replicaCount == 1) {
                                        System.out.println("going to call global Adaptation since relicaCount=1 for CPU BN ");
                                        //executorForGlobalAdaptation.submit(() -> globalAdaptation(entry.getValue()));
                                        globalAdaptation(entry.getValue());
                                    } else {
                                        listOfVmsWithBottleNeck.put(entry.getValue().getVmName(), entry.getValue());
                                    }
                                }

                                // iterator.remove(); // Safe removal during iteration
                            }
                            vmsWithCPU_BN.clear();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }


                    //manage BNs in terms of BW
                    List<String> allOpswithSameFunc;
                    HashMap<String, Integer> replicaCountForEachComponent = new HashMap<>();
                    for (String s : listOfComponentsWithBWBottleNeck) {
                        if (s.startsWith("gb") && boltRecords.get(s).getNameOfClassLoadedInside().equals("none")) {
                            continue;
                        } else {
                            allOpswithSameFunc = findAllBoltNamesWithSameFunctionality(s);
                            replicaCountForEachComponent.put(s, allOpswithSameFunc.size());
                        }
                    }
                    if (cycle > 3 && !listOfComponentsWithBWBottleNeck.isEmpty() && cycle % globalAdaptationInterval != 0) {

                        System.out.println("inside method cycle > 3 && !listOfComponentsWithBWBottleNeck.isEmpty()");

                        for (Map.Entry<String, Integer> replicaCount : replicaCountForEachComponent.entrySet()) {


                            if (replicaCount.getValue() == 1) {
                                System.out.println("now isGlobalAdaptationNeeded is true since replicaCountForEachComponent=" + replicaCountForEachComponent.toString());

                                String vmName = boltRecords.get(replicaCount.getKey()).getHostName();
                                float rInBW = tableOfRBWOfVms.get(vmName).getRemainingInBandwidthPercentage();
                                float rOutBW = tableOfRBWOfVms.get(vmName).getRemainingOutBandwidthPercentage();
                                BottleneckDetails bNDetail = new BottleneckDetails();

                                if (1 - rOutBW >= applicationSettings.getMax_throughput_threshold()) {
                                    bNDetail.setVmName(vmName);
                                    bNDetail.setUsagePercentage(1 - rOutBW);
                                    bNDetail.setCause("outBW");
                                    globalAdaptation(bNDetail);
                                    // executorForGlobalAdaptation.submit(() -> globalAdaptation(bNDetail));
                                } else if (1 - rInBW >= applicationSettings.getMax_throughput_threshold()) {
                                    bNDetail.setVmName(vmName);
                                    bNDetail.setUsagePercentage(1 - rInBW);
                                    bNDetail.setCause("inBW");
                                    globalAdaptation(bNDetail);
                                    //executorForGlobalAdaptation.submit(() -> globalAdaptation(bNDetail));
                                }
                            }

                        }
                    }
                    if (cycle > 3 && cycle % globalAdaptationInterval == 0) {

                        System.out.println("because cycle % interval == 0, we are going to call global Adaptation");
                        executorForGlobalAdaptation.submit(() -> globalAdaptation());
                    }
                    listOfComponentsWithBWBottleNeck.clear();

                    System.out.println("after submitting a thread for manageUnderUtilizationSignal");


                    if (cycle > 0 && emitRate == 0) {
                        for (String opName : boltRecords.keySet()) {
                            if (opName.startsWith("gb_")) {
                                try {
                                    removeAReplica(new RequestToRemoveReplica(findNameOfOriginalBoltByReplicaName(opName), opName));
                                } catch (Exception e) {
                                    System.err.println("Failed to remove replica: " + opName);
                                    e.printStackTrace();
                                }
                            }
                        }
                    } else if (cycle > 0 && emitRate != 0) {
                        executorForUnderUtilization.submit(() -> manageUnderUtilizationSignal());
                    }
                    //going to publish MQTT message
                    storeVMBandwidthInfoInCSVFile(cycle);
                }
            }
        };

        rBWExecutor.scheduleAtFixedRate(task, initialDelay, period, TimeUnit.SECONDS);

    }

    public void globalAdaptation() {
        System.out.println("Going to do global Adaptation");
        List<Map.Entry<String, BottleneckDetails>> sorted = sortByPriority(listOfVmsWithBottleNeck);

        // Debug: Print the sorted VMs
        System.out.println("going to print all vms with bottleneck sorted according to importance");
        for (Map.Entry<String, BottleneckDetails> entry : sorted) {
            System.out.println("VM: " + entry.getKey()
                    + ", Cause: " + entry.getValue().getCause()
                    + ", Usage: " + entry.getValue().getUsagePercentage());
        }

        boolean handled = false;

        // Traverse from highest priority (index 0) to lower
        for (Map.Entry<String, BottleneckDetails> entry : sorted) {
            BottleneckDetails details = entry.getValue();
            String cause = details.getCause();
            String vmName = entry.getKey();

            // Debug: Show which VM/cause you are trying
            System.out.println("Trying to solve bottleNeck in VM: " + vmName
                    + " with cause: " + cause
                    + " usage: " + details.getUsagePercentage());

            boolean success = false;

            // Pick the correct replica-creation method
            if ("CPU".equals(cause)) {
                success = manageReplicaCreationBasedOnCPU(vmName);
            } else if ("inBW".equals(cause)) {
                success = manageReplicaCreationBasedOnRemainingInBandwidth(vmName);
            } else if ("outBW".equals(cause)) {
                success = manageReplicaCreationBasedOnRemainingOutBandwidth(vmName);
            }

            // If one of them succeeds, mark handled=true and break
            if (success) {
                System.out.println("In this round we solved: VM=" + vmName + " cause=" + cause);
                handled = true;
                break;
            } else {
                System.out.println("trying next bottleNeck with lower priority...");
            }
        }

        if (!handled) {
            System.out.println("Global adaptation: Nothing happened in this round");
        }

        // Cleanup
        sorted.clear();
        synchronized (this) {
            listOfVmsWithBottleNeck.clear();
        }
    }

    public void globalAdaptation(BottleneckDetails specificBN) {
        System.out.println("Going to do global Adaptation for specific bottleNeck");


        boolean handled = false;

        // Traverse from highest priority (index 0) to lower

        String cause = specificBN.getCause();
        String vmName = specificBN.getVmName();

        // Debug: Show which VM/cause you are trying
        System.out.println("Trying to solve bottleNeck in VM: " + vmName
                + " with cause: " + cause
                + " usage: " + specificBN.getUsagePercentage());

        boolean success = false;

        // Pick the correct replica-creation method
        if ("CPU".equals(cause)) {
            success = manageReplicaCreationBasedOnCPU(vmName);
        } else if ("inBW".equals(cause)) {
            success = manageReplicaCreationBasedOnRemainingInBandwidth(vmName);
        } else if ("outBW".equals(cause)) {
            success = manageReplicaCreationBasedOnRemainingOutBandwidth(vmName);
        }

        // If one of them succeeds, mark handled=true and break
        if (success) {
            System.out.println("In this round we solved: VM=" + vmName + " cause=" + cause);
            handled = true;
        }

        if (!handled) {
            System.out.println("Global adaptation for specific BN did not create any replica");
        }

    }

    public double getAverageCPuUsageOfAllBoltsOnANode(String hostName) {
        double averageCpuUsage = 0.0;
        List<BoltInfo> listOfBoltsOnTheHost = getBoltsRunningOnHost(hostName);
        for (BoltInfo boltInfo : listOfBoltsOnTheHost) {
            averageCpuUsage += boltInfo.getMetrics().getCpu();
        }
        averageCpuUsage = averageCpuUsage / listOfBoltsOnTheHost.size();
        return averageCpuUsage;
    }


    public String findHostNameOfOperator(String operatorName) {
        BoltInfo boltInfo = boltRecords.get(operatorName);
        return boltInfo.getHostName();
    }

    public void sendOpsTheirUpstreams() {
        for (String boltName : boltRecords.keySet()) {
            System.out.println("opName=" + boltName);
            HashMap<String, String> listOfUpStreamBoltsWithHostName = new HashMap<>();
            List<String> upStreamOps = findUpStreamBoltsByOriginalBoltName(boltName);
            for (String s : upStreamOps) {
                listOfUpStreamBoltsWithHostName.put(s, boltRecords.get(s).getHostName());
            }
            if (listOfUpStreamBoltsWithHostName.size() > 0) {
                try {
                    publishAMessage(boltName + "/upStreamOps", objectMapper.writeValueAsString(listOfUpStreamBoltsWithHostName));
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}




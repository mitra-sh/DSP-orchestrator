package com.test.LoadBalancerDemo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.test.LoadBalancerDemo.boltDetails.*;
import com.test.LoadBalancerDemo.metrics.BoltLatencyDetails;
import com.test.LoadBalancerDemo.configs.ApplicationSettings;
import com.test.LoadBalancerDemo.metrics.CandidateEvaluationMetrics;
import com.test.LoadBalancerDemo.requests.RequestToAddAReplica;
import com.test.LoadBalancerDemo.requests.RequestToRemoveReplica;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.eclipse.paho.client.mqttv3.*;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class LoadBalancer {
    private static final int MAX_RECORDS_PER_FLUSH = 100;
    private final String filePath = "message_processing_times.csv";
    public IMqttClient mqttClient;
    public IMqttClient mqttClientForPorts;
    public IMqttClient mqttClientForGBs;
    public IMqttClient messagePublisher;
    public IMqttClient startTimeListener;
    public IMqttClient endTimeListener;
    public HashMap<String, BoltInfo> boltRecords;
    //the key is bolt name and the value is name of all DS bolts
    public HashMap<String, List<String>> topology;
    //the key is nameOfOriginalBolt like b2 and the value is the name of replicas
    public HashMap<String, List<String>> replicas;
    public ObjectMapper objectMapper;
    public ApplicationSettings applicationSettings;
    public HashMap<String, BoltLatencyDetails> latencyRecordsFromUpStreamBolts;
    public HashMap<String, BoltLatencyDetails> latencyRecordsFromDownStreamBolts;
    public HashMap<String, List<Integer>> listOfFreeServerPorts;
    public HashMap<String, MessageProcessingTimeUnit> messageProcessingTime;
    public List<LatencyInfo> latencyInfoList;
    public HashMap<String, BandwidthInfo> bandwidthTable;
    public ExecutorService executorService;
    public BufferedWriter writer;
    public HashMap<String, RemainingBandwidthInfoOfVm> tableOfRemainingBandwidthOfVms;
    public ExecutorService threadPool;
    int metricPrecision = (int) Math.pow(10, 3); // 10^3 for 3 decimal places


    public LoadBalancer() {
        boltRecords = new HashMap<String, BoltInfo>();
        topology = new HashMap<String, List<String>>();
        objectMapper = new ObjectMapper();
        replicas = new HashMap<String, List<String>>();
        latencyRecordsFromUpStreamBolts = new HashMap<String, BoltLatencyDetails>();
        latencyRecordsFromDownStreamBolts = new HashMap<String, BoltLatencyDetails>();
        listOfFreeServerPorts = new HashMap<String, List<Integer>>();
        loadLatencyFile();
        executorService = Executors.newFixedThreadPool(10);
        messageProcessingTime = new HashMap<String, MessageProcessingTimeUnit>();
        initializeWriter();


        // Write the header to the file
        try {
            writer.write("MessageID,StartTime,ExecutionTime\n");
            writer.flush();  // Ensure the header is written out immediately
        } catch (IOException e) {
            e.printStackTrace();
        }
        // Schedule periodic flushing of the HashMap to the file
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(this::flushDataToFile, 1, 1, TimeUnit.MINUTES);

        //read bandwidth file
        loadEdgeDeviceBandwidthInfo();
        threadPool = Executors.newFixedThreadPool(20);
    }

    public void manageReplicaCreationBasedOnRemainingInBandwidth(String hostName) {
        //   threadPool.submit(() -> {
        System.out.println("there is a bottleNeck in terms of in_bandwidth in vm = " + hostName);
        String nameOfResourceIntensiveBolt = findBoltWithHighestInBandwidthUsage(hostName);
        System.out.println("name Of resource intensive bolt: " + nameOfResourceIntensiveBolt);

        boolean result = isAverageRemainingInBandwidthBelowThreshold(nameOfResourceIntensiveBolt);
        System.out.println("isAverageRemainingInBandwidthBelowThreshold returns " + result);
        if (result) {
            createReplicaBasedOnBestCandidate(nameOfResourceIntensiveBolt);
        }
        //      });

    }

    public void manageReplicaCreationBasedOnRemainingOutBandwidth(String hostName) {
        System.out.println("there is a bottleNeck in terms of out_bandwidth in vm = " + hostName);
        String nameOfResourceIntensiveBolt = findBoltWithHighestOutBandwidthUsage(hostName);
        System.out.println("name Of resource intensive bolt: " + nameOfResourceIntensiveBolt);

        boolean result = isAverageRemainingOutBandwidthBelowThreshold(nameOfResourceIntensiveBolt);
        System.out.println("isAverageRemainingOutBandwidthBelowThreshold returns " + result);
        if (result) {
            createReplicaBasedOnBestCandidate(nameOfResourceIntensiveBolt);
        }
    }

    public void createReplicaBasedOnBestCandidate(String nameOfResourceIntensiveBolt) {
        HashMap<String, CandidateEvaluationMetrics> listOfAllCandidates = identifyCandidatesForSpanningAReplica(nameOfResourceIntensiveBolt);
        if (listOfAllCandidates != null && listOfAllCandidates.size() > 0) {
            String chosenBoltToBeAReplica = calculateScoreForAllCandidatesAndSelectTheBestCandidate(listOfAllCandidates);
            String originalBolName = findOriginalBoltNameRelatedToAComponentName(nameOfResourceIntensiveBolt);
            addANewReplica(new RequestToAddAReplica(boltRecords.get(nameOfResourceIntensiveBolt).getNameOfClassLoadedInside(),
                    chosenBoltToBeAReplica, originalBolName));
        } else {
            System.out.println("there is no candidate");
        }
    }


    private void initializeWriter() {
        try {
            this.writer = new BufferedWriter(new FileWriter(filePath, true));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void flushDataToFile() {
        try {
            int recordsProcessed = 0;
            Iterator<Map.Entry<String, MessageProcessingTimeUnit>> iterator = messageProcessingTime.entrySet().iterator();

            while (iterator.hasNext() && recordsProcessed < MAX_RECORDS_PER_FLUSH) {
                Map.Entry<String, MessageProcessingTimeUnit> entry = iterator.next();
                String messageId = entry.getKey();
                MessageProcessingTimeUnit unit = entry.getValue();

                // Write to file only if execution time is set
                if (unit.getExecutionTime() != null) {
                    writer.write(messageId + "," + unit.getStartingTime() + "," + unit.getExecutionTime() + "\n");
                    iterator.remove();  // Safely remove the entry
                    recordsProcessed++;
                } else {
                    if (unit.getStartingTime() != null && unit.getEndTime() != null) {
                        Long executionTime = unit.getEndTime() - unit.getStartingTime();
                        unit.setExecutionTime(executionTime);
                        writer.write(messageId + "," + unit.getStartingTime() + "," + unit.getExecutionTime() + "\n");
                        iterator.remove();  // Safely remove the entry
                        recordsProcessed++;
                    }
                }
            }
            writer.flush();  // Ensure all data is written out immediately
            System.out.println("the data is just written to the file");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void connectToBroker() {
        //create a new IMqttClient synchronous instance:
        //The server endpoint we're using is a public MQTT broker hosted
        // by the Paho project, which allows anyone with an internet connection to test clients without
        // the need of any authentication

        System.out.println("load balancer is connecting to the broker");

        try {
            mqttClient = new MqttClient("tcp://192.168.122.98:1883", "LoadBalancer");
            mqttClientForPorts = new MqttClient("tcp://192.168.122.98:1883", "LoadBalancer2");
            mqttClientForGBs = new MqttClient("tcp://192.168.122.98:1883", "LoadBalancer3");
            messagePublisher = new MqttClient("tcp://192.168.122.98:1883", "LoadBalancer4");
            startTimeListener = new MqttClient("tcp://192.168.122.98:1883", "LoadBalancer5");
            endTimeListener = new MqttClient("tcp://192.168.122.98:1883", "LoadBalancer6");

        } catch (MqttException e) {
            System.out.println("there is a problem in creating a mqtt client");
            throw new RuntimeException(e);
        }
        MqttConnectOptions connectOptions = new MqttConnectOptions();
        connectOptions.setCleanSession(true);
        connectOptions.setAutomaticReconnect(true);
        connectOptions.setConnectionTimeout(10000000);
        try {
            mqttClient.connect(connectOptions);
            mqttClientForPorts.connect(connectOptions);
            mqttClientForGBs.connect(connectOptions);
            messagePublisher.connect(connectOptions);
            startTimeListener.connect(connectOptions);
            endTimeListener.connect(connectOptions);
        } catch (MqttException e) {
            System.out.println("there is a problem in connection between load balancer and broker");
            throw new RuntimeException(e);
        }
    }

    public void publishAMessage(String topic, String payload) {
        MqttMessage mqttMessage = new MqttMessage();
        mqttMessage.setQos(2);
        mqttMessage.setRetained(true);
        mqttMessage.setPayload(payload.getBytes(StandardCharsets.UTF_8));

        try {
            messagePublisher.publish(topic, mqttMessage);
            System.out.println("the massage with topic= " + topic + " and payload " + payload + " was successfully published");
        } catch (MqttException e) {
            System.out.println("there is a problem in publishing messages in the load balancer");
            throw new RuntimeException(e);
        }
    }

    public void subscribeToProcessingTime_start(String topic) {
        startTimeListener.setCallback(new MqttCallback() {
            @Override
            public void connectionLost(Throwable throwable) {
                System.out.println("The connection of load balancer to broker is broken");
                throwable.printStackTrace();
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) {
                String messagePayload = new String(message.getPayload());
                //   System.out.println("A message arrived with the topic = " + topic + " and the payload is " + messagePayload);
                Long startTime;
                if (topic.endsWith("/processingTime/start")) {
                    String[] parts = messagePayload.split("/");
                    String messageId = parts[0];
                    startTime = Long.parseLong(parts[1]);
                    // System.out.println("messageId= " + messageId + ", startTime"+ startTime);
                    if (!messageProcessingTime.containsKey(messageId)) {
                        messageProcessingTime.put(messageId, new MessageProcessingTimeUnit(startTime));
                    } else {
                        messageProcessingTime.get(messageId).setEndTime(startTime);
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

    public void subscribeToProcessingTime_end(String topic) {
        endTimeListener.setCallback(new MqttCallback() {
            @Override
            public void connectionLost(Throwable throwable) {
                System.out.println("The connection of load balancer to broker is broken");
                throwable.printStackTrace();
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) {
                String messagePayload = new String(message.getPayload());
                //  System.out.println("A message arrived with the topic = " + topic + " and the payload is " + messagePayload);
                Long endTime;
                Long executionTIme;
                if (topic.endsWith("/processingTime/end")) {
                    if (topic.startsWith("sink")) {
                        String[] parts = messagePayload.split("/");
                        String messageId = parts[0];
                        endTime = Long.parseLong(parts[1]);

                        //the duration is in millisecond
                        if (messageProcessingTime.containsKey(messageId)) {
                            executionTIme = endTime - (messageProcessingTime.get(messageId).getStartingTime());
                            messageProcessingTime.get(messageId).setExecutionTime(executionTIme);
                            messageProcessingTime.get(messageId).setEndTime(endTime);

                            //System.out.println("messageId= "+messageId+", value part ="+messageProcessingTime.get(messageId));
                        } else {

                            MessageProcessingTimeUnit messageProcessingTimeUnit = new MessageProcessingTimeUnit();
                            messageProcessingTimeUnit.setEndTime(endTime);
                            messageProcessingTime.put(messageId, messageProcessingTimeUnit);

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


    public void subscribeToGenericBoltInitializationTopic(String topic) {
        mqttClientForGBs.setCallback(new MqttCallback() {
            @Override
            public void connectionLost(Throwable throwable) {
                System.out.println("The connection of load balancer to broker is broken");
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
                        // boltRecords.notifyAll();
                    }
                }
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {
            }
        });

        try {
            mqttClientForGBs.subscribe(topic);
            System.out.println("Subscribed to topic= " + topic);
        } catch (MqttException e) {
            System.out.println("Error subscribing to /freeServerPort topic");
            throw new RuntimeException(e);
        }
    }

    public void subscribeToFreeServerPortTopic(String topic) {
        mqttClientForPorts.setCallback(new MqttCallback() {
            @Override
            public void connectionLost(Throwable throwable) {
                System.out.println("The connection of load balancer to broker is broken");
                throwable.printStackTrace();
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {
                // Handle messages for "/freeServerPort"
                String messagePayload = new String(message.getPayload());
                System.out.println("A message for /freeServerPort arrived: " + messagePayload);
                if (topic.endsWith("/freeServerPort")) {
                    System.out.println("A message arrived with the topic = " + topic + " and the payload is " + messagePayload);
                    String receivedPort = messagePayload;
                    synchronized (listOfFreeServerPorts) {
                        String nameOfBolt = topic.substring(0, topic.indexOf("/"));
                        if ((!listOfFreeServerPorts.containsKey(nameOfBolt)) || listOfFreeServerPorts.get(nameOfBolt).size() == 0) {
                            List<Integer> values = new ArrayList<>();
                            values.add(Integer.parseInt(receivedPort));
                            listOfFreeServerPorts.put(nameOfBolt, values);
                        } else {
                            listOfFreeServerPorts.get(nameOfBolt).add(Integer.parseInt(receivedPort));
                        }

                        System.out.println("listOfFreeServerPorts after /freeServerPort is " + listOfFreeServerPorts.toString());
                        listOfFreeServerPorts.notifyAll();
                    }
                }
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {
                // Handle completed delivery
            }
        });

        try {
            mqttClientForPorts.subscribe(topic);
            System.out.println("Subscribed to /freeServerPort topic");
        } catch (MqttException e) {
            System.out.println("Error subscribing to /freeServerPort topic");
            throw new RuntimeException(e);
        }
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
                System.out.println("A message arrived with the topic = " + topic + " and the payload is " + messagePayload);
                if (topic.endsWith("initialInfo")) {

                    BoltInfo updatedBoltInfo = new BoltInfo();
                    try {
                        updatedBoltInfo = objectMapper.readValue(messagePayload, BoltInfo.class);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                    boltRecords.put(updatedBoltInfo.getComponentName(), updatedBoltInfo);
                }
                else if (topic.endsWith("metrics")) {

                    Metrics metrics = new Metrics();
                    int index = topic.indexOf('/');
                    String componentName = null;
                    if (index != -1) {
                        componentName = topic.substring(0, index);
                    }
                    try {
                        metrics = objectMapper.readValue(messagePayload, Metrics.class);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                    boltRecords.get(componentName).getMetrics().setCpu(metrics.getCpu());
                    boltRecords.get(componentName).getMetrics().setCpuAtBoltLevel(metrics.getCpuAtBoltLevel());
                    boltRecords.get(componentName).getMetrics().setIn_throughput(metrics.getIn_throughput());
                    boltRecords.get(componentName).getMetrics().setOut_throughput(metrics.getOut_throughput());
                    //System.out.println("after updating metrics of "+componentName+" , new metrics are= " +boltRecords.get(componentName).getMetrics());


                }
                else if (topic.endsWith("topologyUpdate")) {
                    messagePayload = messagePayload.replace("[", "");
                    messagePayload = messagePayload.replace("]", "");
                    messagePayload = messagePayload.replace(" ", "");
                    String componentName = topic.substring(0, topic.indexOf("/"));
                    topology.put(componentName, Arrays.asList(messagePayload.split(",")));
                    System.out.println("the topology is: " + topology);
                }
                else if (topic.equals("applicationSettings")) {
                    //in order to be done only once
                    if (applicationSettings == null) {
                        try {
                            applicationSettings = new ApplicationSettings();
                            applicationSettings = objectMapper.readValue(messagePayload, ApplicationSettings.class);
                        } catch (JsonMappingException e) {
                            throw new RuntimeException(e);
                        } catch (JsonProcessingException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
                else if (topic.equals("bottleNeck/CPU")) {

                    String nameOfResourceIntensiveBolt = identifyCPUIntensiveBolt(messagePayload);
                    System.out.println("name Of resource intensive bolt: " + nameOfResourceIntensiveBolt);
                    if (nameOfResourceIntensiveBolt != null) {
                        boolean result = isAverageCPUAboveThreshold(nameOfResourceIntensiveBolt);
                        System.out.println("checkIfANewReplicaIsRequired returns " + result);
                        if (result) {
                            createReplicaBasedOnBestCandidate(nameOfResourceIntensiveBolt);
                        }
                    }

                } else if (topic.equals("underUtilization")) {
                    if (topology.containsKey(messagePayload)) {
                        List<String> listOfAllBoltsWithSameFunctionality = findAllBoltNamesWithSameFunctionality(messagePayload);
                        System.out.println("listOfAllBoltsWithSameFunctionality= " + listOfAllBoltsWithSameFunctionality.toString());
                        String nameOfBoltToBeDeleted = null;
                        if (checkIfAReplicaMustBeRemoved(listOfAllBoltsWithSameFunctionality)) {
                            System.out.println("checkIfAReplicaMustBeRemoved= true");
                            nameOfBoltToBeDeleted = identifyAReplicaToBeDeleted(listOfAllBoltsWithSameFunctionality);
                        }
                        if (nameOfBoltToBeDeleted != null) {
                            String nameOfReplicaToBeDeleted = nameOfBoltToBeDeleted;
                            System.out.println("nameOfBoltToBeDeleted = " + nameOfReplicaToBeDeleted);
                            removeAReplica(new RequestToRemoveReplica(findNameOfOriginalBoltByReplicaName(nameOfReplicaToBeDeleted), nameOfReplicaToBeDeleted));
                        }
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
        System.out.println("list of bolts on the specified host name" + boltsOnSameNode.toString());
        return boltsOnSameNode;
    }

    public List<String> findAllBoltNamesWithSameFunctionality(String boltName) {
        System.out.println("going to find all bolt names which loaded same class");
        String nameOfClassLoadedInside = boltRecords.get(boltName).getNameOfClassLoadedInside();
        System.out.println("nameOfClassLoadedInside= " + nameOfClassLoadedInside);
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
        tableOfRemainingBandwidthOfVms = new HashMap<String, RemainingBandwidthInfoOfVm>();
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
                tableOfRemainingBandwidthOfVms.put(vmName, new RemainingBandwidthInfoOfVm());
            }
        } catch (IOException e) {
            throw new RuntimeException("Error reading CSV file", e);
        }
        System.out.println("bandwidth table = " + bandwidthTable.toString());
        System.out.println("throughput table = " + tableOfRemainingBandwidthOfVms.toString());
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

    public boolean checkIfAReplicaMustBeRemoved(List<String> listOfReplicas) {
        if (listOfReplicas.size() > 1) {
            double averageInThroughputAfterRemovingAReplica = 0.0;
            double averageOutThroughputAfterRemovingAReplica = 0.0;
            double averageCPUUsageAfterRemovingAReplica = 0.0;
            for (String replica : listOfReplicas) {
                averageInThroughputAfterRemovingAReplica += boltRecords.get(replica).getMetrics().getIn_throughput();
                averageOutThroughputAfterRemovingAReplica += boltRecords.get(replica).getMetrics().getOut_throughput();

                // averageCPUUsageAfterRemovingAReplica += boltRecords.get(replica).getCpu();
            }
            averageCPUUsageAfterRemovingAReplica = averageCPUUsageAfterRemovingAReplica / (listOfReplicas.size() - 1);
            averageInThroughputAfterRemovingAReplica = averageInThroughputAfterRemovingAReplica / (listOfReplicas.size() - 1);
            averageOutThroughputAfterRemovingAReplica = averageOutThroughputAfterRemovingAReplica / (listOfReplicas.size() - 1);
            if ((averageInThroughputAfterRemovingAReplica <= applicationSettings.getThresholdForMinAverageThroughputOfAllReplicas()) &&
                    (averageOutThroughputAfterRemovingAReplica <= applicationSettings.getThresholdForMinAverageThroughputOfAllReplicas())
                //   && averageCPUUsageAfterRemovingAReplica <= applicationSettings.getThresholdForMinAverageCpuUsageOfAllReplicas()
            ) {
                return true;
            } else return false;
        } else return false;
    }


    public String identifyAReplicaToBeDeleted(List<String> listOfReplicas) {
        double minCPUUsage = 100.0;
        String nameOfReplicaToBeDeleted = null;

        for (String replica : listOfReplicas) {
            if (replica.startsWith("gb") && boltRecords.get(replica).getMetrics().getCpuAtBoltLevel() <= minCPUUsage) {
                minCPUUsage = boltRecords.get(replica).getMetrics().getCpuAtBoltLevel();
                nameOfReplicaToBeDeleted = boltRecords.get(replica).getComponentName();
            }
        }
        return nameOfReplicaToBeDeleted;
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
            if ((latencyInfo.source.equals(hostName1) && latencyInfo.dest.equals(hostName2)) ||
                    (latencyInfo.source.equals(hostName2) && latencyInfo.dest.equals(hostName1))) {
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

    public String findNameOfOriginalBoltByBoltName(String boltName) {
        if (boltName.startsWith("b")) {
            return boltName;
        } else if (boltName.startsWith("gb_") && replicas != null) {
            for (Map.Entry<String, List<String>> entry : replicas.entrySet()) {
                if (entry.getValue().contains(boltName)) {
                    return entry.getKey();
                }
            }
        }
        return null;
    }

    public String findNameOfClassLoadedInsideByBoltName(String boltName) {
        return boltRecords.get(boltName).getNameOfClassLoadedInside();
    }

    public boolean isAverageCPUAboveThreshold(String componentName) {
        double averageCPUOfReplicas = 0.0;

        List<String> listOfBoltsWithSameFunctionality = findAllBoltNamesWithSameFunctionality(componentName);

        for (String s : listOfBoltsWithSameFunctionality) {
            averageCPUOfReplicas += boltRecords.get(s).getMetrics().getCpu();
        }

        averageCPUOfReplicas = averageCPUOfReplicas / listOfBoltsWithSameFunctionality.size();
        System.out.println("averageCPUOfReplicas = " + averageCPUOfReplicas);
        if (averageCPUOfReplicas > applicationSettings.getThresholdForMaxAverageCpuUsageOfAllReplicas()) {

            return true;
        } else return false;

    }

    public boolean isAverageRemainingInBandwidthBelowThreshold(String componentName) {
        List<String> listOfBoltsWithSameFunctionality = new ArrayList<String>();
        double averageRemainingInBandwidthOfAllReplicas = 0.0;

        listOfBoltsWithSameFunctionality = findAllBoltNamesWithSameFunctionality(componentName);
        for (String s : listOfBoltsWithSameFunctionality) {
            averageRemainingInBandwidthOfAllReplicas += boltRecords.get(s).getMetrics().getRemaining_in_bandwidth_of_vm_in_percentage();
        }
        averageRemainingInBandwidthOfAllReplicas = averageRemainingInBandwidthOfAllReplicas / listOfBoltsWithSameFunctionality.size();
        System.out.println("averageRemainingInBandwidthOfAllReplicas= " + averageRemainingInBandwidthOfAllReplicas);
        if (averageRemainingInBandwidthOfAllReplicas <= applicationSettings.getReplicasMinAvgRemainingBandwidthThreshold()) {
            return true;
        } else return false;
    }

    public boolean isAverageRemainingOutBandwidthBelowThreshold(String componentName) {
        List<String> listOfBoltsWithSameFunctionality = new ArrayList<String>();
        double averageRemainingOutBandwidthOfAllReplicas = 0.0;

        listOfBoltsWithSameFunctionality = findAllBoltNamesWithSameFunctionality(componentName);
        for (String s : listOfBoltsWithSameFunctionality) {
            averageRemainingOutBandwidthOfAllReplicas += boltRecords.get(s).getMetrics().getRemaining_out_bandwidth_of_vm_in_percentage();
        }
        averageRemainingOutBandwidthOfAllReplicas = averageRemainingOutBandwidthOfAllReplicas / listOfBoltsWithSameFunctionality.size();
        System.out.println("averageRemainingOutBandwidthOfAllReplicas= " + averageRemainingOutBandwidthOfAllReplicas);
        if (averageRemainingOutBandwidthOfAllReplicas <= applicationSettings.getReplicasMinAvgRemainingBandwidthThreshold()) {
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

    public HashMap<String, CandidateEvaluationMetrics> identifyCandidatesForSpanningAReplica(String componentName) {
        System.out.println("Going to identify candidate for spanning a new replica");
        System.out.println("the bolt records are=" + boltRecords.toString());
        //by considering only cpu and latency and remain_in_bandwidth_of_vm
        // having concurrent bolts on a same device is ok
        String originalBoltName = findOriginalBoltNameRelatedToAComponentName(componentName);
        System.out.println("the original bolt name =" + originalBoltName);
        List<String> upStreamBolts = findUpStreamBoltsByOriginalBoltName(originalBoltName);
        System.out.println("the upStreamBolts =" + upStreamBolts.toString());
        HashMap<String, CandidateEvaluationMetrics> listOfAllCandidates = new HashMap<String, CandidateEvaluationMetrics>();
        //the key is the candidate bolt name

        for (Map.Entry<String, BoltInfo> entry : boltRecords.entrySet()) {
            System.out.println("entry.getValue().getComponentName()=" + entry.getValue().getComponentName());
            System.out.println("entry.getValue().getNameOfClassLoadedInside()=" + entry.getValue().getNameOfClassLoadedInside());
            System.out.println("entry.getValue().getMetrics().getCpu()=" + entry.getValue().getMetrics().getCpu());
            System.out.println("entry.getValue().getMetrics().getRemaining_in_bandwidth_of_vm=" + entry.getValue().getMetrics().getRemaining_in_bandwidth_of_vm()
            );

            if (entry.getValue().getNameOfClassLoadedInside().equals("none") &&
                    entry.getValue().getMetrics().getCpu() < applicationSettings.getMax_cpu_threshold()
                    && entry.getValue().getMetrics().getRemaining_in_bandwidth_of_vm() > applicationSettings.getMin_remaining_bandwidth_threshold()
            ) {

                String candidateName = entry.getValue().getComponentName();
                System.out.println("candidate name=" + candidateName);
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
                listOfAllCandidates.put(candidateName, candidateEvaluationMetrics);
            }
        }
        System.out.println("listOfAllCandidates is: " + listOfAllCandidates.toString());
        return listOfAllCandidates;
    }

    public String calculateScoreForAllCandidatesAndSelectTheBestCandidate(HashMap<String, CandidateEvaluationMetrics> listOfAllCandidates) {
        if (listOfAllCandidates == null) {
            System.out.println("list of candidates are empty");
            return null;
        }
        //going to normalize stuff
        double maxLatencySum = listOfAllCandidates.values().stream().mapToDouble(c -> c.averageConnectionLatencyToAllDownStreamBolts + c.averageConnectionLatencyToAllUpStreamBolts).max().getAsDouble();
        //    System.out.println("maxLatencySum is " + maxLatencySum);

        double maxCPUUsage = listOfAllCandidates.values().stream().mapToDouble(c -> c.cpuUsage).max().getAsDouble();
        //     System.out.println("maxCPUUsage is " + maxCPUUsage);
        double maxRemainingBandwidth = listOfAllCandidates.values()
                .stream()
                .mapToDouble(c -> c.getRemainingInBandwidth())
                .max()
                .getAsDouble();

        //  float maxRemainingBandwidthAsFloat = (float) maxRemainingBandwidth;
        for (CandidateEvaluationMetrics candidate : listOfAllCandidates.values()) {
            double latencySum = candidate.getAverageConnectionLatencyToAllDownStreamBolts() + candidate.getAverageConnectionLatencyToAllUpStreamBolts();

            double normalizedLatencySum = (maxLatencySum + 1) - latencySum;
            double normalizedCPUUsage = (maxCPUUsage + 1) - candidate.getCpuUsage();
            double normalizedRemainingBandwidth = candidate.getRemainingInBandwidth() / (maxRemainingBandwidth + 1);

            candidate.setScore((normalizedLatencySum * applicationSettings.getWeight_for_latency()) +
                    (normalizedRemainingBandwidth * applicationSettings.getWeight_for_in_bandwidth_of_target()) +
                    normalizedCPUUsage * applicationSettings.getWeight_for_cpu());
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
    }

    public <K, V> K getKeyFromValue(HashMap<K, V> map, V value) {
        for (Map.Entry<K, V> entry : map.entrySet()) {
            if (entry.getValue().equals(value)) {
                return entry.getKey();
            }
        }
        return null; // Return null if the value is not found
    }

    public List<BoltInfo> informDSBltsOfNewReplica(String nameOfOriginalBolt, String replicaName) {
        List<String> listOfDSBoltsName = new ArrayList<String>();
        listOfDSBoltsName = topology.get(nameOfOriginalBolt);

        List<BoltInfo> dsBolts = new ArrayList<BoltInfo>();
        for (String name : listOfDSBoltsName) {
            BoltInfo boltInfo = boltRecords.get(name);
            publishAMessage(name + "/newUpstreamGb", replicaName);
            dsBolts.add(boltInfo);
        }
        for (BoltInfo boltInfo : dsBolts) {
            synchronized (listOfFreeServerPorts) {
                while (!listOfFreeServerPorts.containsKey(boltInfo.getComponentName()) ||
                        listOfFreeServerPorts.get(boltInfo.getComponentName()).size() == 0) {
                    try {
                        listOfFreeServerPorts.wait();  // Wait until a free port is available
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();  // set the interrupt flag
                        System.out.println("Thread was interrupted, failed to complete operation");
                        return dsBolts;  // or handle interruption appropriately
                    }
                }
                // Proceed when the condition is met
                boltInfo.setServerPort(listOfFreeServerPorts.get(boltInfo.getComponentName()).get(0));
                listOfFreeServerPorts.get(boltInfo.getComponentName()).remove(0);
            }
        }
        return dsBolts;

    }

    public void removeAReplica(RequestToRemoveReplica request) {
        String nameOfReplica = request.getBoltNameToDelete();

        String originalBoltName = request.getOriginalBoltName();
        List<String> upStreamBolts = findUpStreamBoltsByOriginalBoltName(originalBoltName);
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
    }

    public void addANewReplica(RequestToAddAReplica request) {

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

            while (!boltRecords.get(nameOfReplica).nameOfClassLoadedInside.equals(request.getNameOfClassLoadedInside())) {
            }
            List<String> upStreamBolts = findUpStreamBoltsByOriginalBoltName(originalBoltName);

            for (String nameOfUpStreamBolt : upStreamBolts) {
                publishAMessage(nameOfReplica + "/newUpstreamGb", nameOfUpStreamBolt);
                synchronized (listOfFreeServerPorts) {
                    while (!(listOfFreeServerPorts.containsKey(nameOfReplica)) ||
                            listOfFreeServerPorts.get(nameOfReplica).size() == 0) {
                        try {
                            listOfFreeServerPorts.wait();  // Wait until the condition changes
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }

                BoltInfo b = boltRecords.get(nameOfReplica);
                b.setServerPort(listOfFreeServerPorts.get(nameOfReplica).get(0));
                try {
                    publishAMessage(nameOfUpStreamBolt + "/add", objectMapper.writeValueAsString(b));
                    List<String> modifiableList = new ArrayList<>(topology.get(nameOfUpStreamBolt));
                    modifiableList.add(nameOfReplica);
                    topology.put(nameOfUpStreamBolt, modifiableList);
                    System.out.println("topology is: " + topology.toString());
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }

                listOfFreeServerPorts.get(nameOfReplica).remove(0);
                //  }
                listOfFreeServerPorts.remove(nameOfReplica);
            }
        }
    }

    public void storeVMBandwidthInfoInCSVFile() {
        threadPool.submit(() -> {
            File csvFile = new File("remaining_bandwidth.csv");
            boolean isFileEmpty = csvFile.length() == 0;  // Check if file is empty

            try (FileWriter writer = new FileWriter(csvFile, true)) { // Append mode enabled
                // Write header only if the file doesn't exist
                if (isFileEmpty) {
                    writer.append("VM Name,Remaining In Bandwidth,Remaining Out Bandwidth,In Bandwidth Percentage,Out Bandwidth Percentage\n");
                }

                // Write data
                for (String vmName : tableOfRemainingBandwidthOfVms.keySet()) {
                    RemainingBandwidthInfoOfVm bandwidthInfo = tableOfRemainingBandwidthOfVms.get(vmName);
                    writer.append(vmName)
                            .append(",")
                            .append(String.valueOf(bandwidthInfo.getRemainingInBandwidth()))
                            .append(",")
                            .append(String.valueOf(bandwidthInfo.getRemainingOutBandwidth()))
                            .append(",")
                            .append(String.valueOf(bandwidthInfo.getRemainingInBandwidthPercentage()))
                            .append(",")
                            .append(String.valueOf(bandwidthInfo.getRemainingOutBandwidthPercentage()))
                            .append("\n");
                }
                System.out.println("table of bandwidth info has been saved to remaining_bandwidth.csv");

            } catch (IOException e) {
                e.printStackTrace();
            }
        });

    }

    public void calculateRemainingBandwidthOfVms(int initialDelay, int period) {
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        Runnable task = new Runnable() {
            public List<String> listOfVmsWithInBandwidthBottleNeck = new ArrayList<>();
            public List<String> listOfVmsWithOutBandwidthBottleNeck = new ArrayList<>();
            float vmOutBandwidth;
            float vmInBandwidth;

            @Override
            public void run() {
                //     System.out.println("BIBBBBB, going to calculate remaining bandwidth of Vms");
                //     if(listOfVmsWithInBandwidthBottleNeck!=null){
                listOfVmsWithInBandwidthBottleNeck.clear();
                //    }
                //   if(listOfVmsWithOutBandwidthBottleNeck!=null){
                listOfVmsWithOutBandwidthBottleNeck.clear();
                //   }

                for (String vmName : tableOfRemainingBandwidthOfVms.keySet()) {
                    // System.out.println("BIBBBBB, in tableOfRemainingBandwidthOfVms, vm name ="+vmName);
                    float in_throughput = 0;
                    float out_throughput = 0;
                    float currentRemainingInBandwidth = 0;
                    float currentRemainingOutBandwidth = 0;
                    for (BoltInfo infoOfOperator : boltRecords.values()) {
                        if (infoOfOperator.getHostName().equals(vmName)) {
                            //  System.out.println("BIBBBBB, boltName ="+infoOfOperator.getComponentName());
                            //  System.out.println("BIBBBBB, in_th ="+infoOfOperator.getMetrics().getIn_throughput());
                            //  System.out.println("BIBBBBB, out_th ="+infoOfOperator.getMetrics().getOut_throughput());

                            in_throughput += infoOfOperator.getMetrics().getIn_throughput();
                            out_throughput += infoOfOperator.getMetrics().getOut_throughput();
                        }
                    }
                    // System.out.println("BIBBBBB, overall in_throughput=" +in_throughput+"vm name= "+vmName);
                    // System.out.println("BIBBBBB, overall out_throughput=" +out_throughput+"vm name= "+vmName);

                    vmOutBandwidth = bandwidthTable.get(vmName).getOutBandwidth();
                    vmInBandwidth = bandwidthTable.get(vmName).getInBandwidth();
                    //should be performed after all vms got their bandwidth updated
                    if (in_throughput / vmInBandwidth >= applicationSettings.getMax_throughput_threshold()) {
                        listOfVmsWithInBandwidthBottleNeck.add(vmName);
                    }
                    if (out_throughput / vmOutBandwidth >= applicationSettings.getMax_throughput_threshold()) {
                        listOfVmsWithOutBandwidthBottleNeck.clear();
                    }
                    currentRemainingOutBandwidth = vmOutBandwidth - out_throughput;
                    currentRemainingInBandwidth = vmInBandwidth - in_throughput;

                    if (currentRemainingOutBandwidth < 0) {
                        currentRemainingOutBandwidth = 0;
                    }
                    if (currentRemainingInBandwidth < 0) {
                        currentRemainingInBandwidth = 0;
                    }
                    tableOfRemainingBandwidthOfVms.get(vmName).setRemainingOutBandwidth(currentRemainingOutBandwidth);
                    tableOfRemainingBandwidthOfVms.get(vmName).setRemainingInBandwidth(currentRemainingInBandwidth);
                    if (currentRemainingInBandwidth != 0.0) {
                        tableOfRemainingBandwidthOfVms.get(vmName).setRemainingInBandwidthPercentage((Math.round((currentRemainingInBandwidth / vmInBandwidth) * metricPrecision) / (float) metricPrecision));
                    }
                    if (currentRemainingOutBandwidth != 0.0) {
                        tableOfRemainingBandwidthOfVms.get(vmName).setRemainingOutBandwidthPercentage((Math.round((currentRemainingOutBandwidth / vmOutBandwidth) * metricPrecision) / (float) metricPrecision));
                    }

                }
                // System.out.println("BIBBBBB, tableOfRemainingBandwidthOfVms: " + tableOfRemainingBandwidthOfVms.toString());

                //going to update bolt records
                for (BoltInfo infoOfOperator : boltRecords.values()) {
                    infoOfOperator.getMetrics().setRemaining_in_bandwidth_of_vm(
                            tableOfRemainingBandwidthOfVms.get(infoOfOperator.getHostName()).getRemainingInBandwidth());
                    infoOfOperator.getMetrics().setRemaining_out_bandwidth_of_vm(
                            tableOfRemainingBandwidthOfVms.get(infoOfOperator.getHostName()).getRemainingOutBandwidth());

                    infoOfOperator.getMetrics().setRemaining_in_bandwidth_of_vm_in_percentage(
                            tableOfRemainingBandwidthOfVms.get(infoOfOperator.getHostName()).getRemainingInBandwidthPercentage());
                    infoOfOperator.getMetrics().setRemaining_out_bandwidth_of_vm_in_percentage(
                            tableOfRemainingBandwidthOfVms.get(infoOfOperator.getHostName()).getRemainingOutBandwidthPercentage());

                }
                System.out.println("BIBBBBB, now BoltRecords are: " + boltRecords.toString());

                //going to publish MQTT message
                for (Map.Entry<String, RemainingBandwidthInfoOfVm> a : tableOfRemainingBandwidthOfVms.entrySet()) {
                    publishAMessage(a.getKey() + "/remaining_in_bandwidth_of_vm",
                            String.valueOf(a.getValue().getRemainingInBandwidth()) + "/" + String.valueOf(a.getValue().getRemainingInBandwidthPercentage()));
                    publishAMessage(a.getKey() + "/remaining_out_bandwidth_of_vm",
                            String.valueOf(a.getValue().getRemainingOutBandwidth()) + "/" + String.valueOf(a.getValue().getRemainingOutBandwidthPercentage()));
                }
                for (String vmName : listOfVmsWithOutBandwidthBottleNeck) {
                    manageReplicaCreationBasedOnRemainingOutBandwidth(vmName);
                }
                for (String vmName : listOfVmsWithInBandwidthBottleNeck) {
                    manageReplicaCreationBasedOnRemainingInBandwidth(vmName);
                }
                storeVMBandwidthInfoInCSVFile();
            }
        };

        executorService.scheduleAtFixedRate(task, initialDelay, period, TimeUnit.SECONDS);
    }
}




package com.test.LoadBalancerDemo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.test.LoadBalancerDemo.boltDetails.LatencyInfo;
import com.test.LoadBalancerDemo.metrics.BoltLatencyDetails;
import com.test.LoadBalancerDemo.boltDetails.BoltInfo;
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
    public ExecutorService executorService;
    public BufferedWriter writer;
    private static final int MAX_RECORDS_PER_FLUSH = 100;

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
          /*  mqttClient = new MqttClient("tcp://localhost:1883", "LoadBalancer");
            mqttClientForPorts = new MqttClient("tcp://localhost:1883", "LoadBalancer2");
            mqttClientForGBs = new MqttClient("tcp://localhost:1883", "LoadBalancer3");
            messagePublisher = new MqttClient("tcp://localhost:1883", "LoadBalancer4");
            startTimeListener = new MqttClient("tcp://localhost:1883", "LoadBalancer5");
            endTimeListener = new MqttClient("tcp://localhost:1883", "LoadBalancer6");

           */
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
            public void messageArrived(String topic, MqttMessage message) throws Exception {
                String messagePayload = new String(message.getPayload());
                System.out.println("A message arrived with the topic = " + topic + " and the payload is " + messagePayload);
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
                System.out.println("A message arrived with the topic = " + topic + " and the payload is " + messagePayload);
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
            public void messageArrived(String topic, MqttMessage message) throws Exception {
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
                if (topic.endsWith("metrics")) {

                    BoltInfo updatedBoltInfo = new BoltInfo();
                    try {
                        updatedBoltInfo = objectMapper.readValue(messagePayload, BoltInfo.class);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                    if (boltRecords == null || !boltRecords.containsKey(updatedBoltInfo.getComponentName())) {
                        boltRecords.put(updatedBoltInfo.getComponentName(), updatedBoltInfo);
                    } else {
                        boltRecords.get(updatedBoltInfo.getComponentName()).setThroughput(updatedBoltInfo.getThroughput());
                        boltRecords.get(updatedBoltInfo.getComponentName()).setCpu(updatedBoltInfo.getCpu());
                    }

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
                    boolean result = checkIfANewReplicaIsRequired_CPUFocused(messagePayload);
                    System.out.println("checkIfANewReplicaIsRequired returns " + result);
                    if (result) {
                        String nameOfBusyBolt = messagePayload;
                        HashMap<String, CandidateEvaluationMetrics> listOfAllCandidates = identifyCandidatesForSpanningAReplica(nameOfBusyBolt);
                        if (listOfAllCandidates != null && listOfAllCandidates.size() > 0) {
                            String chosenBoltToBeAReplica = calculateScoreForAllCandidatesAndSelectTheBestCandidate(listOfAllCandidates);
                            String originalBolName = findOriginalBoltNameRelatedToAComponentName(nameOfBusyBolt);
                            RequestToAddAReplica requestToAddAReplica = new RequestToAddAReplica(boltRecords.get(originalBolName).getNameOfClassLoadedInside(),
                                    chosenBoltToBeAReplica, originalBolName);
                            System.out.println(requestToAddAReplica);
                            addANewReplica(new RequestToAddAReplica(boltRecords.get(nameOfBusyBolt).getNameOfClassLoadedInside(), chosenBoltToBeAReplica, originalBolName));
                        } else {
                            System.out.println("there is no candidate");
                        }
                    }

                } else if (topic.equals("bottleNeck/throughput")) {
                    boolean result = checkIfANewReplicaIsRequired_throughputFocused(messagePayload);
                    System.out.println("checkIfANewReplicaIsRequired returns " + result);
                    if (result) {
                        HashMap<String, CandidateEvaluationMetrics> listOfAllCandidates = identifyCandidatesForSpanningAReplica(messagePayload);
                        if (listOfAllCandidates != null && listOfAllCandidates.size() > 0) {
                            String chosenBoltToBeAReplica = calculateScoreForAllCandidatesAndSelectTheBestCandidate(listOfAllCandidates);
                            String originalBolName = findOriginalBoltNameRelatedToAComponentName(messagePayload);
                            addANewReplica(new RequestToAddAReplica(boltRecords.get(messagePayload).getNameOfClassLoadedInside(),
                                    chosenBoltToBeAReplica, originalBolName));
                        } else {
                            System.out.println("there is no candidate");
                        }
                    }

                }
                else if (topic.equals("underUtilization")) {
                  /*  if (topology.containsKey(messagePayload)) {
                        System.out.println("just to test, " + boltRecords.get(messagePayload).getNameOfClassLoadedInside());
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

                   */
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

    public List<String> findAllBoltNamesWithSameFunctionality(String boltName) {
        String nameOfClassLoadedInside = boltRecords.get(boltName).getNameOfClassLoadedInside();
        if (!nameOfClassLoadedInside.equals("none")) {
            List<String> listOfBoltNamesWithSameFunctionality = new ArrayList<String>();

            for (Map.Entry<String, BoltInfo> entry : boltRecords.entrySet()) {
                if (entry.getValue().getNameOfClassLoadedInside().equals(nameOfClassLoadedInside)) {
                    listOfBoltNamesWithSameFunctionality.add(entry.getKey());
                }
            }
            return listOfBoltNamesWithSameFunctionality;
        } else return null;
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
            double averageThroughputAfterRemovingAReplica = 0.0;
            double averageCPUUsageAfterRemovingAReplica = 0.0;
            for (String replica : listOfReplicas) {
                averageThroughputAfterRemovingAReplica += boltRecords.get(replica).getThroughput();
                averageCPUUsageAfterRemovingAReplica += boltRecords.get(replica).getCpu();
            }
            averageCPUUsageAfterRemovingAReplica = averageCPUUsageAfterRemovingAReplica / (listOfReplicas.size() - 1);
            averageThroughputAfterRemovingAReplica = averageThroughputAfterRemovingAReplica / (listOfReplicas.size() - 1);
            if (averageThroughputAfterRemovingAReplica <= applicationSettings.getThresholdForMinAverageThroughputOfAllReplicas() && averageCPUUsageAfterRemovingAReplica <= applicationSettings.getThresholdForMinAverageCpuUsageOfAllReplicas()) {
                return true;
            } else return false;
        } else return false;
    }

    public String identifyAReplicaToBeDeleted(List<String> listOfReplicas) {
        double minCPUUsage = 100.0;
        String nameOfReplicaToBeDeleted = null;

        for (String replica : listOfReplicas) {
            if (replica.startsWith("gb") && boltRecords.get(replica).getCpu() <= minCPUUsage) {
                minCPUUsage = boltRecords.get(replica).getCpu();
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

    public boolean checkIfANewReplicaIsRequired_CPUFocused(String componentName) {
        System.out.println("the cpu usage of the bolt which sent bottleNeck message is " + boltRecords.get(componentName).getCpu());
        List<String> listOfBoltsWithSameFunctionality = new ArrayList<String>();
        double averageCPUOfReplicas = 0.0;

        listOfBoltsWithSameFunctionality = findAllBoltNamesWithSameFunctionality(componentName);

        for (String s : listOfBoltsWithSameFunctionality) {
            averageCPUOfReplicas += boltRecords.get(s).getCpu();
        }

        averageCPUOfReplicas = averageCPUOfReplicas / listOfBoltsWithSameFunctionality.size();
        if (averageCPUOfReplicas > applicationSettings.getThresholdForMaxAverageCpuUsageOfAllReplicas()) {

            return true;
        } else return false;
    }

    public boolean checkIfANewReplicaIsRequired_throughputFocused(String componentName) {
        System.out.println("the cpu usage of the bolt which sent bottleNeck message is " + boltRecords.get(componentName).getCpu());
        List<String> listOfBoltsWithSameFunctionality = new ArrayList<String>();
        double averageThroughputOfReplicas = 0.0;

        listOfBoltsWithSameFunctionality = findAllBoltNamesWithSameFunctionality(componentName);
        for (String s : listOfBoltsWithSameFunctionality) {
            averageThroughputOfReplicas += boltRecords.get(s).getThroughput();
        }
        averageThroughputOfReplicas = averageThroughputOfReplicas / listOfBoltsWithSameFunctionality.size();
        if (averageThroughputOfReplicas > applicationSettings.getThresholdForMaxAverageThroughputOfAllReplicas()) {
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
        //  System.out.println("the bolt records are=" + boltRecords.toString());
        //by considering only cpu and latency
        // having concurrent bolts on a same device is ok
        String originalBoltName = findOriginalBoltNameRelatedToAComponentName(componentName);
        //System.out.println("the original bolt name =" + originalBoltName);
        List<String> upStreamBolts = findUpStreamBoltsByOriginalBoltName(originalBoltName);
        //  System.out.println("the upStreamBolts =" + upStreamBolts.toString());
        HashMap<String, CandidateEvaluationMetrics> listOfAllCandidates = new HashMap<String, CandidateEvaluationMetrics>();
        //the key is the candidate bolt name
        for (Map.Entry<String, BoltInfo> entry : boltRecords.entrySet()) {
            if (entry.getKey().startsWith("gb_") && entry.getValue().getNameOfClassLoadedInside().equals("none") &&
                    entry.getValue().getCpu() < applicationSettings.getMax_cpu_threshold() &&
                    entry.getValue().getThroughput() < applicationSettings.getMax_throughput_threshold()) {
                //     System.out.println("gb is found among bolt records");
                String candidateName = entry.getValue().getComponentName();
                //    System.out.println("candidateName is " + candidateName);
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
                candidateEvaluationMetrics.setCpuUsage(entry.getValue().getCpu());
                listOfAllCandidates.put(candidateName, candidateEvaluationMetrics);
            }
        }
        //  System.out.println("listOfAllCandidates is: " + listOfAllCandidates.toString());
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

        for (CandidateEvaluationMetrics candidate : listOfAllCandidates.values()) {
            double latencySum = candidate.getAverageConnectionLatencyToAllDownStreamBolts() + candidate.getAverageConnectionLatencyToAllUpStreamBolts();
            double normalizedLatencySum = (maxLatencySum + 1) - latencySum;
            double normalizedCPUUsage = (maxCPUUsage + 1) - candidate.getCpuUsage();
            candidate.setScore((normalizedLatencySum * applicationSettings.getWeight_for_latency()) + (normalizedCPUUsage * applicationSettings.getWeight_for_cpu()));
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
}




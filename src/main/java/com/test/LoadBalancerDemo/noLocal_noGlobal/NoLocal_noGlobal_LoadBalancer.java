package com.test.LoadBalancerDemo.noLocal_noGlobal;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.test.LoadBalancerDemo.streamProcessor.LoadBalancer;
import com.test.LoadBalancerDemo.streamProcessor.boltDetails.*;
import com.test.LoadBalancerDemo.streamProcessor.configs.ApplicationSettings;
import com.test.LoadBalancerDemo.streamProcessor.metrics.BoltLatencyDetails;
import com.test.LoadBalancerDemo.streamProcessor.metrics.CandidateEvaluationMetrics;
import com.test.LoadBalancerDemo.streamProcessor.metrics.RemovalCandidate;
import com.test.LoadBalancerDemo.streamProcessor.requests.RequestToAddAReplica;
import com.test.LoadBalancerDemo.streamProcessor.requests.RequestToRemoveReplica;
import com.test.LoadBalancerDemo.streamProcessor.MessageProcessingTimeUnit;
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

import static com.test.LoadBalancerDemo.streamProcessor.BottleneckSorter.sortByPriority;

public class NoLocal_noGlobal_LoadBalancer extends LoadBalancer {
    public NoLocal_noGlobal_LoadBalancer() {
        super();
    }

    @Override
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
                System.out.println("A message arrived with the topic = " + topic);
                //     System.out.println("A message arrived with the topic = " + topic + " and the payload is " + messagePayload);

                if (topic.endsWith("initialInfo")) {

                    BoltInfo updatedBoltInfo = new BoltInfo();
                    try {
                        updatedBoltInfo = objectMapper.readValue(messagePayload, BoltInfo.class);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                    boltRecords.put(updatedBoltInfo.getComponentName(), updatedBoltInfo);
                    storeVMCPUUsageInCSVFile(updatedBoltInfo.getHostName(), updatedBoltInfo.getMetrics().getCpu(), 0, 0);

                }

                else if (topic.endsWith("metrics")) {
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
                    boltRecords.get(componentName).getMetrics().setCpuAtBoltLevel(metrics.getCpuAtBoltLevel());
                    boltRecords.get(componentName).getMetrics().setIn_throughput(metrics.getIn_throughput());
                    boltRecords.get(componentName).getMetrics().setOut_throughput(metrics.getOut_throughput());
                    storeVMCPUUsageInCSVFile(boltRecords.get(componentName).getHostName(), metrics.getCpu(), count, emitRate);

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
                            numberOfOperatorsInTopology = applicationSettings.getNumberOfBoltsInTheSystem() + applicationSettings.getNumberOfSinksInTheSystem()
                                    + applicationSettings.getNumberOfSpoutsInTheSystem()
                                    + applicationSettings.getNumberOfGenericBoltsInTheSystem();
                            emitRate = Integer.parseInt(applicationSettings.getInitialEmitRate());
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
                                }
                            };
                            executorServiceForUpStreamOps.schedule(task, 0, TimeUnit.SECONDS);

                        } catch (JsonMappingException e) {
                            throw new RuntimeException(e);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
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




    @Override
    public void calculateRemainingBandwidthOfVms(int initialDelay, int period) {
        rBWExecutor = Executors.newSingleThreadScheduledExecutor();
        Runnable task = new Runnable() {

            float vmOutBW;
            float vmInBW;

            @Override
            public void run() {
                System.out.println("going to calculate remaining bandwidth of Vms");
                if (boltRecords.size() == 0 || applicationSettings == null || boltRecords.size() < numberOfOperatorsInTopology || writerForBWCsvFile == null) {
                    System.out.println("boltRecords.size()=" + boltRecords.size());
                    System.out.println("numberOfOperatorsInTopology=" + numberOfOperatorsInTopology);
                    System.out.println("Condition not met, skipping this run.");
                    return;
                } else {
                    for (String vmName : tableOfRBWOfVms.keySet()) {
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



                    //going to update bolt records
                    for (BoltInfo infoOfOperator : boltRecords.values()) {
                        String hostName = infoOfOperator.getHostName();
                        infoOfOperator.getMetrics().setRemaining_in_bandwidth_of_vm(tableOfRBWOfVms.get(hostName).getRemainingInBandwidth());
                        infoOfOperator.getMetrics().setRemaining_out_bandwidth_of_vm(tableOfRBWOfVms.get(hostName).getRemainingOutBandwidth());

                        infoOfOperator.getMetrics().setRemaining_in_bandwidth_of_vm_in_percentage(tableOfRBWOfVms.get(hostName).getRemainingInBandwidthPercentage());
                        infoOfOperator.getMetrics().setRemaining_out_bandwidth_of_vm_in_percentage(tableOfRBWOfVms.get(hostName).getRemainingOutBandwidthPercentage());

                    }
                    storeVMBandwidthInfoInCSVFile(cycle);
                }
            }
        };

        rBWExecutor.scheduleAtFixedRate(task, initialDelay, period, TimeUnit.SECONDS);

    }
}




package com.test.LoadBalancerDemo.Stormbaselines;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.test.LoadBalancerDemo.noLocal_noGlobal.NoLocal_noGlobal_LoadBalancer;
import com.test.LoadBalancerDemo.streamProcessor.boltDetails.BoltInfo;
import com.test.LoadBalancerDemo.streamProcessor.boltDetails.Metrics;
import com.test.LoadBalancerDemo.streamProcessor.configs.ApplicationSettings;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class StormBaseline_LoadBalancer extends NoLocal_noGlobal_LoadBalancer {

    public StormBaseline_LoadBalancer() {
        super();
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
                    opsSentUpstreamAck.add(topic.split("/")[0] );
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
              //  System.out.println("A message arrived with the topic = " + topic);
                    System.out.println("A message arrived with the topic = " + topic + " and the payload is " + messagePayload);

                if (topic.endsWith("initialInfo")) {

                    BoltInfo updatedBoltInfo = new BoltInfo();
                    try {
                        updatedBoltInfo = objectMapper.readValue(messagePayload, BoltInfo.class);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                    String componentIdentifier=updatedBoltInfo.getComponentName()+"-"+updatedBoltInfo.getTaskId();
                    System.out.println("componentIdentifier="+componentIdentifier);
                    boltRecords.put(componentIdentifier, updatedBoltInfo);
                    storeVMCPUUsageInCSVFile(updatedBoltInfo.getHostName(), updatedBoltInfo.getMetrics().getCpu(), 0, 0);

                } else if (topic.endsWith("metrics")) {
                    String[] parts = topic.split("/");

                    // Extract the componentName and count
                    String componentIdentifier = parts[0];
                    int count = cycle;

                    Metrics metrics = new Metrics();
                    try {
                        metrics = objectMapper.readValue(messagePayload, Metrics.class);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                    boltRecords.get(componentIdentifier).getMetrics().setIn_throughput(metrics.getIn_throughput());
                    boltRecords.get(componentIdentifier).getMetrics().setOut_throughput(metrics.getOut_throughput());
                    storeVMCPUUsageInCSVFile(boltRecords.get(componentIdentifier).getHostName(), metrics.getCpu(), count, emitRate);

                }




                else if (topic.endsWith("topologyUpdate")) {
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


                            numberOfOperatorsInTopology = applicationSettings.getTotalNumberOfOpsInStormBaselines();


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

                            System.out.println("numberOfOpsInSystem= " + numberOfOperatorsInTopology);
                            Runnable task = new Runnable() {
                                @Override
                                public void run() {
                                    while (programStarted == false) {
                                        System.out.println("going to prepare the thread for upstream ops");
                                        while (boltRecords.size() != numberOfOperatorsInTopology) {
                                            System.out.println("boltRecord.size=" + boltRecords.size());
                                            try {
                                                Thread.sleep(5000);
                                            } catch (InterruptedException e) {
                                                throw new RuntimeException(e);
                                            }
                                        }


                                        sendOpsTheirUpstreams();

                                        while (numberOfReadyOperators < numberOfOperatorsInTopology) {
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
                                        if (opsSentUpstreamAck.size() == numberOfOperatorsInTopology - applicationSettings.getNumberOfSpoutsInTheSystem()) {
                                            publishAMessage("start", "1");
                                            programStarted = true;
                                        } else {
                                            System.out.println("opsSentUpstreamAck="+opsSentUpstreamAck);
                                            System.out.println(" numberOfOperatorsInTopology - applicationSettings.getNumberOfSpoutsInTheSystem()="+(numberOfOperatorsInTopology- applicationSettings.getNumberOfSpoutsInTheSystem()));
                                            System.out.println("going to send op info once again");


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
                                    numberOfOperatorsInTopology=applicationSettings.getNumberOfSpoutsInTheSystem()+applicationSettings.getNumberOfSinksInTheSystem()+applicationSettings.getNumberOfBoltsInTheSystem();

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


}

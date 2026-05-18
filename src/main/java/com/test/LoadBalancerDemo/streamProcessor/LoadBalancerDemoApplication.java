package com.test.LoadBalancerDemo.streamProcessor;

import com.test.LoadBalancerDemo.scalabilityTest.LoadBalancer_ScalabilityTest;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RestController;


@SpringBootApplication
@RestController
public class LoadBalancerDemoApplication {
    //public static LoadBalancer lb;
    //  public static NoLocal_noGlobal_LoadBalancer lb;
    // public static StormBaseline_LoadBalancer lb;
    public static LoadBalancer_ScalabilityTest lb;


    public static void main(String[] args) {
        lb = new LoadBalancer_ScalabilityTest();
        //    lb = new NoLocal_noGlobal_LoadBalancer();
        //  lb = new StormBaseline_LoadBalancer();
        //lb = new LoadBalancer();

        SpringApplication.run(LoadBalancerDemoApplication.class, args);
        System.out.println("Load balancer is up");
        lb.connectToBroker();
        lb.subscribeToATopic("+/metrics");
        lb.subscribeToATopic("+/initialInfo");
        lb.subscribeToATopic("+/topologyUpdate");
        lb.subscribeToATopic("+/+/bottleNeck/CPU");
        lb.subscribeToATopic("applicationSettings");
        lb.subscribeToFreeServerPortTopic("+/freeServerPort");
        lb.subscribeToGenericBoltInitializationTopic("+/classLoaded");
        //   lb.subscribeToATopic("underUtilization");
        lb.subscribeToProcessingTime_start("+/processingTime/start");
        lb.subscribeToProcessingTime_end("+/processingTime/end");
        lb.subscribeToEmitRate("+/emitRate");
        lb.subscribeToEmitRate("+/ready");
        lb.subscribeToEmitRate("+/upstreamAck");
        //  lb.subscribeToBNSignals("+/+/bottleNeck/CPU");
        lb.subscribeToBNSignals("underUtilization");

    }

}

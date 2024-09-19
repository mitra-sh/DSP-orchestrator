package com.test.LoadBalancerDemo;

import com.test.LoadBalancerDemo.boltDetails.BoltRequest;
import com.test.LoadBalancerDemo.requests.BoltInfoTestRequest;
import com.test.LoadBalancerDemo.requests.RequestToAddAReplica;
import com.test.LoadBalancerDemo.requests.RequestToRemoveReplica;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


@SpringBootApplication
@RestController
public class LoadBalancerDemoApplication {
    public static LoadBalancer lb;

    public static void main(String[] args) {
        SpringApplication.run(LoadBalancerDemoApplication.class, args);
        System.out.println("Load balancer is up");
        lb = new LoadBalancer();
        lb.connectToBroker();
        lb.subscribeToATopic("+/metrics");
        lb.subscribeToATopic("+/initialInfo");
        lb.subscribeToATopic("+/topologyUpdate");
        lb.subscribeToATopic("bottleNeck/+");
        lb.subscribeToATopic("applicationSettings");
        lb.subscribeToFreeServerPortTopic("+/freeServerPort");
        lb.subscribeToGenericBoltInitializationTopic("+/classLoaded");
        lb.subscribeToATopic("underUtilization");
        lb.subscribeToProcessingTime_start("+/processingTime/start");
        lb.subscribeToProcessingTime_end("+/processingTime/end");
        lb.calculateRemainingBandwidthOfVms(180,60);
    }

    @RequestMapping(value = "/manualCommandToRemoveAReplica")
    public void removeAReplicaController(@RequestBody RequestToRemoveReplica restCommand) {
        RequestToRemoveReplica command = restCommand;
        lb.removeAReplica(command);
    }

    @RequestMapping(value = "/manualCommandToAddNewReplica")
    public void givingManualCommandController(@RequestBody RequestToAddAReplica restCommand) {
        RequestToAddAReplica command = restCommand;
        lb.addANewReplica(command);
    }


    // this is just for test
    @RequestMapping(value = "/test")
    public void test(@RequestBody BoltInfoTestRequest boltInfoTestRequest) {
        BoltInfoTestRequest request = boltInfoTestRequest;
        System.out.println("the request is= " + request.toString());

        lb.boltRecords.get(request.name).getMetrics().setIn_throughput((float) request.throughput);
        lb.boltRecords.get(request.name).getMetrics().setCpu(request.cpu);
        System.out.println("the boltRecords is= " + lb.boltRecords.toString());
    }

    @RequestMapping(value = "/test1")
    public void findBoltsWithSameFunctionality(@RequestBody BoltRequest request) {
        System.out.println("the output of method findAllBoltNamesWithSameFunctionality= "
                + lb.findAllBoltNamesWithSameFunctionality(request.getName()));
    }


}

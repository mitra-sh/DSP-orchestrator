This project is the external orchestrator for the Storm pipeline, responsible for elastic scaling decisions in the distributed stream processing system.

It receives the bandwidth and CPU metrics published periodically by each operator in the Storm pipeline (see storm-streaming-engine), and aggregates them up to the node level before making any decision. Rather than reacting to noisy signals from individual operators, it calculates the overall inbound and outbound bandwidth, as well as the overall CPU usage, at the node level to check whether the node itself is becoming overloaded.

When adding a new replica, the orchestrator evaluates every candidate idle bolt as a potential host, and picks the one expected to handle the busy operator's load best, the one that has enough resources and would result in the lowest mean latency compared to the other candidates.

When removing a replica, the orchestrator verifies that the remaining replicas can absorb its load without creating a new bottleneck elsewhere. 

Build and deployment are handled through Maven plugins in pom.xml, which package the project and push it directly to the master node on the server running the pipeline.

Tech: Java, Spring Boot, Docker, Maven

A basic implementation of the pastry p2p system

----TODO----
removing nodes from the cluster
	manually
	detecting node failure

----EXAMPLE COMMANDS----
java -cp build/libs/BasicPastry.jar com.hamersaw.basic_pastry.DiscoveryNode 15605
java -cp build/libs/BasicPastry.jar com.hamersaw.basic_pastry.PastryNode /tmp localhost 15605 15606
java -cp build/libs/BasicPastry.jar com.hamersaw.basic_pastry.StoreData 15604 localhost 15605

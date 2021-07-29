# Spark RealTime Projects

> **JVM Arguments** 

```
-Dlog4j.configuration=file:log4j.properties
-Dspark.yarn.app.container.log.dir=app-logs
-Dlogfile.name=bikes
```

> **Realtime UseCase** 

1. Find Yamaha Power Bikes (cc greater than 150) and filter the first owner
2. Find how many distinct bike brands by first owner and second owner, age within 3 and price range between 40K to 1 lakh
 

> **Spark Submit** 
 
 1. **Local Mode** *(For Windows keep back tick (`) or for Linux replace with (\\))*
 
      ```
        spark-submit `
        --master local[*] `
        --files "log4j.properties" `
        --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties -Dspark.yarn.app.container.log.dir=app-logs -Dlogfile.name=bikesss" `
        --conf "spark.excutor.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties -Dspark.yarn.app.container.log.dir=app-logs -Dlogfile.name=bikesss" `
        --conf "outputPath=target/vehicles" `
        --class com.hackprotech.FirstOwnerYamahaPowerBikesDF `
        target/scala-2.12/spark-realtime-projects-assembly-1.0.jar
      ```

> **How to setup Multinode Cluster with Apache Spark and Apache Hadoop**

1. Create 3 Virtual machines in **Cloud(GCS, AWS., etc)** or **Local(VMware, VirtualBox)**
2. Need to form the cluster by connecting all maching together using **SSH**
3. **Download Apache Hadoop and Apache Spark in Machine**
4. Edit the **core-site.xml** file in hadoop.3.3.1/etc folder
5. 

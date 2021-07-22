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
 
 1. **Local Mode**
 
      ```
        spark-submit --master local[*] --class com.hackprotech.FirstOwnerYamahaHeavyPowerBikes --files src/main/resources/env/local.conf  --conf "spark.driver.extraJavaOptions=-Dconfig.file=src/main/resources/env/local.conf"  target/scala-2.12/spark-realtime-projects-assembly-1.0.jar
      ```

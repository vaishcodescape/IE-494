<details>
<summary>pom.xml (full)</summary>
	```xml
	<?xml version="1.0" encoding="UTF-8"?>
	<project xmlns="http://maven.apache.org/POM/4.0.0"
	         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
	         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
	
	    <modelVersion>4.0.0</modelVersion>
	
	    <groupId>pmj.bdp.spark</groupId>
	    <artifactId>lab5</artifactId>
	    <version>1.0</version>
	    <packaging>jar</packaging>
	
	    <properties>
	        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
	        <maven.compiler.release>17</maven.compiler.release>
	        <spark.version>4.2.0</spark.version>
	    </properties>
	
	    <dependencies>
	        <dependency>
	            <groupId>org.apache.spark</groupId>
	            <artifactId>spark-core_2.13</artifactId>
	            <version>${spark.version}</version>
	        </dependency>
	    </dependencies>
	
	    <build>
	        <finalName>lab5</finalName>
	        <plugins>
	            <plugin>
	                <groupId>org.codehaus.mojo</groupId>
	                <artifactId>exec-maven-plugin</artifactId>
	                <version>3.5.0</version>
	            </plugin>
	        </plugins>
	    </build>
	
	</project>
	```
</details>
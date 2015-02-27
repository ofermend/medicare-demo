Using PageRank to find Anomalies in healthcare
==================

This repository is a simple demo of using Personalized PageRank to identify provider features that may be useful for anomaly detection in healthcare payment data. 

Authors:
* Ofer Mendelevitch, Hortonworks
* Jiwon Seo, Stanford University

Overview
========
Identifying fraud, waste and abuse is a critical application for any insurance company, and a big focus for healthcare insureres around the world. Various approaches have been applied to address this problem, including rules-based and machine-learning-based solutions. 

Graph algorithms have potential to improve the accuracy of such systems, and are thus of high interest. In this demo we show an approach to identifying anomalies in a real-world healthcare payment dataset, the Medicare-B data set, using a variant of the personalized PageRank algorithm.

We use Apache Pig and SociaLite (open-source graph analysis platform) to preprocess the data and analyze the data.
The details of the algorithm is described in Hortonworks blog (TBD link)

Installation with Hortonworks Sandbox
====
To try this demo on the Hortonworks Sandbox, follow these steps:

* Download the HDP Sandbox and install it on your machine, following the instructions here: http://hortonworks.com/products/hortonworks-sandbox/. We recommend at least 8GB to be allocated to the Sandbox. Whether you use VMWare, VirtualBox or Hyper-V is your choice, the demo should work equally well on each of these products.
* Login to the new sandbox as root
* Install ant: "yum install -y ant"
* Install some development package: "yum install -y python-devel gcc-c++"
* Install the python pandas library: "pip install pandas"
* login as user guest: "su - guest"
* Clone this github repository: "git clone https://github.com/ofermend/medicare-demo.git"
* Change folder to the medicare-demo folder: "cd medicare-demo"
* run setup script to download datasets: "source setup.sh". This downloads various datasets used in the demo and places them into the "ref_data" subfolder
* Configure SociaLite. SociaLite source code is already included in this repository, under the "socialite" folder. You will need to configure and compile SociaLite before using it, using the following steps:
  * cd socialite
  * Edit conf/socialite-env.sh and ensure settings are correct (or change if needed):
    * JAVA_HOME should point to your Java installation path (e.g. /usr/lib/jvm/jdk1.7.0_51/), or be commented out if this environment variable is already set.
    * HADOOP_HOME should point to your Hadoop installation path (e.g., /usr/hdp/current/hadoop-client), or be commented out if this environment variable is already set.
    * SOCIALITE_HEAPSIZE should be set to 6000 (6GB). In the repo it is set to 10000 (10GB) to enable processing the full graph, but with the sandbox we will work with a smaller sub-graph so that it fits in the more limited memory space.
  * Compile socialite using this command: "ant" 

Installation on a Hadoop cluster
====
If you have a Hadoop cluster you can use, follow these steps:

* Login to one of the edge nodes on the cluster.
* Ensure you have python, pandas and ant installed on your edge node. You might need to ask your administrator to help you with those if they are not already installed.
* Clone this github repository: "git clone https://github.com/ofermend/medicare-demo.git"
* Change folder to the medicare-demo folder: "cd medicare-demo"
* run setup script to download datasets: "source setup.sh". This downloads various datasets used in the demo and places them into the "ref_data" subfolder
* Configure SociaLite. SociaLite source code is already included in this repository, under the "socialite" folder. You will need to configure and compile SociaLite before using it, using the following steps:
  * cd socialite
  * Edit conf/socialite-env.sh and ensure settings are correct (or change if needed):
    * JAVA_HOME should point to your Java installation path (e.g. /usr/lib/jvm/jdk1.7.0_51/), or be commented out if this environment variable is already set.
    * HADOOP_HOME should point to your Hadoop installation path (e.g., /usr/hdp/current/hadoop-client), or be commented out if this environment variable is already set.
    * SOCIALITE_HEAPSIZE should be set to 10000 (10GB).
  * Compile socialite using this command: "ant" 

Running the demo
====
To run the demo code, follow these steps:

* run1.sh: perform pre-processing using Apache PIG scripts to clean the data and generate the provider similarity graph with 10% of the providers. 
  * If running on the Sandbox, you should use "run1.sh 10". This executes the script but only on 10% of the providers. This limits the dataset to a smaller size, making it reasonable for a small sandbox environment

* "run2.sh": runs the personalized PageRank algorithm to find anomalies in the data set. The find-anomalies.py implements the algorithm. The output is printed on the screen and also stored in the results-100.txt so it's easier to view it later.
  * If running in the Sandbox, you should use "run2.sh 10" to target the script at the 10% reduced-size dataset. The output file will be named results-10.txt

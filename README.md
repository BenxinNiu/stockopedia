# stockopedia

# Project setup (Developers)
1. Install IDE
2. install sbt by running sudo apt-get install sbt
3. Check java and jdk by running java -vesion
4. Install Spark, if desired to submit spark job locally. 
5. git clone this project from master branch
6. sbt package 
7. sbt run    to run the application locally

# Spark submission
1. Submit a complete built jar: 
./bin/spark-submit \   # your spark localtion
  --class Entry/DailyEntry \
  --master local[4] \  # your core number
  --deploy-mode cluster \
  --conf "path to you conf file" \  # ingore this as default conf will be used during develop 
  Target/stockopedia.0.0.1.jar \ 
  false                 # env argument
  
  # Daily data download automation
  1. Navigate to Scripts directory
  2. run wgetIt.sh Default download directories are "/home/benxin/stockopedia_daily/..."
  
  # NiFi configuration
  1. Navigate to NiFi directory
  2. Replace flow.xml and bootstrap.conf file in you local NiFi directory bin diectory
  3. Start NiFi
  
  

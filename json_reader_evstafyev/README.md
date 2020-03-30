#### Task:

Напишите приложение, которое читает [json-файл](https://storage.googleapis.com/otus_sample_data/winemag-data.json.tgz) 
с помощью Spark RDD API без использования Dataframe/Dataset,
конвертирует его содержимое в case class’ы и распечатывает их в stdout.
Расположение файла передается первым и единственным аргументом.

#### Install software (macOS)

- builder: `brew install sbt`
- ide: `brew cask install intellij-idea-ce` 
- install scala==2.11:
    1. `brew install scala@2.11`
    2. `echo 'export PATH="/usr/local/opt/scala@2.11/bin:$PATH"' >> ~/.zshrc` 
    3. `source ~/.zshrc` 
- install spark==2.4.x:
    1. `brew install apache-spark`
    2. `echo 'export SPARK_HOME="/usr/local/Cellar/apache-spark/2.4.5/libexec"' >> ~/.zshrc`
    3. `echo 'export PYTHONPATH="/usr/local/Cellar/apache-spark/2.4.5/libexec/python/:$PYTHONP$"' >> ~/.zshrc`
- create initial project (sbt OR idea): 
    - `sbt new MrPowers/spark-sbt.g8`
    - https://www.javahelps.com/2018/12/setup-scala-on-intellij-idea.html
#### Build project (optional, because JAR file is already prepared)

- create fat jar file: `sbt compile && sbt assembly`

#### How to RUN:

1. `git clone git@github.com:achicha/otus-de-2020-02.git`
2. `cd json_reader_evstafyev`
3. `spark-submit --master "local[*]" --class JsonReader target/scala-2.11/json_reader_evstafyev-assembly-0.2.jar src/main/resources/winemag-data.json`

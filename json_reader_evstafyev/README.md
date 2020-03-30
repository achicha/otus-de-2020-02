#### задача:

Напишите приложение, которое читает [json-файл](https://storage.googleapis.com/otus_sample_data/winemag-data.json.tgz) 
с помощью Spark RDD API без использования Dataframe/Dataset,
конвертирует его содержимое в case class’ы и распечатывает их в stdout.
Расположение файла передается первым и единственным аргументом.


#### Приложение запускается командой:

`/path/to/spark/bin/spark-submit --master "local[*]" --class JsonReader /path/to/assembly-jar {path/to/winemag.json}`
`spark-submit --master "local[*]" --class JsonReader target/scala-2.11/json_reader_evstafyev-assembly-0.2.jar winemag-data.json
`
RUN:

`spark-submit --master local[*] --class GrimesGuide target/scala-2.11/crimes_in_boston-assembly-1.0.jar src/main/resources/crime.csv src/main/resources/offense_codes.csv src/main/resources/output`
RUN:

`spark-submit --master "local[*]" --class CrimesGuide target/scala-2.11/crime_in_boston-assembly-0.2.jar src/main/resources/crime.csv src/main/resources/offense_codes.csv src/main/resources/output`

#### Task

Практика для занятия Spark - Part 2
Для практики понадобятся sbt и IntelliJ IDEA.

Скачайте датасет https://drive.google.com/open?id=1lreZQzEArdpatlZ2DYBK1cxA_H0IZiam
Этот датасет содержит 3 файла:

    - omni_clickstream.json - логи визитов посетителей по различным страницам продуктов
    - products.json - соответствие URL продукта и категории
    - users.json - описание посетителей

Соберите витрину, содержащую топ 3 наиболее популярных категорий, просматриваемых мужчинами и женщинами (по 3 категории для каждой группы), с указанием числа просмотров.


#### Run
`spark-submit --master "local[*]" --class ClickStream target/scala-2.11/ClickStream-assembly-0.1.jar src/main/resources`
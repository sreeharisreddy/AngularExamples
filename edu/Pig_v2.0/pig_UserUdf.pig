users = LOAD  '/user/training/movie-100k/u.user' USING  PigStorage('|')   AS (userId: chararray, age: int, gender: chararray, occupation: chararray, zipCode: chararray);
REGISTER /home/training/Desktop/exportedUdf.jar
filtered = FILTER users BY com.edupristine.training.pig.FilterTechProfessionalsInAgeOf30s();
dump filtered;
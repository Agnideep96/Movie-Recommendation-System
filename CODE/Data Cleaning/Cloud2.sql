loadMovieData = LOAD 'gs://dataproc-staging-europe-west4-396516896416-gmdooth6/Assignment2/movies.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',') as(movieId:chararray, title:chararray, genres:chararray);
viewMovieData = LIMIT loadMovieData 10;
dump viewMovieData;
loadRatingData = LOAD 'gs://dataproc-staging-europe-west4-396516896416-gmdooth6/Assignment2/ratings.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',') as(userId:chararray, movieId:chararray, rating:chararray, timestamp:chararray);
viewRatingData = LIMIT loadRatingData 10;
dump viewRatingData;

mergedData = JOIN loadRatingData BY movieId, loadMovieData BY movieId;
mergedDataTable = FOREACH mergedData GENERATE loadRatingData::movieId AS movieId, loadRatingData::userId AS userId, loadRatingData::rating AS rating, LOWER(title) AS title;
cleanedData = FILTER mergedDataTable BY (movieId IS NOT NULL) and (userId IS NOT NULL) and (rating IS NOT NULL);
final_cleaned_data = FOREACH cleanedData GENERATE REPLACE(REPLACE(REPLACE(REPLACE(REPLACE((REPLACE(title,'[\r\n]+','')),'<[^>]*>' , ' '),'[^a-zA-Z\\s\']+',' '),'(?=\\S*[\'])([a-zA-Z\'-]+)',''),'(?<![\\w\\-])\\w(?![\\w\\-])',''),'[ ]{2,}',' ') AS title, movieId, rating, userId;
order_by_userId = ORDER final_cleaned_data BY userId ASC;
vieWorderedData = LIMIT order_by_userId 10;
dump vieWorderedData;

STORE order_by_userId INTO 'gs://dataproc-staging-europe-west4-396516896416-gmdooth6/Assignment2/cleanedData.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage;

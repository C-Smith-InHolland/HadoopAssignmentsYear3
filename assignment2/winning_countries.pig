/* Read in the data */
playersCSV = LOAD '/user/maria_dev/diplomacy/players.csv' USING PigStorage(',') 
AS(game_id:chararray,
    country:chararray,
    won:chararray,
    num_supply_centers:chararray,
    target:chararray,
    eliminated:chararray,
    start_turn:chararray,
    end_turn:chararray);
  
/* Create sub dataset with the needed columns country and won */
games = FOREACH playersCSV GENERATE country, won;

/* Get all locations where the won is "1" aka a won match*/
filter_data = FILTER games BY (won == '"1"');

/* Group the data on the country resulting in a tuple of values per country */
grouped_data = GROUP filter_data BY country;

/*Flatten the tuples into separate values and count the occurence of each country and append it to the data*/
count2 = foreach grouped_data GENERATE 
             FLATTEN($1.country), 
			 COUNT(filter_data.won);

/* Get all unique values */
unique = DISTINCT count2;

/* Order alphabetically on country, starting at A*/
ordered_data = ORDER unique BY $0 ASC;

/*Show the results */
DUMP ordered_data;




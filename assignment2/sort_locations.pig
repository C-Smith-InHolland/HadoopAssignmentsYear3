/* Read in the data */
ordersCSV = LOAD '/user/maria_dev/diplomacy/orders.csv' USING PigStorage(',') 
AS(game_id:chararray,
    unit_id:chararray,
    unit_order:chararray,
    location:chararray,
    target:chararray,
    target_dest:chararray,
    success:chararray,
    reason:chararray,
    turn_num:chararray);
  
/* Create sub dataset with the needed columns location and target */
locations = FOREACH ordersCSV GENERATE location, target;

/* Get all locations where the target is Holland*/
filter_data = FILTER locations BY (target == '"Holland"');

/* Group the data on the location resulting in a tuple of values per location */
grouped_data = GROUP filter_data BY location;

/*Flatten the tuples into separate values and count the occurence of each location and append it to the data*/
count2 = foreach grouped_data GENERATE 
             FLATTEN($1), 
			 COUNT(filter_data.location);

/* Get all unique values */
unique = DISTINCT count2;

/* Order alphabetically on location, starting at A*/
ordered_data = ORDER unique BY $0 ASC;

/*Show the results */
DUMP ordered_data;




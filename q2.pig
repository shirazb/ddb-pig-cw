-- Load the state, feature and populated_place tables.
RUN /vol/automed/data/usgs/load_tables.pig

place_state_elevation_population =
    FOREACH  populated_place
    GENERATE state_code, elevation, population;

places_by_state_code =
    GROUP place_state_elevation_population
    BY    state_code;

state_code_data =
    FOREACH  places_by_state_code
    GENERATE group AS state_code,
             SUM(place_state_elevation_population.population) AS population,
             ROUND(AVG(place_state_elevation_population.elevation)) AS elevation;

state_code_and_name_data =
    JOIN state_code_data BY state_code,
         state BY code;

state_name_data =
    FOREACH state_code_and_name_data
    GENERATE name AS state_name,
             population,
             elevation;

sorted_state_name_data =
    ORDER state_name_data
    BY state_name;
             
STORE sorted_state_name_data INTO 'q2' USING PigStorage(',');


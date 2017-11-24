-- Load the state, feature and populated_place tables.
RUN /vol/automed/data/usgs/load_tables.pig

place_state_name_pop = 
    FOREACH  populated_place
    GENERATE name, state_code, population;

places_by_state_code =
    GROUP place_state_name_pop
    BY    state_code;

-- Where is optimal place to group? Here has fewest state_code rows.
-- Is join optimised to trim cols of state other than code and name (used later)?
places_by_state_code_and_name =
    JOIN places_by_state_code BY group,
         state BY code;

flat_state_top_five_populations =
    FOREACH places_by_state_code_and_name {
        most_populated_places = 
            ORDER place_state_name_pop
            BY population, name;

        five_most_populated = 
            LIMIT most_populated_places 5;

        five_most_populated_name_population =
            FOREACH  five_most_populated
            GENERATE name,
                     population;
         
        GENERATE state::name AS state_name,
                 FLATTEN(five_most_populated_name_population);
    }

sorted_top_fives =
    ORDER flat_state_top_five_populations
    BY    state_name, population DESC, name;

STORE sorted_top_fives INTO 'q4' USING PigStorage(',');


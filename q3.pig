-- Load the state, feature and populated_place tables.
RUN /vol/automed/data/usgs/load_tables.pig

feature_state_county_type_all = 
    FOREACH  feature
    GENERATE state_name, county, type;

feature_state_county_type = 
    FILTER feature_state_county_type_all
    BY     type == 'ppl' OR type == 'stream';

feature_by_county_and_state =
    GROUP feature_state_county_type
    BY    (state_name, county);

types_per_county =
    FOREACH feature_by_county_and_state {
        ppl_features =
            FILTER feature_state_county_type
            BY     type == 'ppl';
        
        stream_features =
            FILTER feature_state_county_type
            BY     type == 'stream';
        
        GENERATE group.state_name AS state_name,
                 group.county AS county,
                 COUNT(ppl_features.state_name) AS no_ppl,
                 COUNT(stream_features.state_name) AS no_stream;
    }

sorted_types_per_county = 
    ORDER types_per_county
    BY    state_name, county;

STORE sorted_types_per_county INTO 'q3' USING PigStorage(',');


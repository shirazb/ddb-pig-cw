-- Load the state, feature and populated_place tables.
RUN /vol/automed/data/usgs/load_tables.pig

q = FOREACH feature GENERATE latitude;

STORE q INTO 'q' USING PigStorage(',');


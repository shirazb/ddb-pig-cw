-- Load the state, feature and populated_place tables.
RUN /vol/automed/data/usgs/load_tables.pig

feature_states_upper =
    FOREACH  feature
    GENERATE UPPER(state_name) AS state_name;

features_with_states =
    JOIN feature_states_upper BY state_name LEFT,
         state BY name;

features_with_unknown_states =
    FILTER features_with_states
    BY     state::name IS NULL;

unknown_state_names =
    FOREACH  features_with_unknown_states
    GENERATE state_name;

ordered_unknown_state_names =
    ORDER unknown_state_names
    BY    state_name;

-- I think the question means no duplicates
distinct_ordered_unknown_state_names =
    DISTINCT ordered_unknown_state_names;

STORE distinct_ordered_unknown_state_names INTO 'q1' USING PigStorage(',');


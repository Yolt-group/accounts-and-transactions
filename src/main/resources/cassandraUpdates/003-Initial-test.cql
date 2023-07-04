/* This script just exists for 2 reasons. 1) It was used as a test to see if it properly created the table in the correct keyspace.
   Secondly, this script needs to exit because we create the 'snapshot' functionality /test/resources/cassandraUpdateSnapshot to make sure
   the datascience tables exist in an integration test. This functionality NEEDS a script file with the same name. */

CREATE TABLE IF NOT EXISTS test (
    id uuid,
    primary key (id)
);
DROP TABLE IF EXISTS test;

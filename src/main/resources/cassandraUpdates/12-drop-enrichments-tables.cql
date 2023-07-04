/*
 This file is unfortunately prefixed with number 12.. and is not appended to the last file in this folder (034 currently), why?
 Because otherwise it runs before 11-update-activityenrichment-table, which would then add columns to a table already dropped.
 */
DROP TABLE IF EXISTS activity_enrichments_v2;
DROP TABLE IF EXISTS activity_enrichments;
DROP TABLE IF EXISTS activity_enrichments_initiation;
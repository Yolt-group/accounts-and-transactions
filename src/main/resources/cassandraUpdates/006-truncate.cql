/*
 DO NOT REMOVE.

 This migration was used at some point to truncate the accounts and transactions table to clean up
 stale data prior to going live with the new accounts-and-transactions solutions.

 This file is required by the model mutation.

 We've added some dummy statements because otherwise this pod won't boot on a fresh environment (Cassa modelmutation updater thinks this file failed because 0 statements were applied)
 */
 CREATE TABLE IF NOT EXISTS test(id uuid, primary key (id));
 DROP TABLE IF EXISTS test;

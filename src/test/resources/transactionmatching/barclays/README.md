# About this data

The data in this folder is real user-data that was created as follows:
- grab http traffic between bank <-> providers
- run json through the open-banking project to convert to ProviderTransactionDTO
- anonymize the data

## Flawed transactionId generation

The `transactionId` field in this data have been generated with a method that contains a flaw.
The flaw: every 'block' of 50 transactions is assigned a page number.
This page number has been included in the input data for the hash that determines the `transactionId`.
The second batch at `2020-03-31T04:34.json` contains some extra transactions at the beginning which causes the `transactionId` of several subsequent transactions that cross page boundaries to be perturbed.
The logic that matches transactions has been altered to include logic that looks at a transactions' attributes.

## Anonymization

Every field that contains a description has been hashed with a salt that has been discarded.

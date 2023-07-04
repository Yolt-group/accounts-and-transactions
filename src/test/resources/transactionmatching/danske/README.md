# About this data

The data in this folder is real user-data that was created as follows:
- grab http traffic between bank <-> providers
- run json through the open-banking project to convert to ProviderTransactionDTO
- anonymize the data

This test was included because DANSKE changes the `transactionId` (we store this as `externalId`) a lot.
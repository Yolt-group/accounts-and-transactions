# Accounts and Transactions

Responsibilities:

* receives account updates via Kafka topic.
* updates transactions data in the data science keyspace
* updates accounts and transaction data in the keyspace of this pod
* posts messages to upstream Kafka topic to trigger further processing in data science pipeline
* receives messages from data science with information to enrich transactions (see [transaction-enrichment](docs/processes/transaction-enrichment.adoc))
* has an API with which clients can fetch account and transaction data

The most important responsibility of this service is reconciling transaction data.
This is further described in [a dedicated document about transaction reconciliation](docs/processes/transaction-reconciliation.adoc).

## Concepts

The most important concepts in this API are listed below. Read them to get an idea of the data handled by this API:

- [accounts](docs/concepts/accounts.md)
- [transactions](docs/concepts/transactions.md)

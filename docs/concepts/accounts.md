# Account

An account describes all the financial transactions between the bank and a user. 
A user can have multiple accounts at a single bank which means there is a one-to-many association between user-sites and accounts.

### Properties 
There are multiple types of accounts and in general they have the following in common:
* An account has a balance
* An account has transactions (credit or debit)
* An account is related to a user-site

#### Available account types:
The available account types in the system:
```
CURRENT_ACCOUNT
CREDIT_CARD
SAVINGS_ACCOUNT
PREPAID_ACCOUNT
PENSION
INVESTMENT
```

### Lifecycle
We fetch accounts after the user has given consent to fetch their accounts and transactions from a bank (site).

From that point on, an account will stay in the system until one of the following happens:
1. A user marks the whole user-site as deleted. This causes all accounts related to the user-site to also be marked for deletion.

2. A user manually deletes a user-site. This also causes the accounts associated with the user-site to be deleted.

3. The maintenance pod triggers a delete-action on the accounts-and-transactions service which is usually a side effect of a user or a user-site being deleted. This also causes the accounts and transactions to be deleted.
 
* It is currently impossible to delete a single account without also deleting the associated user-site.
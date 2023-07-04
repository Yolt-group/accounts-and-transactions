ALTER TABLE transactions ADD enrichment_labels frozen<set<text>>;
ALTER TABLE transactions ADD last_updated_time timestamp;
ALTER TABLE transactions ADD bank_specific frozen<map<text, text>>;

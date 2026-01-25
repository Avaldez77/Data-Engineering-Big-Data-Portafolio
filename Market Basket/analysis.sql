SELECT item, COUNT(*) AS purchase_count
FROM transactions_clean
GROUP BY item
ORDER BY purchase_count DESC;

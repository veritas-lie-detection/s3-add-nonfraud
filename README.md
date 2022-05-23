# s3-add-nonfraud
## Matching fraudulent 10-Ks to non-fraudulent 10-Ks
This script attempts to find non-fraudulent companies similar to fraudulent companies. To find a similar company, current market cap is found then compared with other companies in the same industry with the same SIC code. If there are no similar market caps, the companies with the largest market cap in the industry are selected. This is done under the assumption that the largest company would follow guidelines similar to the smaller companies. With similar companies found, 10-Ks of the same time range as when fraudulent activity occurred are collected.

### Improvements
Market cap should be matched when fraud was committed, not in the current day. There may also be better methods of finding similar companies.

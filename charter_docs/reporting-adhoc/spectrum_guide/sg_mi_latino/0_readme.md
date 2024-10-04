#### Venona Notes:
* ```SQL ON venona.billing_id = prod.aes_encrypt(prod.aes_decrypt256(bi.account__number_aes256)) --> BI Account to Venona Account```
* ```SQL ON visit__device__uuid = prod.aes_encrypt(prod.aes_decrypt256(equipment__derived_mac_address_aes256)) --> BI MAC TO VENONA MAC ```

#### Video Packages
* LOW TIER PLANS - SAVE OPTION
    * 'SPP Choice', 'Choice'
    * 'SPP Essentials', 'Essentials',


* UPGRADE/STANDARD PLANS:
    * 'Gold', 'SPP Gold',
    * 'Mi Plan Latino', 'SPP Mi Plan Latino',
    * 'Mi Plan Latino Gold','SPP Mi Plan Latino Gold',
    * 'Mi Plan Latino Silver','SPP Mi Plan Latino Silver',
    * 'Seasonal', 'SPP Seasonal',
    * 'Select', 'SPP Select',
    * 'Silver', 'SPP Silver',


* LEGACY PLANS:
  * 'Legacy Limited Basic',
  * 'Legacy Seasonal',
  * 'Legacy Standard',
  * 'Limited',


* Stream TV
  * 'TV Stream', 'SPP Stream','TV Stream Plus', 'SPP Stream Plus', 'Spectrum TV Stream'


* Other:
  * 'NULL',
  * 'Bulk Video'

#### KEY FILTERS USED IN STANDARD VOD TABLE: prod.VOD_ACCT_LEVEL_USAGE_DAILY_AGG
    ```SQL
    AND vod.title__service_category_name != 'ADS'
    AND vod.session__is_error = FALSE
    AND vod.session__viewing_in_s < 14400
    AND vod.session__viewing_in_s >0
    AND vod.asset__class_code = 'MOVIE'
    ```

#### Data Disclaimers:
* WHEN LOOKING AT VIDEO PACKAGES, PLEASE BE ADVISED THAT THERE WAS A VIDEO PACKAGE NAME CHANGE IN MAY 2017 THAT REQUIRES A CASE STMT FOR NORMALIZATION.
* THE VIDEO PACKAGES THEMSELVES REMAINED THE SAME, BUT MOST PLANS GOT A 'SPP' ADDED TO THEIR NAMES.

5) KEY FILTER NOT USED IN STANDARD VOD TABLE: prod.VOD_ACCT_LEVEL_USAGE_DAILY_AGG
    AND vod.session__vod_lease_sid IS NOT NULL


5) prod.VOD_ACCT_LEVEL_USAGE_DAILY_AGG
  a) We observe that the table has accounts with null viewing times, which should not happen.
  b) We observe that employee accounts show up in the table, which should not happen.
  c) We observe that there are macs that are present in Verimatrix and meet the standard VOD vilters in a day but do not show up in the table.

6) AT THIS TIME, YOU DO NOT TRUST THE prod.VOD_ACCT_LEVEL_USAGE_DAILY_AGG. AS A RESULT, YOU UTILIZE VOD EVENTS AND THE KEY VOD FILTERS TO PULL VOD USAGE.

6) DVR vs NON-DVR:
  a) IF AN ACCOUNT HAS AT LEAST ONE DVR ENABLED MAC ID IN A MONTH, THAT ENTIRE ACCOUNT IS CONSIDERED DVR.
  b) IF AN ACCOUNT HAS NO DVR ENABLED MAC IDS, THEN THAT ACCOUNT IS CONSIDERED NON-DVR.
  c) THIS IS DONE TO AVOID AN ACCOUNT BEING CLASSIFED AS BOTH DVR AND NON-DVR WHICH WOULD LEAD TO DOUBLE COUNTING AN ACCOUNT.

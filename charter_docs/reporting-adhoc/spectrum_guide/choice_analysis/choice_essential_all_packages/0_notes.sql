-- ======
-- DATA CONSIDERATIONS
-- ======
- IGUIDE & SPECTRUM GUIDE ACCOUNTS ARE CONSIDERED
- CUSTOMERS MUST BE ACTIVE FOR AN ENTIRE MONTH TO BE COUNTED
- YOU CAPTURE THE FIRST DATE A PACKAGE APPEARS, YOU DO NOT CAPTURE THE FOLLOWING SCENARIO:
  -PACKAGE A WAS CONNECTED ON 2017-01-01
  -PACKAGE B WAS CONNECTED ON 2017-05-01
  -PACLAGE A WAS RECONNECTED ON 2017-07-01
  -IN THE CASE ABOVE, YOUR LOGIC WOULD ONLY CAPTURE THE 2017-01-01 DATE FOR PACKAGE A, NOT BOTH
- BASIC CHURN LOGIC: CUSTOMERS ACTIVE ON FIRST DAY OF THE MONTH MINUS CUSTOMERS ACTIVE ON LAST DAY OF THE MONTH = CUSTOMERS CHURNED
- THERE WERE PACKAGE NAMING CHANGES IN 2017-05-01 THAT HAD TO BE STANDARDIZED VIA CASE STMT
- THERE WAS KMA NAMING CHANGE IN FEB 2017 THAT HAD TO BE STANDARDIZED VIA CASE STMT
- DVR VS NON-DVR STATUS ARE TAKEN ON THE FIRST OF THE MONTH
- THE FOLLOWING VIDEO PACKAGES ARE NOT CONSIDERED TRADITIONAL STB VIDEO PACKAGES:
              -AND ah.product__video_package_type NOT IN ('TV Stream', 'SPP Stream', 'TV Stream Plus', 'SPP Stream Plus')
- WE ONLY CONSIDER ACCOUNTS TIED TO EQUIPMENT LINKED TO THE TRADITIONAL STBs
              -('HD/DVR Converters', 'Standard Digital Converters', 'HD Converters')
- WE OBSERVE NEGATIVE TENURE WHEN A CUSTOMER TRANSFER FROM BEING NON-DVR ON THE FIRST OF THE MONTH AND DVR ON THE END OF THE MONTH
- FOR ACCOUNT CATEGORY, IF AN ACCOUNT HAS AT LEAST ONE DVR ENABLED BOOK, THAT ACCOUNT IS CONSIDERED DVR ENABLED, ELSE THAT ACCOUNT IS CONSIDERED NON-DVR
- KEEP IN MIND THAT YOU HAVE NEGATIVE TENURE HERE, FILTER OUT NEGATIVE CHOICE OR ESSENTIAL VIDEO PACKAGE TENURE or negative any package tenure to avoid counting a record that should not be counted


KEY FILTERS THAT WILL RULE ACCOUNT OUT FROM YOUR BASE TABLES

-- ACCOUNT INFORMATON
AND ah.account__type IN ('SUBSCRIBER')
AND ah.customer__type IN ('Residential')
AND ah.meta__file_type IN ('Residential')
AND ah.product__package_category LIKE '%Video%'
AND ah.product__video_package_type IS NOT NULL
AND ah.product__video_package_type NOT IN ('TV Stream', 'SPP Stream', 'TV Stream Plus', 'SPP Stream Plus', 'N/A')
AND ah.system__kma_desc IN (
                           'Central States',
                           'Central States (KMA)',
                           'Eastern NC',
                           'Greenville, SC / Georgia',
                           'Kansas City / Lincoln',
                           'Michigan',
                           'Michigan (KMA)',
                           'Minnesota',
                           'Minnesota/Nebraska (KMA)',
                           'Northwest (KMA)',
                           'Pacific Northwest',
                           'Sierra Nevada',
                           'Wisconsin',
                           'Wisconsin  (KMA)'
                         )


-- ACCOUNT INFORMATON
AND equipment__category_name IN ('HD/DVR Converters', 'Standard Digital Converters', 'HD Converters')
AND equipment__derived_mac_address_aes256 IS NOT NULL



CUSTOMER CATEGORIES:
Disconnect
Internet Only
Video Only
Video/Voice
Video/Internet/Voice
Voice Only
Other
Video/Internet
Bulk Upgrade
No PSU
Internet/Voice

VIDEO PACKAGES
NULL
Bulk Video
Choice
Essentials
Gold
Legacy
Legacy Limited Basic
Legacy Seasonal
Legacy Standard
Limited
Mi Plan Latino
Mi Plan Latino Gold
Mi Plan Latino Silver
SPP Choice
SPP Essentials
SPP Gold
SPP Limited Basic
SPP Mi Plan Latino
SPP Mi Plan Latino Gold
SPP Mi Plan Latino Silver
SPP Seasonal
SPP Select
SPP Silver

Seasonal
Select
Silver

SPP Stream
SPP Stream Plus
Spectrum TV Stream
TV Stream
TV Stream Plus


LOW TIER PLANS - SAVE OPTION
'SPP Choice', 'Choice'
'SPP Essentials', 'Essentials',

UPGRADE/STANDARD PLANS:
'Gold', 'SPP Gold',
'Mi Plan Latino', 'SPP Mi Plan Latino',
'Mi Plan Latino Gold',
'SPP Mi Plan Latino Gold',
'Mi Plan Latino Silver',
'SPP Mi Plan Latino Silver',
'Seasonal', 'SPP Seasonal',
'Select', 'SPP Select',
'Silver', 'SPP Silver',


LEGACY PLANS:
'Legacy Limited Basic',
'Legacy Seasonal',
'Legacy Standard',
'Limited',

ONLY STREAM:
'TV Stream', 'SPP Stream','TV Stream Plus', 'SPP Stream Plus', 'Spectrum TV Stream'

OTHER:
'NULL',
'Bulk Video'

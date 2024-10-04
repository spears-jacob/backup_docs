2021-08-03
Amanda Ramsay

for each day, the mobile accounts that activated on that day,
their core (and mobile??) account numbers
and if that account has had any portals logins in their first month, then it tells us the first day they visited each portal

(This means that it duplicates accounts, if the account visited multiple portals in the first month)

Depends on:
  prod_mob.mvno_accounts -- there is no stg version of this table, so stg flow has to read across accounts

Dependencies:
  Will be used for a Tableau report, not yet created.
    Working title "SpecMobile SSPP Engagement"

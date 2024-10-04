Digital Ad Insertion occurred beginning of November.
Due to this, A stream post October containing ads is broken up, in order to get proper view times and view counts
    one must group by session__vod_lease_sid.


Due to VOD reporting outage, we have incomplete data for 10/28 to 11/06.
Thus, in for the GPM, we did not report on November.
In the GPM, a household is only counted as deployed/using vod if the STB was connected on both the first and last of the month.
A household is considered a 'NEW_CONNECT' in terms for Legacy STB if the equipment__connection_date is greater than June 1, 2016.

The field asset__class_code = 'MOVIE'  contains both full length movies, as well as regular TV episodes.
The field asset__class_code = 'PREVW' is how to determine if a view was a preview.
Pre DAI, asset__class_code = 'MOVIE' filters out any Previews.
Post DAI, asset__class_code = 'MOVIE' AND/OR session__vod_lease_sid IS NULL filters out any previews.
Post DAI, Ads from DAI can be found in the field title__service_category_name with the value 'ADS'.
Post DAI, to find if a session had an error, take the MAX value of session__is_error.  It is possible
    that a session__vod_lease_sid has TRUE and FALSE for this field, this happens approx 0.01% of the time.
POST DAI, Very rarely, session__viewing_in_s can have innacurate counts (in the range of 40 years or -40 years).
because of this we filter on session__viewing_in_s > 0 and session__viewing_in_s < 14400.

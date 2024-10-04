--add branch1


Input Timezone samples:

UTC  - > GMT or UTC time
America/Denver -> Mountain time
America/Los_Angeles -> pacific time
America/Chicago -> central time
America/New_York -> Eastern Time



Converting epoch to Date

Sample Input format:

1486688400852 (bigint)
or
'1486688400852'(String)


_Func(Column,Output_timezone_format)


select prod.epoch_converter(1486688400852,'America/Denver'); -> Date in Denver
output: 2017-02-09
select prod.epoch_converter(1486688400852,'UTC'); -> Date in UTC
output: 2017-02-10
select prod.epoch_converter(1486688400852); --> Date in Denver(default)
output: 2017-02-09


Converting epoch to Date_hour

Sample Input format:
1486688400852 (bigint)
or
'1486688400852'(String)


_Func(Column,output_timezone_format)

select prod.epoch_datehour(1486688400852,'UTC');
output: 2017-02-10_01
select prod.epoch_datehour(1486688400852,'America/Denver');
output: 2017-02-09_18
select prod.epoch_datehour(1486688400852);  -> date_hour in denver(default)
output: 2017-02-09_18


Converting epoch to Timestamp

_Func(Column,output_timezone_format)

select prod.epoch_timestamp(1486688400852,'UTC');
output: 2017-02-10 01:00:00
select prod.epoch_timestamp(1486688400852,'America/Denver');
output: 2017-02-09 18:00:00
select prod.epoch_timestamp(1486688400852);  
output: 2017-02-09 18:00:00


Converting epoch to Timestamp with millisec

_Func(Column,output_timezone_format)

select prod.epoch_millisec(1486688400852,'UTC');
output: 2017-02-10 01:00:00.852
select prod.epoch_millisec(1486688400852,'America/Denver');
output: 2017-02-09 18:00:00.852
select prod.epoch_millisec(1486688400852);  
output: 2017-02-09 18:00:00.852


Converting Date time(hh:mm | hh:mm:ss |hh:mm:sss) to epoch

case 1:  

Sample Input Format:

2012-10-01T09:45:00Z
2012-10-01T09:45:00.000+02:00
2012-10-01T09:45:00.000Z
2012-10-01T09:45:00.000+0200

2012-10-01 09:45:00Z
2012-10-01 09:45:00.000+02:00
2012-10-01 09:45:00.000Z
2012-10-01 09:45:00.000+0200


_Func(Column)

usage:
select prod.datetime_converter('2012-10-01T09:45:00Z');
output: 1349084700000
select prod.datetime_converter('2012-10-01T09:45:00.000+02:00');
output: 1349077500000
select prod.datetime_converter('2012-10-01T09:45:00.000Z');
output: 1349084700000
select prod.datetime_converter('2012-10-01T09:45:00.000+0200');
output: 1349077500000

select prod.datetime_converter('2012-10-01 09:45:00Z');
output:1349084700000
select prod.datetime_converter('2012-10-01 09:45:00.000+02:00');
output: 1349077500000
select prod.datetime_converter('2012-10-01 09:45:00.000Z');
output: 1349084700000
select prod.datetime_converter('2012-10-01 09:45:00.000+0200');
output: 1349077500000

Note: Above Strings returns EPOCH in UTC, No Additional Parameters required.

case 2:

Sample input Format:
2017-02-10 01:00:00:000
2017-02-10 01:00:00

_Func(column,input_tz_format)-> Provides default epoch in 'UTC'

usage:
select prod.datetime_converter('2017-02-10 01:00:00:000','UTC');
output: 1486688400000
select prod.datetime_converter('2017-02-10 01:00:00','UTC');
output: 1486688400000

Case 3:

Sample Input format
2017-02-10 01:00
2017-02-10 01:00:00
2017-02-10 01:00:00:000

_Func(column,input_tz_format, output_tz_format)

select prod.datetime_converter('2017-02-10 01:00:00:000','UTC','UTC');
output: 1486688400000
select prod.datetime_converter('2017-02-10 01:00','UTC','America/Denver');
output: 1486663200000
select prod.datetime_converter('2017-02-10 01:00:00','UTC','America/Denver');
output: 1486663200000
select prod.datetime_converter('2017-02-10 01:00:00:000','UTC','America/Denver');
output: 1486663200000





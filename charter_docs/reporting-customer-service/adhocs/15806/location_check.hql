use prod;
set mapreduce.input.fileinputformat.split.maxsize=5368709120;
set mapreduce.input.fileinputformat.split.minsize=5368709120;

SELECT DISTINCT
region
, location_state
, division
--, location_city
FROM
prod.quantum_atom_snapshot_accounts_v
WHERE
partition_date_denver>='2019-05-14'

SELECT 
--count(DISTINCT a.call_inbound_key)
DISTINCT a.call_inbound_key, a.call_segment_stop_date_time
FROM 
prod.quantum_atom_call_care_v a
LEFT JOIN 
test.red_atom_cs_call_care_data_v c
on c.call_inbound_key =    a.call_inbound_key
WHERE c.segment_id IS NULL
AND a.partition_date = '2019-10-20'
limit 100
;

--SELECT count(distinct a.call_inbound_key)
--FROM 
--prod.quantum_atom_call_care_v a
--WHERE a.partition_date = '2019-09-01'
--;

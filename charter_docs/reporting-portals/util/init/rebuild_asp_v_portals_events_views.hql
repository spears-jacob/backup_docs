show create table asp_v_venona_events;
show create table asp_v_venona_events_portals;
show create table asp_v_venona_events_portals_idm;
show create table asp_v_venona_events_portals_msa;
show create table asp_v_venona_events_portals_smb;
show create table asp_v_venona_events_portals_specnet;

alter view asp_v_venona_events as select * from prod.core_quantum_events;
alter view asp_v_venona_events_portals as select * from prod.venona_events_portals;
alter view asp_v_venona_events_portals_idm as select * from prod.venona_events_portals where visit__application_details__application_name = 'IDManagement';
alter view asp_v_venona_events_portals_msa as select * from prod.venona_events_portals where (visit__application_details__application_name = 'MySpectrum');
alter view asp_v_venona_events_portals_smb as select * from prod.venona_events_portals where LOWER(visit__application_details__application_name) = 'smb';
alter view asp_v_venona_events_portals_specnet as select * from prod.venona_events_portals where LOWER(visit__application_details__application_name) = 'specnet';

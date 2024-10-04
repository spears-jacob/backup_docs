DROP VIEW IF EXISTS core_quantum_events_portals_v;
DROP VIEW IF EXISTS core_quantum_events_portals_idm_v;
DROP VIEW IF EXISTS core_quantum_events_portals_msa_v;
DROP VIEW IF EXISTS core_quantum_events_portals_smb_v;
DROP VIEW IF EXISTS core_quantum_events_portals_specnet_v;

CREATE VIEW IF NOT EXISTS core_quantum_events_portals_v as select * from prod.venona_events_portals;
CREATE VIEW IF NOT EXISTS core_quantum_events_portals_idm_v as select * from prod.venona_events_portals where visit__application_details__application_name = 'IDManagement';
CREATE VIEW IF NOT EXISTS core_quantum_events_portals_msa_v as select * from prod.venona_events_portals where (visit__application_details__application_name = 'MySpectrum');
CREATE VIEW IF NOT EXISTS core_quantum_events_portals_smb_v as select * from prod.venona_events_portals where LOWER(visit__application_details__application_name) = 'smb';
CREATE VIEW IF NOT EXISTS  core_quantum_events_portals_specnet_v as select * from prod.venona_events_portals where LOWER(visit__application_details__application_name) = 'specnet';

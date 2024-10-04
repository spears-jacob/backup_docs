USE ${env:ENVIRONMENT};

DROP VIEW IF EXISTS asp_v_operational_daily;

create view IF NOT EXISTS asp_v_operational_daily as 
select * from prod.asp_operational_daily;

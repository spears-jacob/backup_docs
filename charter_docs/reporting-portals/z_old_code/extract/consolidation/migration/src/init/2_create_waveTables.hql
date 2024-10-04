USE ${env:ENVIRONMENT};

drop table IF EXISTS asp_migration_date;
create table IF NOT EXISTS asp_migration_date (
  wid int,
  customer_type string,
  wstart string,
  wend string
);

insert into asp_migration_date Values
(1,'Support','2019-01-01','2019-02-22');
insert into asp_migration_date Values
(2,'Support','2019-01-01','2019-02-22');
insert into asp_migration_date Values
(3,'Support','2019-01-01','2019-02-22');
insert into asp_migration_date Values
(4,'Employee','2019-02-22','2019-04-04');
insert into asp_migration_date Values
(5,'Employee','2019-04-04','2019-06-12');
insert into asp_migration_date Values
(6,'Employee','2019-06-12','2019-07-02');
insert into asp_migration_date Values
(7,'Superusers','2019-06-12','2019-07-02');
insert into asp_migration_date Values
(8,'commercial_business','2019-06-25','2019-07-09');
insert into asp_migration_date Values
(9,'Residential','2019-07-09','2019-07-23');
insert into asp_migration_date Values
(10,'commercial_business','2019-07-09','2019-07-23');
insert into asp_migration_date Values
(11,'Employee','2019-07-10','2019-07-16');
insert into asp_migration_date Values
(12,'Residential','2019-07-16','2019-07-30');
insert into asp_migration_date Values
(13,'commercial_business','2019-07-16','2019-07-30');
insert into asp_migration_date Values
(14,'Residential','2019-07-16','2019-07-30');
insert into asp_migration_date Values
(15,'commercial_business','2019-07-23','2019-08-06');
insert into asp_migration_date Values
(16,'commercial_business','2019-07-23','2019-08-06');
insert into asp_migration_date Values
(17,'Residential','2019-07-23','2019-08-06');
insert into asp_migration_date Values
(18,'Residential','2019-07-23','2019-08-06');
insert into asp_migration_date Values
(19,'commercial_business','2019-07-23','2019-08-06');
insert into asp_migration_date Values
(20,'Residential','2019-07-30','2019-08-13');
insert into asp_migration_date Values
(21,'Residential','2019-07-30','2019-08-13');
insert into asp_migration_date Values
(22,'commercial_business','2019-07-30','2019-08-13');
insert into asp_migration_date Values
(23,'Residential','2019-08-06','2019-08-16');
insert into asp_migration_date Values
(24,'Residential','2019-08-06','2019-08-16');
insert into asp_migration_date Values
(25,'commercial_business','2019-08-06','2019-08-16');
insert into asp_migration_date Values
(26,'Residential','2019-08-06','2019-08-16');
insert into asp_migration_date Values
(27,'Residential','2019-08-13','2019-08-20');
insert into asp_migration_date Values
(28,'commercial_business','2019-08-13','2019-08-20');


drop table IF EXISTS asp_migration_spa;
create table IF NOT EXISTS asp_migration_spa (
  wid int,
  footprint string,
  customer_type string,
  sys string,
  prin string,
  agent string
);

insert into asp_migration_spa Values
(1,'TWC','Support','','','');
insert into asp_migration_spa Values
(2,'TWC','Support','','','');
insert into asp_migration_spa Values
(3,'TWC','Support','','','');
insert into asp_migration_spa Values
(4,'TWC','Employee','','','');
insert into asp_migration_spa Values
(5,'TWC','Employee','','','');
insert into asp_migration_spa Values
(6,'TWC','Employee','','','');
insert into asp_migration_spa Values
(7,'TWC','Superusers','','','');

insert into asp_migration_spa Values
(8,'TWC','commercial_business','8347','1000','All');
insert into asp_migration_spa Values
(9,'TWC','Residential','8347','1000','All');
insert into asp_migration_spa Values
(10,'TWC','commercial_business','8109','2000','6000');
insert into asp_migration_spa Values
(11,'BHN','Employee','','','');
insert into asp_migration_spa Values
(12,'TWC','Residential','8109','2000','6000');

insert into asp_migration_spa Values
(13,'TWC','commercial_business','8260','1300','All');
insert into asp_migration_spa Values
(13,'TWC','commercial_business','8260','1400','All');
insert into asp_migration_spa Values
(13,'TWC','commercial_business','8260','1600','All');
insert into asp_migration_spa Values
(13,'TWC','commercial_business','8260','1700','All');
insert into asp_migration_spa Values
(13,'TWC','commercial_business','8260','1800','All');

insert into asp_migration_spa Values
(14,'TWC','Residential','8260','1300','All');
insert into asp_migration_spa Values
(14,'TWC','Residential','8260','1400','All');
insert into asp_migration_spa Values
(14,'TWC','Residential','8260','1600','All');
insert into asp_migration_spa Values
(14,'TWC','Residential','8260','1700','All');
insert into asp_migration_spa Values
(14,'TWC','Residential','8260','1800','All');

insert into asp_migration_spa Values
(15,'TWC','commercial_business','8448','2000','All');
insert into asp_migration_spa Values
(15,'TWC','commercial_business','8448','4000','All');
insert into asp_migration_spa Values
(15,'TWC','commercial_business','8448','4100','All');
insert into asp_migration_spa Values
(15,'TWC','commercial_business','8448','4200','All');
insert into asp_migration_spa Values
(15,'TWC','commercial_business','8448','6000','All');
insert into asp_migration_spa Values
(15,'TWC','commercial_business','8448','6100','All');
insert into asp_migration_spa Values
(15,'TWC','commercial_business','8448','6200','All');

insert into asp_migration_spa Values
(16,'TWC','commercial_business','8109','1000','All');

insert into asp_migration_spa Values
(17,'TWC','Residential','8109','1000','All');
insert into asp_migration_spa Values
(18,'TWC','Residential','8150','All','All');
insert into asp_migration_spa Values
(19,'TWC','commercial_business','8150','All','All');

insert into asp_migration_spa Values
(20,'TWC','Residential','8448','2000','All');
insert into asp_migration_spa Values
(20,'TWC','Residential','8448','4000','All');
insert into asp_migration_spa Values
(20,'TWC','Residential','8448','4100','All');
insert into asp_migration_spa Values
(20,'TWC','Residential','8448','4200','All');
insert into asp_migration_spa Values
(20,'TWC','Residential','8448','6000','All');
insert into asp_migration_spa Values
(20,'TWC','Residential','8448','6100','All');
insert into asp_migration_spa Values
(20,'TWC','Residential','8448','6200','All');

insert into asp_migration_spa Values
(21,'TWC','Residential','1','All','All');
insert into asp_migration_spa Values
(21,'TWC','Residential','2','All','All');

insert into asp_migration_spa Values
(22,'TWC','commercial_business','1','All','All');
insert into asp_migration_spa Values
(22,'TWC','commercial_business','2','All','All');
insert into asp_migration_spa Values
(22,'TWC','commercial_business','3','All','All');
insert into asp_migration_spa Values
(22,'TWC','commercial_business','4','All','All');
insert into asp_migration_spa Values
(22,'TWC','commercial_business','202','All','All');

insert into asp_migration_spa Values
(23,'BHN','Residential','All','All','All');

insert into asp_migration_spa Values
(24,'TWC','Residential','8448','3000','All');

insert into asp_migration_spa Values
(25,'TWC','commercial_business','8448','3000','All');

insert into asp_migration_spa Values
(26,'TWC','Residential','3','All','All');
insert into asp_migration_spa Values
(26,'TWC','Residential','4','All','All');

insert into asp_migration_spa Values
(27,'TWC','Residential','202','All','All');

insert into asp_migration_spa Values
(28,'BHN','commercial_business','All','All','All');

drop VIEW IF EXISTS asp_v_migration_wave;
create VIEW IF NOT EXISTS asp_v_migration_wave
AS
SELECT a.*,
       b.wstart as wstart,
       b.wend as wend
  from asp_migration_spa a,
       asp_migration_date b
 WHERE  a.wid = b.wid
 ORDER by a.wid;

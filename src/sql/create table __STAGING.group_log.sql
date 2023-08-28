CREATE TABLE STV230547__STAGING.group_log
(
    id  IDENTITY ,
    group_id int NOT NULL,
    user_id int NOT NULL,
    user_id_from int,
    event varchar(50),
    "datetime" timestamp
)
PARTITION BY ((group_log."datetime")::date) GROUP BY (CASE WHEN ("datediff"('year', (group_log."datetime")::date, ((now())::timestamptz(6))::date) >= 2) THEN (date_trunc('year', (group_log."datetime")::date))::date WHEN ("datediff"('month', (group_log."datetime")::date, ((now())::timestamptz(6))::date) >= 3) THEN (date_trunc('month', (group_log."datetime")::date))::date ELSE (group_log."datetime")::date END);


ALTER TABLE STV230547__STAGING.group_log ADD CONSTRAINT C_FOREIGN FOREIGN KEY (group_id) references STV230547.groups (id);
ALTER TABLE STV230547__STAGING.group_log ADD CONSTRAINT C_FOREIGN_1 FOREIGN KEY (user_id) references STV230547.users (id);

CREATE PROJECTION STV230547__STAGING.group_log /*+createtype(P)*/ 
(
 id,
 group_id,
 user_id,
 user_id_from,
 event,
 "datetime"
)
AS
 SELECT group_log.id,
        group_log.group_id,
        group_log.user_id,
        group_log.user_id_from,
        group_log.event,
        group_log."datetime"
 FROM STV230547__STAGING.group_log
 ORDER BY group_log.id,
          group_log.group_id,
          group_log.user_id
SEGMENTED BY hash(group_log.id) ALL NODES KSAFE 1;


SELECT MARK_DESIGN_KSAFE(1);
create table t2(
  id serial primary key
);

---- create above / drop below ----

drop table IF EXISTS t2;

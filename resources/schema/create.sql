create table location (
  id varchar(255) not null,
  x float not null,
  y float not null,
  z float not null,
  primary key (id)
) ;

create table infrastructure_type (
  id varchar(255) not null,
  description varchar(255) default null,
  name varchar(255) default null,
  primary key (id)
) ;

create table infrastructure (
  name varchar(255) default null,
  infrastructure_type_id varchar(255) default null,
  id varchar(255) not null,
  floor integer not null,
  primary key (id),
  foreign key (infrastructure_type_id) references infrastructure_type (id)
) ;

create table infrastructure_location (
  location_id varchar(255) not null,
  infrastructure_id varchar(255) not null,
  primary key(location_id, infrastructure_id),
  foreign key (location_id) references location (id),
  foreign key (infrastructure_id) references infrastructure (id)
) ;

create table platform_type (
  id varchar(255) not null,
  description varchar(255) default null,
  name varchar(255) default null unique,
  primary key (id)
) ;

create table users (
  email varchar(255) default null unique,
  google_auth_token varchar(255) default null,
  name varchar(255) default null,
  id varchar(255) not null,
  primary key (id)
 ) ;

create table user_group (
  id varchar(255) not null,
  description varchar(255) default null,
  name varchar(255) default null,
  primary key (id)
) ;

create table user_group_membership (
  user_id varchar(255) not null,
  user_group_id varchar(255) not null,
  primary key (user_group_id, user_id),
  foreign key (user_id) references users (id),
  foreign key (user_group_id) references user_group (id)
) ;

create table platform (
  id varchar(255) not null,
  name varchar(255) default null,
  user_id varchar(255) default null,
  platform_type_id varchar(255) default null,
  hashed_mac varchar(255) default null,
  primary key (id),
  foreign key (user_id) references users (id),
  foreign key (platform_type_id) references platform_type (id)
) ;

create table sensor_type (
  id varchar(255) not null,
  description varchar(255) default null,
  mobility varchar(255) default null,
  name varchar(255) default null,
  capture_functionality varchar(255) default null,
  payload_schema varchar(255),
  primary key (id)
) ;

create table sensor (
  id varchar(255) not null,
  name varchar(255) default null,
  infrastructure_id varchar(255) default null,
  user_id varchar(255) default null,
  sensor_type_id varchar(255) default null,
  sensor_config varchar(255) default null,
  primary key (id),
  foreign key (sensor_type_id) references sensor_type (id),
  foreign key (infrastructure_id) references infrastructure (id),
  foreign key (user_id) references users (id)
) ;

create table coverage_infrastructure (
  sensor_id varchar(255) not null,
  infrastructure_id varchar(255) not null,
  primary key (infrastructure_id, sensor_id),
  foreign key (infrastructure_id) references infrastructure (id),
  foreign key (sensor_id) references sensor (id)
) ;

create table wemoobservation (
  id varchar(255) not null,
  currentmilliwatts integer default null,
  ontodayseconds integer default null,
  timestamp timestamp not null,
  sensor_id varchar(255) default null,
  primary key (id),
  foreign key (sensor_id) references sensor (id)
) ;

create table wifiapobservation (
  id varchar(255) not null,
  clientid varchar(255) default null,
  timestamp timestamp not null,
  sensor_id varchar(255) default null,
  primary key (id),
  foreign key (sensor_id) references sensor (id)
) ;

create table thermometerobservation (
  id varchar(255) not null,
  temperature integer default null,
  timestamp timestamp not null,
  sensor_id varchar(255) default null,
  primary key (id),
  foreign key (sensor_id) references sensor (id)
) ;

create table semantic_observation_type (
  id varchar(255) not null,
  description varchar(255) default null,
  name varchar(255) default null,
  primary key (id)
) ;

create table virtual_sensor_type (
  id varchar(255) not null,
  name varchar(255) default null,
  description varchar(255) default null,
  input_type_id varchar(255) default null,
  semantic_observation_type_id varchar(255) default null,
  primary key (id),
  foreign key (input_type_id) references sensor_type (id),
  foreign key (semantic_observation_type_id) references semantic_observation_type (id)
) ;

create table virtual_sensor (
  id varchar(255) not null,
  name varchar(255) default null,
  description varchar(255) default null,
  language varchar(255) default null,
  project_name varchar(255) default null,
  type_id varchar(255) default null,
  primary key (id),
  foreign key (type_id) references virtual_sensor_type (id)
) ;

create table occupancy (
  id varchar(255) not null,
  semantic_entity_id varchar(255) not null,
  occupancy integer default null,
  timestamp timestamp not null,
  virtual_sensor_id varchar(255) default null,
  primary key (id),
  foreign key (virtual_sensor_id) references virtual_sensor (id),
  foreign key (semantic_entity_id) references infrastructure (id)
) ;

create table presence (
  id varchar(255) not null,
  semantic_entity_id varchar(255) not null,
  location varchar(255) default null,
  timestamp timestamp not null,
  virtual_sensor_id varchar(255) default null,
  primary key (id),
  foreign key (virtual_sensor_id) references virtual_sensor (id),
  foreign key (semantic_entity_id) references users (id)
) ;

create index wifi_timestamp_idx on wifiapobservation(timestamp);
create index wemo_timestamp_idx on wemoobservation(timestamp);
create index temp_timestamp_idx on thermometerobservation(timestamp);
create index presence_timestamp_idx on presence(timestamp);
create index occupancy_timestamp_idx on occupancy(timestamp);

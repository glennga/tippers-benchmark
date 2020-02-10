CREATE TABLE location
(
    ID varchar(255) NOT NULL,
    X  float        NOT NULL,
    Y  float        NOT NULL,
    Z  float        NOT NULL,
    PRIMARY KEY (ID)
);

CREATE TABLE infrastructure_type
(
    ID          varchar(255) NOT NULL,
    DESCRIPTION varchar(255) DEFAULT NULL,
    NAME        varchar(255) DEFAULT NULL,
    PRIMARY KEY (ID)
);

CREATE TABLE infrastructure
(
    NAME                   varchar(255) DEFAULT NULL,
    INFRASTRUCTURE_TYPE_ID varchar(255) DEFAULT NULL,
    ID                     varchar(255) NOT NULL,
    FLOOR                  integer      NOT NULL,
    PRIMARY KEY (ID),
    FOREIGN KEY (INFRASTRUCTURE_TYPE_ID) REFERENCES infrastructure_type (ID)
);

CREATE TABLE infrastructure_location
(
    LOCATION_ID       varchar(255) NOT NULL,
    INFRASTRUCTURE_ID varchar(255) NOT NULL,
    PRIMARY KEY (LOCATION_ID, INFRASTRUCTURE_ID),
    FOREIGN KEY (LOCATION_ID) REFERENCES location (ID),
    FOREIGN KEY (INFRASTRUCTURE_ID) REFERENCES infrastructure (ID)
);

CREATE TABLE platform_type
(
    ID          varchar(255) NOT NULL,
    DESCRIPTION varchar(255) DEFAULT NULL,
    NAME        varchar(255) DEFAULT NULL UNIQUE,
    PRIMARY KEY (ID)
);

CREATE TABLE users
(
    EMAIL             varchar(255) DEFAULT NULL UNIQUE,
    GOOGLE_AUTH_TOKEN varchar(255) DEFAULT NULL,
    NAME              varchar(255) DEFAULT NULL,
    ID                varchar(255) NOT NULL,
    PRIMARY KEY (ID)
);

CREATE TABLE user_group
(
    ID          varchar(255) NOT NULL,
    DESCRIPTION varchar(255) DEFAULT NULL,
    NAME        varchar(255) DEFAULT NULL,
    PRIMARY KEY (ID)
);

CREATE TABLE user_group_membership
(
    USER_ID       varchar(255) NOT NULL,
    USER_GROUP_ID varchar(255) NOT NULL,
    PRIMARY KEY (USER_GROUP_ID, USER_ID),
    FOREIGN KEY (USER_ID) REFERENCES users (ID),
    FOREIGN KEY (USER_GROUP_ID) REFERENCES user_group (ID)
);

CREATE TABLE platform
(
    ID               varchar(255) NOT NULL,
    NAME             varchar(255) DEFAULT NULL,
    USER_ID          varchar(255) DEFAULT NULL,
    PLATFORM_TYPE_ID varchar(255) DEFAULT NULL,
    HASHED_MAC       varchar(255) DEFAULT NULL,
    PRIMARY KEY (ID),
    FOREIGN KEY (USER_ID) REFERENCES users (ID),
    FOREIGN KEY (PLATFORM_TYPE_ID) REFERENCES platform_type (ID)
);

CREATE TABLE sensor_type
(
    ID                    varchar(255) NOT NULL,
    DESCRIPTION           varchar(255) DEFAULT NULL,
    MOBILITY              varchar(255) DEFAULT NULL,
    NAME                  varchar(255) DEFAULT NULL,
    CAPTURE_FUNCTIONALITY varchar(255) DEFAULT NULL,
    PAYLOAD_SCHEMA        varchar(255),
    PRIMARY KEY (ID)
);

CREATE TABLE sensor
(
    ID                varchar(255) NOT NULL,
    NAME              varchar(255) DEFAULT NULL,
    INFRASTRUCTURE_ID varchar(255) DEFAULT NULL,
    USER_ID           varchar(255) DEFAULT NULL,
    SENSOR_TYPE_ID    varchar(255) DEFAULT NULL,
    SENSOR_CONFIG     varchar(255) DEFAULT NULL,
    PRIMARY KEY (ID),
    FOREIGN KEY (SENSOR_TYPE_ID) REFERENCES sensor_type (ID),
    FOREIGN KEY (INFRASTRUCTURE_ID) REFERENCES infrastructure (ID),
    FOREIGN KEY (USER_ID) REFERENCES users (ID)
);

CREATE TABLE coverage_infrastructure
(
    SENSOR_ID         varchar(255) NOT NULL,
    INFRASTRUCTURE_ID varchar(255) NOT NULL,
    PRIMARY KEY (INFRASTRUCTURE_ID, SENSOR_ID),
    FOREIGN KEY (INFRASTRUCTURE_ID) REFERENCES infrastructure (ID),
    FOREIGN KEY (SENSOR_ID) REFERENCES sensor (ID)
);

CREATE TABLE wemoobservation
(
    id                varchar(255) NOT NULL,
    currentMilliWatts integer      DEFAULT NULL,
    onTodaySeconds    integer      DEFAULT NULL,
    timeStamp         timestamp    NOT NULL,
    sensor_id         varchar(255) DEFAULT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (sensor_id) REFERENCES sensor (ID)
);

CREATE TABLE wifiapobservation
(
    id        varchar(255) NOT NULL,
    clientId  varchar(255) DEFAULT NULL,
    timeStamp timestamp    NOT NULL,
    sensor_id varchar(255) DEFAULT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (sensor_id) REFERENCES sensor (ID)
);

CREATE TABLE thermometerobservation
(
    id          varchar(255) NOT NULL,
    temperature integer      DEFAULT NULL,
    timeStamp   timestamp    NOT NULL,
    sensor_id   varchar(255) DEFAULT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (sensor_id) REFERENCES sensor (ID)
);

CREATE TABLE semantic_observation_type
(
    ID          varchar(255) NOT NULL,
    DESCRIPTION varchar(255) DEFAULT NULL,
    NAME        varchar(255) DEFAULT NULL,
    PRIMARY KEY (ID)
);

CREATE TABLE virtual_sensor_type
(
    ID                           varchar(255) NOT NULL,
    NAME                         varchar(255) DEFAULT NULL,
    DESCRIPTION                  varchar(255) DEFAULT NULL,
    INPUT_TYPE_ID                varchar(255) DEFAULT NULL,
    SEMANTIC_OBSERVATION_TYPE_ID varchar(255) DEFAULT NULL,
    PRIMARY KEY (ID),
    FOREIGN KEY (INPUT_TYPE_ID) REFERENCES sensor_type (ID),
    FOREIGN KEY (SEMANTIC_OBSERVATION_TYPE_ID) REFERENCES semantic_observation_type (ID)
);

CREATE TABLE virtual_sensor
(
    ID           varchar(255) NOT NULL,
    NAME         varchar(255) DEFAULT NULL,
    DESCRIPTION  varchar(255) DEFAULT NULL,
    LANGUAGE     varchar(255) DEFAULT NULL,
    PROJECT_NAME varchar(255) DEFAULT NULL,
    TYPE_ID      varchar(255) DEFAULT NULL,
    PRIMARY KEY (ID),
    FOREIGN KEY (TYPE_ID) REFERENCES virtual_sensor_type (ID)
);

CREATE TABLE occupancy
(
    id                 varchar(255) NOT NULL,
    semantic_entity_id varchar(255) NOT NULL,
    occupancy          integer      DEFAULT NULL,
    timeStamp          timestamp    NOT NULL,
    virtual_sensor_id  varchar(255) DEFAULT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (virtual_sensor_id) REFERENCES virtual_sensor (ID),
    FOREIGN KEY (semantic_entity_id) REFERENCES infrastructure (ID)
);

CREATE TABLE presence
(
    id                 varchar(255) NOT NULL,
    semantic_entity_id varchar(255) NOT NULL,
    location           varchar(255) DEFAULT NULL,
    timeStamp          timestamp    NOT NULL,
    virtual_sensor_id  varchar(255) DEFAULT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (virtual_sensor_id) REFERENCES virtual_sensor (ID),
    FOREIGN KEY (semantic_entity_id) REFERENCES users (ID)
);

CREATE INDEX wifi_timestamp_idx ON wifiapobservation (timeStamp);
CREATE INDEX wemo_timestamp_idx ON wemoobservation (timeStamp);
CREATE INDEX temp_timestamp_idx ON thermometerobservation (timeStamp);

CREATE INDEX presence_timestamp_idx ON presence (timeStamp);
CREATE INDEX occupancy_timestamp_idx ON occupancy (timeStamp);
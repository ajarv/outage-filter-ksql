## Prerequisites

* Unix like system
* kafkacat installed -  sudo apt-get install kafkacat
* ksql cli - get it from https://www.confluent.io/product/ksql/
* clone this repo to a working folder e.g `~/workspace/outage-filter-ksql`

## Send Few  events 

```bash
BROKER_URL='aplmuled002:29092' 
KSQL_URL='http://aplmuled002:2092'


echo 'e9100000| {"seqNo": 1, "meterESN": "e9100000", "eventType": "POWERON"}' | \
    kafkacat -b ${BROKER_URL} -t sm_event -P -K"|"  
echo 'e9100000|{"meterESN": "e9100000", "transformerId": "t-001","eventType":"JOIN"}' | \
    kafkacat -b ${BROKER_URL} -t registerEvent -P -K"|"  

```

## Receive 1 event

```bash
kafkacat -b ${BROKER_URL} -t sm_event -C  -c 1


```

## send multiple events from input file

```bash
#multiple reg
cat ./feed/f-o-reg.txt |kafkacat -b ${BROKER_URL} -t registerEvent -P -K"|"  

#multiple poweroff
cat ./feed/f-o-poff.txt |kafkacat -b ${BROKER_URL} -t sm_event -P -K"|"  

#multiple poweron
cat ./feed/f-o-pon.txt |kafkacat -b ${BROKER_URL} -t sm_event -P -K"|"  

```


## KSQL Stream from topics

Run KSQL CLI
```bash

ksql ${KSQL_URL}

```

```sql
-- setting for getting all messages in a kstream useful for testing
SET 'auto.offset.reset'='earliest';


CREATE STREAM SMEVENT01 ( \
  SEQNO BIGINT, \
  METERESN VARCHAR, \
  EVENTTYPE VARCHAR ) \
WITH (KAFKA_TOPIC='sm_event',VALUE_FORMAT='JSON') ;

-- read some events from SMEVENT01 stream
SELECT * FROM SMEVENT01;
--<

CREATE STREAM SMEVENT11 WITH (PARTITIONS = 1) AS SELECT * FROM SMEVENT01;

--ok
CREATE TABLE MTR_REG (createdTime BIGINT, METERESN VARCHAR, transformerId VARCHAR) WITH \
(kafka_topic='registerEvent', value_format='JSON', key = 'METERESN');
--<


--ok
CREATE TABLE MTR_STATE (createdTime BIGINT, METERESN VARCHAR, eventType VARCHAR) WITH \
(kafka_topic='sm_event', value_format='JSON', key = 'METERESN');
--<

--ok
CREATE STREAM SMEVENT_W_TRANSFORMER WITH (PARTITIONS = 1) AS \
SELECT T1.SEQNO,T1.METERESN,T1.EVENTTYPE,T2.TRANSFORMERID \
FROM SMEVENT01  T1 \
LEFT JOIN MTR_REG  T2 \
ON T1.METERESN = T2.METERESN  ;
-- enriched stream


-- ok
SELECT T1.METERESN , T1.EVENTTYPE,T1.SEQNO \
FROM SMEVENT01  T1 \
LEFT JOIN SMEVENT11  T2 \
WITHIN (0 SECONDS,5 SECONDS) \
ON T1.METERESN = T2.METERESN  \
WHERE T2.METERESN IS NULL;
--<  Remove duplicates within 5 SEC


--Remove duplicates
SELECT T1.ROWTIME, T1.METERESN , T1.EVENTTYPE ,T2.EVENTTYPE \
FROM SMEVENT01  T1 \
LEFT JOIN SMEVENT11  T2 \
WITHIN 5 SECONDS \
ON T1.METERESN = T2.METERESN  \
WHERE ( (T2.SEQNO IS NULL) OR ( T2.EVENTTYPE != T1.EVENTTYPE ) ) ;
-- ok no event - new event
-- ok new event - same event   in 5 seconds
-- multiple  event 1 - event 2 - event 1 -- causes multiple
--<


-- Select  outages by transformer ID in 300 second window.
--ok
CREATE TABLE OUTAGE_MAP_SESSION_300 AS \
SELECT TRANSFORMERID, \
        HISTOGRAM(T1_METERESN) AS METER_MAP \
FROM SMEVENT_W_TRANSFORMER \
WINDOW SESSION (300 second) \
WHERE EVENTTYPE = 'POWEROFF' \
GROUP BY TRANSFORMERID;
--< OUTAGES


-- Output of OUTAGE_MAP_SESSION_300 is  outage events

```
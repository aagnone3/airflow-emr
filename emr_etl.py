#!/usr/bin/env python
# coding: utf-8
import sys
import logging
import boto3
from pyspark.sql import (
    SparkSession,
    functions as F,
    types as T
)

S3_BUCKET = "agnone"
LOG_FN = "app.log"
logging.basicConfig(level=logging.INFO, filename=LOG_FN)
LOG = logging.getLogger(__name__)


def main():
    LOG.info('Begin execution')
    spark = SparkSession.builder.appName('FlightDelaysETL').getOrCreate()
    datahub_airports_schema = T.StructType([
        T.StructField("continent", T.StringType(), True),
        T.StructField("coordinates", T.StringType(), True),
        T.StructField("elevation_ft", T.FloatType(), True),
        T.StructField("gps_code", T.StringType(), True),
        T.StructField("iata_code", T.StringType(), True),
        T.StructField("ident", T.StringType(), True),
        T.StructField("iso_country", T.StringType(), True),
        T.StructField("iso_region", T.StringType(), True),
        T.StructField("local_code", T.StringType(), True),
        T.StructField("municipality", T.StringType(), True),
        T.StructField("name", T.StringType(), True),
        T.StructField("type", T.StringType(), True)
    ])

    datahub_airports = (
        spark
        .read
        .format('json')
        .schema(datahub_airports_schema)
        .load('s3://{}/dend/airport-codes.json'.format(S3_BUCKET))
    )

    LOG.info('# datahub airport entries: %d', datahub_airports.count())

    scsg_schema = T.StructType([
        T.StructField('iata', T.StringType(), True),
        T.StructField('airport', T.StringType(), True),
        T.StructField('city', T.StringType(), True),
        T.StructField('state', T.StringType(), True),
        T.StructField('country', T.StringType(), True),
        T.StructField('lat', T.FloatType(), True),
        T.StructField('long', T.FloatType(), True)
    ])
    scsg_airports = (
        spark
        .read
        .format('csv')
        .schema(scsg_schema)
        .option('header', True)
        .load('s3://{}/dend/scsg-airports.csv'.format(S3_BUCKET))
    )
    LOG.info('# scsg airport entries: %d', scsg_airports.count())

    scsg_airports.limit(5).show()

    # join the two airport-related tables on the IATA code
    airports = (
        scsg_airports
        .join(datahub_airports, scsg_airports.iata == datahub_airports.iata_code, 'right')
        .drop('airport', 'iata_code', 'gps_code')
    )

    (
        airports
        .write
        .mode('overwrite')
        .parquet('s3://{}/dend/pq_mart/airports'(S3_BUCKET))
    )
    LOG.info('# merged airport entries: %d', airports.count())

    delays_schema = T.StructType([
        T.StructField("Year", T.IntegerType(), True),
        T.StructField("Month", T.IntegerType(), True),
        T.StructField("DayofMonth", T.IntegerType(), True),
        T.StructField("DayOfWeek", T.IntegerType(), True),
        T.StructField("DepTime", T.IntegerType(), True),
        T.StructField("CRSDepTime", T.IntegerType(), True),
        T.StructField("ArrTime", T.IntegerType(), True),
        T.StructField("CRSArrTime", T.IntegerType(), True),
        T.StructField("UniqueCarrier", T.StringType(), True),
        T.StructField("FlightNum", T.StringType(), True),
        T.StructField("TailNum", T.StringType(), True),
        T.StructField("ActualElapsedTime", T.StringType(), True),
        T.StructField("CRSElapsedTime", T.StringType(), True),
        T.StructField("AirTime", T.IntegerType(), True),
        T.StructField("ArrDelay", T.IntegerType(), True),
        T.StructField("DepDelay", T.IntegerType(), True),
        T.StructField("Origin", T.StringType(), True),
        T.StructField("Dest", T.StringType(), True),
        T.StructField("Distance", T.IntegerType(), True),
        T.StructField("TaxiIn", T.IntegerType(), True),
        T.StructField("TaxiOut", T.IntegerType(), True),
        T.StructField("Cancelled", T.IntegerType(), True),
        T.StructField("CancellationCode", T.IntegerType(), True),
        T.StructField("Diverted", T.IntegerType(), True),
        T.StructField("CarrierDelay", T.IntegerType(), True),
        T.StructField("WeatherDelay", T.IntegerType(), True),
        T.StructField("NASDelay", T.IntegerType(), True),
        T.StructField("SecurityDelay", T.IntegerType(), True),
        T.StructField("LateAircraftDelay", T.IntegerType(), True)
    ])

    delays = (
        spark
        .read
        .format('csv')
        .schema(delays_schema)
        .option('header', True)
        .option('nullValue', 'NA')
        .load('s3://{}/dend/flights/1988*'.format(S3_BUCKET))
        .withColumnRenamed('DayofMonth', 'DayOfMonth')
    )
    LOG.info('# flight delay entries: %d', delays.count())

    # - delays: calculate total delay
    # - join two airport-related tables on the iata code

    delay_columns = [
        'ArrDelay',
        'DepDelay',
        'TaxiIn',
        'TaxiOut',
        'CarrierDelay',
        'WeatherDelay',
        'NASDelay',
        'SecurityDelay',
        'LateAircraftDelay'
    ]

    delays = (
        delays
        .fillna(0, subset=delay_columns)
        .withColumn('TotalDelay', sum([F.col(col) for col in delay_columns]))
    )

    # join in the `iso_region` field from the dimension table, so that the downstream business users can
    # derive results on how the flight delays relate to the flight region
    delays = (
        delays
        .join(
            airports.select('iata', 'iso_region'),
            delays.Origin == airports.iata,
            'left'
        )
        .withColumnRenamed('iso_region', 'OriginRegion')
        .drop('iata')
        .join(
            airports.select('iata', 'iso_region'),
            delays.Dest == airports.iata,
            'left'
        )
        .withColumnRenamed('iso_region', 'DestRegion')
        .drop('iata')
    )


if __name__ == '__main__':
    rc = 0
    try:
        main()
    except Exception as exc:
        LOG.error(exc, exc_info=True)
        rc = 1
    finally:
        boto3.resource('s3').Bucket(S3_BUCKET).upload_file(LOG_FN, LOG_FN)
        sys.exit(rc)

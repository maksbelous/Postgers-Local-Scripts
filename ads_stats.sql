
--In case if database already exists and there are open sessions for the database, firstly we must close these processes and only then drop the DB.
-- * At the same time we hvae to make unabale to connect to the DB. Lets do this. */

--Make unabale to connect to the DB. HAS_DB stands for - Household Applience Store Database
SET ROLE postgres;
UPDATE pg_database SET datallowconn = 'false' WHERE lower(datname) = 'vizor_db'

--closing out existing sessions
SELECT pg_terminate_backend(pg_stat_activity.pid)
FROM pg_stat_activity
WHERE lower(pg_stat_activity.datname) = 'vizor_db' AND pid <> pg_backend_pid();

--drop DB
DROP DATABASE IF EXISTS vizor_db;


/*DB creation with default settings. */
CREATE DATABASE vizor_db
	WITH 
	OWNER = postgres
	ENCODING = 'UTF-8'
	TABLESPACE = pg_default
	CONNECTION LIMIT = -1;

/*Schema creation.
 * schema name - test.  */
--DROP SCHEMA BEFORE creation
DROP SCHEMA IF EXISTS test CASCADE;

CREATE SCHEMA IF NOT EXISTS test;

--setting search pass for the new DB to user postgres. it`s more handy to use it
ALTER ROLE postgres IN DATABASE vizor_db SET search_path TO test;

--verifying that previous statement worked
SELECT r.rolname, d.datname, rs.setconfig
FROM   pg_db_role_setting rs
LEFT   JOIN pg_roles      r ON r.oid = rs.setrole
LEFT   JOIN pg_database   d ON d.oid = rs.setdatabase
WHERE  upper(r.rolname) = 'POSTGRES' OR upper(d.datname) = 'VIZOR_DB';

--Tables creation step

--customer table creation
CREATE TABLE IF NOT EXISTS test.cost_data(
	install_date 		DATE,
	media_source		VARCHAR(200),
	platform			VARCHAR(200)	,
	ad_account_id		BIGINT,
	campaign_id			BIGINT,
	campaign_name		VARCHAR(200),
	spend				FLOAT,
	impressions			FLOAT,
	clicks				FLOAT);

ALTER TABLE IF EXISTS test.cost_data OWNER TO postgres;

--Check creations
ALTER TABLE IF EXISTS test.cost_data
	ADD CONSTRAINT cost_data_date CHECK (install_date <= current_date);--CHECK that install date not in the future

CREATE TABLE IF NOT EXISTS test.mpp_data(
	attributed_touch_type VARCHAR(255),
    install_time TIMESTAMP,
    event_time TIMESTAMP,
    event_name VARCHAR(200),
    media_source VARCHAR(200),
    campaign_name VARCHAR(200),
    campaign_id BIGINT,
    country_code VARCHAR(10),
    user_id VARCHAR(200),
    platform VARCHAR(50),
    event_revenue_currency VARCHAR(10),
    event_revenue NUMERIC(10, 2),
    event_revenue_usd NUMERIC(10, 2),
    cohort_day INTEGER);

ALTER TABLE IF EXISTS test.mpp_data OWNER TO postgres;   

--Check creations
ALTER TABLE IF EXISTS test.mpp_data
	ADD CONSTRAINT install_data_date CHECK (install_time <= current_date);--CHECK that install date not in the future

--uploading data into the tables
COPY test.cost_data FROM 'C:\vizor\New\cost_data.csv' WITH (FORMAT CSV, HEADER true);

COPY test.mpp_data FROM 'C:\vizor\New\mmp_data.csv'  WITH (FORMAT CSV, HEADER true);


--view creation to use it in Tableau
CREATE OR REPLACE VIEW test.daily_ads_stats_view as
WITH mpp_aggregated AS (--CTE TO AGGREGATE MPP data
SELECT DATE(install_time) AS install_date,
		campaign_id ,
		platform ,
		count(DISTINCT CASE WHEN lower(event_name) = 'install' THEN user_id ELSE NULL END ) AS installs, --DISTINCT TO EXCLUDE users who instaleld app several times
		count(DISTINCT CASE WHEN lower(event_name) = 'af_purchase' THEN user_id  ELSE NULL END) AS buyers,
		count(DISTINCT CASE WHEN cohort_day = 0 AND event_name = lower('af_purchase') THEN user_id ELSE NULL END) AS buyers_day0,
		count(DISTINCT CASE WHEN cohort_day = 1 AND event_name = lower('af_purchase') THEN user_id ELSE NULL END) AS buyers_day1,
		count(DISTINCT CASE WHEN cohort_day = 3 AND event_name = lower('af_purchase') THEN user_id ELSE NULL END) AS buyers_day3,
		count(DISTINCT CASE WHEN cohort_day = 5 AND event_name = lower('af_purchase') THEN user_id ELSE NULL END) AS buyers_day5,
		sum(event_revenue_usd) AS gross_revenue,
		sum(CASE WHEN cohort_day = 0 THEN event_revenue_usd ELSE NULL END) AS gross_revenue_day0,
		sum(CASE WHEN cohort_day = 1 THEN event_revenue_usd ELSE NULL END) AS gross_revenue_day1,
		sum(CASE WHEN cohort_day = 3 THEN event_revenue_usd ELSE NULL END) AS gross_revenue_day3,
		sum(CASE WHEN cohort_day = 5 THEN event_revenue_usd ELSE NULL END) AS gross_revenue_day5,
		--cumulitive revenue in the first 5 days
		sum(CASE WHEN cohort_day = 0 THEN event_revenue_usd ELSE NULL END) AS revenue_day0,
		sum(CASE WHEN cohort_day <= 1 THEN event_revenue_usd ELSE NULL END) AS revenue_day1,
		sum(CASE WHEN cohort_day <= 3 THEN event_revenue_usd ELSE NULL END) AS revenue_day3,
		sum(CASE WHEN cohort_day <= 5 THEN event_revenue_usd ELSE NULL END) AS revenue_day5
FROM test.mpp_data md
GROUP BY DATE(install_time),
		campaign_id ,
		platform)
SELECT cd.install_date ,
	   cd.media_source ,
	   cd.platform ,
	   cd.ad_account_id ,
	   cd.campaign_id ,
	   cd.campaign_name ,
	   cd.spend,
	   cd.impressions,
	   cd.clicks,
	   ma.installs,
	   ma.buyers,
	   ma.buyers_day0,
	   ma.buyers_day1,
	   ma.buyers_day3,
	   ma.buyers_day5,
	   ma.gross_revenue,
	   ma.gross_revenue_day0,
	   ma.gross_revenue_day1,
	   ma.gross_revenue_day3,
	   ma.gross_revenue_day5,
	   ma.revenue_day0,
	   ma.revenue_day1,
	   ma.revenue_day3,
	   ma.revenue_day5
FROM test.cost_data cd 
LEFT JOIN mpp_aggregated ma ON cd.install_date = ma.install_date AND 
							   cd.campaign_id = ma.campaign_id AND 
							   cd.platform = ma.platform;
	

							 
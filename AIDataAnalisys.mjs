// This file runs on my server every hour via cron, importing the latest data from data.json into a SQLite database and computing daily/hourly aggregates for ML features.
// For Reference only


import Database from "better-sqlite3";
import { DateTime } from "luxon";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

// -----------------------------
// Setup __dirname
// -----------------------------
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// -----------------------------
// Load raw data
// -----------------------------
const dataPath = path.resolve(__dirname, "../data.json");
const raw = JSON.parse(fs.readFileSync(dataPath, "utf-8"));
console.log("Raw devices loaded:", raw.length);

// -----------------------------
// Open database
// -----------------------------
const db = new Database("weatherAI.db");

// Enable WAL for better performance
db.pragma("journal_mode = WAL");

// -----------------------------
// Create tables
// -----------------------------

// Raw readings — one row per poll per sensor location
db.prepare(`
  CREATE TABLE IF NOT EXISTS readings (
    timestamp         TEXT NOT NULL,
    mac_address       TEXT NOT NULL,

    -- Outdoor (main sensor)
    tempf             REAL,
    humidity          REAL,
    feels_like        REAL,
    dew_point         REAL,
    windspeedmph      REAL,
    windgustmph       REAL,
    maxdailygust      REAL,
    winddir           REAL,
    winddir_avg10m    REAL,
    uv                REAL,
    solar_radiation   REAL,
    hourlyrainin      REAL,
    dailyrainin       REAL,
    weeklyrainin      REAL,
    monthlyrainin     REAL,
    yearlyrainin      REAL,
    baromrelin        REAL,
    baromabsin        REAL,
    aqi_pm25          REAL,
    aqi_pm25_24h      REAL,
    lightning_day     INTEGER,
    lightning_hour    INTEGER,
    last_rain         TEXT,

    -- Remote sensor (temp1f / humidity1)
    temp1f            REAL,
    humidity1         REAL,
    feels_like1       REAL,
    dew_point1        REAL,

    -- Indoor sensor
    tempinf           REAL,
    humidityin        REAL,
    feels_like_in     REAL,
    dew_point_in      REAL,

    -- Battery flags
    battout           INTEGER,
    battin            INTEGER,
    batt1             INTEGER,
    batt_lightning    INTEGER,
    batt_co2          INTEGER,

    PRIMARY KEY (timestamp, mac_address)
  )
`).run();

// Daily aggregates per device — min/max/avg for every numeric channel
db.prepare(`
  CREATE TABLE IF NOT EXISTS daily_summary (
    date              TEXT NOT NULL,
    mac_address       TEXT NOT NULL,

    -- Outdoor temp
    temp_min          REAL,
    temp_max          REAL,
    temp_avg          REAL,

    -- Remote temp
    temp1_min         REAL,
    temp1_max         REAL,
    temp1_avg         REAL,

    -- Indoor temp
    tempin_min        REAL,
    tempin_max        REAL,
    tempin_avg        REAL,

    -- Humidity
    humidity_min      REAL,
    humidity_max      REAL,
    humidity_avg      REAL,
    humidity1_avg     REAL,
    humidityin_avg    REAL,

    -- Wind
    windspeed_avg     REAL,
    windgust_max      REAL,
    maxdailygust      REAL,

    -- Rain
    dailyrainin       REAL,   -- last value of the day (cumulative within day)
    hourlyrainin_max  REAL,

    -- Pressure
    baromrel_avg      REAL,
    baromabs_avg      REAL,

    -- UV / Solar
    uv_max            REAL,
    solar_max         REAL,
    solar_avg         REAL,

    -- Air quality
    aqi_pm25_avg      REAL,
    aqi_pm25_24h_avg  REAL,

    -- Lightning
    lightning_day_max INTEGER,

    -- Chilling hours (32–45 °F main outdoor sensor)
    chill_hours       REAL,

    samples           INTEGER,

    PRIMARY KEY (date, mac_address)
  )
`).run();

// Hourly aggregates — useful for time-series ML features
db.prepare(`
  CREATE TABLE IF NOT EXISTS hourly_summary (
    hour              TEXT NOT NULL,   -- "YYYY-MM-DD HH"
    mac_address       TEXT NOT NULL,
    temp_avg          REAL,
    temp1_avg         REAL,
    tempin_avg        REAL,
    humidity_avg      REAL,
    windspeed_avg     REAL,
    windgust_max      REAL,
    solar_avg         REAL,
    aqi_pm25_avg      REAL,
    rain_total        REAL,
    samples           INTEGER,
    PRIMARY KEY (hour, mac_address)
  )
`).run();

// Cumulative chilling hours per winter season
db.prepare(`
  CREATE TABLE IF NOT EXISTS cumulative_chill (
    season_year       TEXT NOT NULL,
    mac_address       TEXT NOT NULL,
    cumulative_hours  REAL NOT NULL,
    PRIMARY KEY (season_year, mac_address)
  )
`).run();

// -----------------------------
// Prepared statements
// -----------------------------
const insertReading = db.prepare(`
  INSERT OR IGNORE INTO readings (
    timestamp, mac_address,
    tempf, humidity, feels_like, dew_point,
    windspeedmph, windgustmph, maxdailygust, winddir, winddir_avg10m,
    uv, solar_radiation,
    hourlyrainin, dailyrainin, weeklyrainin, monthlyrainin, yearlyrainin,
    baromrelin, baromabsin,
    aqi_pm25, aqi_pm25_24h,
    lightning_day, lightning_hour, last_rain,
    temp1f, humidity1, feels_like1, dew_point1,
    tempinf, humidityin, feels_like_in, dew_point_in,
    battout, battin, batt1, batt_lightning, batt_co2
  ) VALUES (
    ?, ?,
    ?, ?, ?, ?,
    ?, ?, ?, ?, ?,
    ?, ?,
    ?, ?, ?, ?, ?,
    ?, ?,
    ?, ?,
    ?, ?, ?,
    ?, ?, ?, ?,
    ?, ?, ?, ?,
    ?, ?, ?, ?, ?
  )
`);

const insertDaily = db.prepare(`
  INSERT OR REPLACE INTO daily_summary (
    date, mac_address,
    temp_min, temp_max, temp_avg,
    temp1_min, temp1_max, temp1_avg,
    tempin_min, tempin_max, tempin_avg,
    humidity_min, humidity_max, humidity_avg, humidity1_avg, humidityin_avg,
    windspeed_avg, windgust_max, maxdailygust,
    dailyrainin, hourlyrainin_max,
    baromrel_avg, baromabs_avg,
    uv_max, solar_max, solar_avg,
    aqi_pm25_avg, aqi_pm25_24h_avg,
    lightning_day_max,
    chill_hours,
    samples
  ) VALUES (
    ?, ?,
    ?, ?, ?,
    ?, ?, ?,
    ?, ?, ?,
    ?, ?, ?, ?, ?,
    ?, ?, ?,
    ?, ?,
    ?, ?,
    ?, ?, ?,
    ?, ?,
    ?,
    ?,
    ?
  )
`);

const insertHourly = db.prepare(`
  INSERT OR REPLACE INTO hourly_summary (
    hour, mac_address,
    temp_avg, temp1_avg, tempin_avg,
    humidity_avg, windspeed_avg, windgust_max,
    solar_avg, aqi_pm25_avg, rain_total, samples
  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

const insertCumulative = db.prepare(`
  INSERT OR REPLACE INTO cumulative_chill (season_year, mac_address, cumulative_hours)
  VALUES (?, ?, ?)
`);

// -----------------------------
// Helper: safe number
// -----------------------------
const n = v => {
  const x = Number(v);
  return Number.isNaN(x) ? null : x;
};

// -----------------------------
// Insert current readings
// -----------------------------
let totalInserted = 0;

for (const device of raw) {
  const d = device.lastData;
  if (!d) continue;

  const mac = device.macAddress;

  // Use dateutc if available, otherwise current UTC
  const timestamp = d.dateutc
    ? DateTime.fromMillis(d.dateutc, { zone: "utc" }).toISO()
    : DateTime.utc().toISO();

  const info = insertReading.run(
    timestamp, mac,
    n(d.tempf), n(d.humidity), n(d.feelsLike), n(d.dewPoint),
    n(d.windspeedmph), n(d.windgustmph), n(d.maxdailygust), n(d.winddir), n(d.winddir_avg10m),
    n(d.uv), n(d.solarradiation),
    n(d.hourlyrainin), n(d.dailyrainin), n(d.weeklyrainin), n(d.monthlyrainin), n(d.yearlyrainin),
    n(d.baromrelin), n(d.baromabsin),
    n(d.aqi_pm25), n(d.aqi_pm25_24h),
    n(d.lightning_day), n(d.lightning_hour), d.lastRain ?? null,
    n(d.temp1f), n(d.humidity1), n(d.feelsLike1), n(d.dewPoint1),
    n(d.tempinf), n(d.humidityin), n(d.feelsLikein), n(d.dewPointin),
    n(d.battout), n(d.battin), n(d.batt1), n(d.batt_lightning), n(d.batt_co2)
  );

  if (info.changes > 0) {
    console.log(`Inserted: ${timestamp} | ${mac} | outdoor:${d.tempf}°F remote:${d.temp1f}°F`);
    totalInserted++;
  }
}

console.log(`Total readings inserted: ${totalInserted}`);

// -----------------------------
// Daily aggregates
// -----------------------------
const dailyRows = db.prepare(`
  SELECT
    substr(timestamp, 1, 10)          AS date,
    mac_address,
    MIN(tempf)                        AS temp_min,
    MAX(tempf)                        AS temp_max,
    AVG(tempf)                        AS temp_avg,
    MIN(temp1f)                       AS temp1_min,
    MAX(temp1f)                       AS temp1_max,
    AVG(temp1f)                       AS temp1_avg,
    MIN(tempinf)                      AS tempin_min,
    MAX(tempinf)                      AS tempin_max,
    AVG(tempinf)                      AS tempin_avg,
    MIN(humidity)                     AS humidity_min,
    MAX(humidity)                     AS humidity_max,
    AVG(humidity)                     AS humidity_avg,
    AVG(humidity1)                    AS humidity1_avg,
    AVG(humidityin)                   AS humidityin_avg,
    AVG(windspeedmph)                 AS windspeed_avg,
    MAX(windgustmph)                  AS windgust_max,
    MAX(maxdailygust)                 AS maxdailygust,
    MAX(dailyrainin)                  AS dailyrainin,
    MAX(hourlyrainin)                 AS hourlyrainin_max,
    AVG(baromrelin)                   AS baromrel_avg,
    AVG(baromabsin)                   AS baromabs_avg,
    MAX(uv)                           AS uv_max,
    MAX(solar_radiation)              AS solar_max,
    AVG(solar_radiation)              AS solar_avg,
    AVG(aqi_pm25)                     AS aqi_pm25_avg,
    AVG(aqi_pm25_24h)                 AS aqi_pm25_24h_avg,
    MAX(lightning_day)                AS lightning_day_max,
    SUM(CASE WHEN tempf BETWEEN 32 AND 45 THEN 0.25 ELSE 0 END) AS chill_hours,
    COUNT(*)                          AS samples
  FROM readings
  GROUP BY date, mac_address
`).all();

db.transaction(rows => {
  for (const r of rows) {
    const fix = v => v != null ? Number(v.toFixed(3)) : null;
    insertDaily.run(
      r.date, r.mac_address,
      fix(r.temp_min), fix(r.temp_max), fix(r.temp_avg),
      fix(r.temp1_min), fix(r.temp1_max), fix(r.temp1_avg),
      fix(r.tempin_min), fix(r.tempin_max), fix(r.tempin_avg),
      fix(r.humidity_min), fix(r.humidity_max), fix(r.humidity_avg), fix(r.humidity1_avg), fix(r.humidityin_avg),
      fix(r.windspeed_avg), fix(r.windgust_max), fix(r.maxdailygust),
      fix(r.dailyrainin), fix(r.hourlyrainin_max),
      fix(r.baromrel_avg), fix(r.baromabs_avg),
      fix(r.uv_max), fix(r.solar_max), fix(r.solar_avg),
      fix(r.aqi_pm25_avg), fix(r.aqi_pm25_24h_avg),
      r.lightning_day_max,
      fix(r.chill_hours),
      r.samples
    );
    console.log(`Daily: ${r.date} | ${r.mac_address} | temp ${r.temp_min?.toFixed(1)}–${r.temp_max?.toFixed(1)}°F | rain:${r.dailyrainin}" | aqi:${r.aqi_pm25_avg?.toFixed(0)}`);
  }
})(dailyRows);

// -----------------------------
// Hourly aggregates
// -----------------------------
const hourlyRows = db.prepare(`
  SELECT
    substr(timestamp, 1, 13)   AS hour,
    mac_address,
    AVG(tempf)                 AS temp_avg,
    AVG(temp1f)                AS temp1_avg,
    AVG(tempinf)               AS tempin_avg,
    AVG(humidity)              AS humidity_avg,
    AVG(windspeedmph)          AS windspeed_avg,
    MAX(windgustmph)           AS windgust_max,
    AVG(solar_radiation)       AS solar_avg,
    AVG(aqi_pm25)              AS aqi_pm25_avg,
    MAX(hourlyrainin)          AS rain_total,
    COUNT(*)                   AS samples
  FROM readings
  GROUP BY hour, mac_address
`).all();

db.transaction(rows => {
  for (const r of rows) {
    const fix = v => v != null ? Number(v.toFixed(3)) : null;
    insertHourly.run(
      r.hour, r.mac_address,
      fix(r.temp_avg), fix(r.temp1_avg), fix(r.tempin_avg),
      fix(r.humidity_avg), fix(r.windspeed_avg), fix(r.windgust_max),
      fix(r.solar_avg), fix(r.aqi_pm25_avg), fix(r.rain_total), r.samples
    );
  }
  console.log(`Hourly aggregates updated: ${rows.length} rows`);
})(hourlyRows);

// -----------------------------
// Cumulative chilling hours per winter season
// -----------------------------
const cumRows = db.prepare(`
  SELECT
    mac_address,
    CASE
      WHEN CAST(strftime('%m', date) AS INTEGER) >= 11
        THEN strftime('%Y', date) || '-' || (CAST(strftime('%Y', date) AS INTEGER)+1)
      ELSE
        (CAST(strftime('%Y', date) AS INTEGER)-1) || '-' || strftime('%Y', date)
    END AS season_year,
    SUM(chill_hours) AS cumulative_hours
  FROM daily_summary
  WHERE CAST(strftime('%m', date) AS INTEGER) IN (11,12,1,2,3)
  GROUP BY season_year, mac_address
`).all();

db.transaction(rows => {
  for (const r of rows) {
    insertCumulative.run(r.season_year, r.mac_address, Number(r.cumulative_hours.toFixed(2)));
    console.log(`Chill season ${r.season_year} | ${r.mac_address} | ${r.cumulative_hours.toFixed(1)}h`);
  }
})(cumRows);

// -----------------------------
// Close DB
// -----------------------------
db.close();
console.log("Import complete ✔");
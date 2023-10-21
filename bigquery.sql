-- timestamp converted to date in  sleepDay_merged table
CREATE OR REPLACE VIEW `bellabeat-gda-capstone-fardin.bellabeat.sleepDayView` AS
SELECT *, DATE(SleepDay) AS SleepDate
FROM `bellabeat-gda-capstone-fardin.bellabeat.sleepDay_merged`;

-- consistent date formatting in weightLogInfo_merged table
CREATE OR REPLACE VIEW `bellabeat-gda-capstone-fardin.bellabeat.weightLogInfoView` AS
SELECT
  Id,
  CASE
    WHEN REGEXP_CONTAINS(Date, r'\d{4}-\d{2}-\d{2}') THEN Date
    WHEN REGEXP_CONTAINS(Date, r'\d{1,2}/\d{1,2}/\d{4}') THEN FORMAT_TIMESTAMP('%Y-%m-%d', PARSE_TIMESTAMP('%m/%d/%Y', Date))
    WHEN REGEXP_CONTAINS(Date, r'\d{1,2} \d{1,2} \d{4}') THEN FORMAT_TIMESTAMP('%Y-%m-%d', PARSE_TIMESTAMP('%m %d %Y', Date))
    ELSE NULL
  END AS fDate,
  Time,  WeightKg, WeightPounds, Fat, BMI, IsManualReport, LogId
FROM
  `bellabeat-gda-capstone-fardin.bellabeat.weightLogInfo_merged`;

-- check for duplicates in each table
SELECT
  Id,
  ActivityDate,
  TotalSteps,
  TotalDistance,
  TrackerDistance,
  LoggedActivitiesDistance,
  VeryActiveDistance,
  ModeratelyActiveDistance,
  LightActiveDistance,
  SedentaryActiveDistance,
  VeryActiveMinutes,
  FairlyActiveMinutes,
  LightlyActiveMinutes,
  SedentaryMinutes,
  Calories,
  COUNT(*) AS DuplicateCount
FROM
  `bellabeat-gda-capstone-fardin.bellabeat.dailyActivity_merged`
GROUP BY
  Id, 
  ActivityDate, 
  TotalSteps, 
  TotalDistance, 
  TrackerDistance, 
  LoggedActivitiesDistance, 
  VeryActiveDistance, 
  ModeratelyActiveDistance, 
  LightActiveDistance, 
  SedentaryActiveDistance, 
  VeryActiveMinutes, 
  FairlyActiveMinutes, 
  LightlyActiveMinutes, 
  SedentaryMinutes, 
  Calories
HAVING
  COUNT(*) > 1;

SELECT
  Id,
  SleepDate,
  TotalSleepRecords,
  TotalMinutesAsleep,
  TotalTimeInBed,
  COUNT(*) AS DuplicateCount
FROM
  `bellabeat-gda-capstone-fardin.bellabeat.sleepDayView`
GROUP BY
  Id, SleepDate, TotalSleepRecords, TotalMinutesAsleep, TotalTimeInBed
HAVING
  COUNT(*) > 1; -- 3 duplicate entries found

SELECT
  Id,
  fDate,
  WeightKg,
  WeightPounds,
  Fat,
  BMI,
  IsManualReport,
  LogId,
  COUNT(*) AS DuplicateCount
FROM
  `bellabeat-gda-capstone-fardin.bellabeat.weightLogInfoView`
GROUP BY
  Id, fDate, WeightKg, WeightPounds, Fat, BMI, IsManualReport, LogId
HAVING
  COUNT(*) > 1;

-- Now remove the duplicates from sleepDayView
CREATE OR REPLACE VIEW `bellabeat-gda-capstone-fardin.bellabeat.sleepDay` AS
SELECT
  Id,
  SleepDate,
  TotalSleepRecords,
  TotalMinutesAsleep,
  TotalTimeInBed
FROM (
  SELECT
    Id,
    SleepDate,
    TotalSleepRecords,
    TotalMinutesAsleep,
    TotalTimeInBed,
    ROW_NUMBER() OVER (PARTITION BY Id, SleepDate, TotalSleepRecords, TotalMinutesAsleep, TotalTimeInBed) AS RowNumber
  FROM
    `bellabeat-gda-capstone-fardin.bellabeat.sleepDayView`
) AS sleepDay
WHERE RowNumber = 1;

SELECT
  COUNT(*) AS TotalRows,
  SUM(IF(Id IS NULL, 1, 0)) AS MissingId,
  SUM(IF(ActivityDate IS NULL, 1, 0)) AS MissingActivityDate,
  SUM(IF(TotalSteps IS NULL, 1, 0)) AS MissingTotalSteps,
  SUM(IF(TotalDistance IS NULL, 1, 0)) AS MissingTotalDistance,
  SUM(IF(TrackerDistance IS NULL, 1, 0)) AS MissingTrackerDistanc,
  SUM(IF(LoggedActivitiesDistance IS NULL, 1, 0)) AS MissingLoggedActivitiesDistance,
  SUM(IF(ModeratelyActiveDistance IS NULL, 1, 0)) AS MissingModeratelyActiveDistance,
  SUM(IF(LightActiveDistance IS NULL, 1, 0)) AS MissingLightActiveDistance,
  SUM(IF(SedentaryActiveDistance IS NULL, 1, 0)) AS MissingSedentaryActiveDistance,
  SUM(IF(VeryActiveMinutes IS NULL, 1, 0)) AS MissingVeryActiveMinutes,
  SUM(IF(FairlyActiveMinutes IS NULL, 1, 0)) AS MissingFairlyActiveMinutes,
  SUM(IF(LightlyActiveMinutes IS NULL, 1, 0)) AS MissingLightlyActiveMinutes,
  SUM(IF(SedentaryMinutes IS NULL, 1, 0)) AS MissingSedentaryMinutes,
  SUM(IF(Calories IS NULL, 1, 0)) AS MissingCalories
FROM `bellabeat-gda-capstone-fardin.bellabeat.dailyActivity_merged`;

SELECT
  COUNT(*) AS TotalRows,
  SUM(IF(Id IS NULL, 1, 0)) AS MissingId,
  SUM(IF(SleepDate IS NULL, 1, 0)) AS MissingSleepDate,
  SUM(IF(TotalSleepRecords IS NULL, 1, 0)) AS MissingTotalSleepRecords,
  SUM(IF(TotalMinutesAsleep IS NULL, 1, 0)) AS MissingTotalMinutesAsleep,
  SUM(IF(TotalTimeInBed IS NULL, 1, 0)) AS MissingTotalTimeInBed
FROM
  `bellabeat-gda-capstone-fardin.bellabeat.sleepDay`;

SELECT
  COUNT(*) AS TotalRows,
  SUM(IF(Id IS NULL, 1, 0)) AS MissingId,
  SUM(IF(fDate IS NULL, 1, 0)) AS MissingfDate,
  SUM(IF(WeightKg IS NULL, 1, 0)) AS MissingWeightKg,
  SUM(IF(WeightPounds IS NULL, 1, 0)) AS MissingWeightPounds,
  SUM(IF(Fat IS NULL, 1, 0)) AS MissingFat,
  SUM(IF(BMI IS NULL, 1, 0)) AS MissingBMI,
  SUM(IF(IsManualReport IS NULL, 1, 0)) AS MissingIsManualReport,
  SUM(IF(LogId IS NULL, 1, 0)) AS MissingLogId
FROM
  `bellabeat-gda-capstone-fardin.bellabeat.weightLogInfoView`;
-- among 67 rows, 65 is missing data in "Fat" column. so we need to exclude it in furthur analysis.

CREATE OR REPLACE TABLE `bellabeat-gda-capstone-fardin.bellabeat.dailyActivitySleep` AS
SELECT
  da.Id AS Id,
  da.ActivityDate AS ActivityDate,
  da.TotalSteps AS TotalSteps,
  da.TotalDistance AS TotalDistance,
  da.TrackerDistance AS TrackerDistance,
  da.LoggedActivitiesDistance AS LoggedActivitiesDistance,
  da.VeryActiveDistance AS VeryActiveDistance,
  da.ModeratelyActiveDistance AS ModeratelyActiveDistance,
  da.LightActiveDistance AS LightActiveDistance,
  da.SedentaryActiveDistance AS SedentaryActiveDistance,
  da.VeryActiveMinutes AS VeryActiveMinutes,
  da.FairlyActiveMinutes AS FairlyActiveMinutes,
  da.LightlyActiveMinutes AS LightlyActiveMinutes,
  da.SedentaryMinutes AS SedentaryMinutes,
  da.Calories AS Calories,
  sd.TotalSleepRecords AS TotalSleepRecords,
  sd.TotalMinutesAsleep AS TotalMinutesAsleep,
  sd.TotalTimeInBed AS TotalTimeInBed
FROM
  `bellabeat-gda-capstone-fardin.bellabeat.dailyActivity_merged` AS da
INNER JOIN
  `bellabeat-gda-capstone-fardin.bellabeat.sleepDay` AS sd
ON
  da.Id = sd.Id AND da.ActivityDate = sd.SleepDate;


-- Check for row counts for sleepDay and new dailyActivitySleep
SELECT
  'dailyActivitySleep' AS table_name,
  COUNT(*) AS row_count
FROM
  `bellabeat-gda-capstone-fardin.bellabeat.dailyActivitySleep`

UNION ALL

SELECT
  'sleepDay' AS table_name,
  COUNT(*) AS row_count
FROM
  `bellabeat-gda-capstone-fardin.bellabeat.sleepDay`; -- for both table the row count is the same which is 410

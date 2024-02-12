/* In order to run this script, log into psql with the command: "psql -U postgres" then run it with "\i andrea-laiena-sql-analysis.sql" */

-- The following line prints the output in a file, for improved readability. Comment it out if you desire standard in-terminal output
-- Note: the "output" folder must exist in the project folder.
\o output/output_file.txt

--/* SECTION 1, Setup - comment this section if you want to run the analysis section multiple times after Setup

CREATE DATABASE world_and_energy_al; -- By Andrea Laiena :D

-- connects to the database we just created
\c world_and_energy_al

--Since the output is in an external file, and I experienced inconsistency in the times required for the printout, we give the user some in-terminal feedback during the process
\echo Step 1: Creating and populating the tables...

CREATE TABLE IF NOT EXISTS sustainable_energy (
    country VARCHAR(255), -- name of the country
    report_year NUMERIC(4), --the year the report (row) refers to
    access_to_electricity NUMERIC(9,6), -- % of population with access to electricity
    access_to_clean_fuels_for_cooking NUMERIC(9,6), --% of population with access to clean fuels for cooking
    renewable_elctricity_generation REAL, --renewable electricity generating capacity, expressed in watts per capita
    financial_flows_to_developing_countries BIGINT, --International finance received for clean energy, expressed in USD (NB the datatype MONEY was giving problems, I reverted it to BIGINT)
    renewable_energy_share NUMERIC(9,6), --% Share of renewable energy in the total energy consumption
    electricity_from_fossil_fuels REAL, -- Fossil fuels electricity (TWh) Terawatt Hour
    electricity_from_nuclear REAL, -- Nuclear electricity (TWh) Terawatt Hour
    electricity_from_renewables REAL, --Renewable electricity (TWh) Terawatt Hour
    low_carbon_electricity NUMERIC(9,6), -- % Low carbon electricity
    primary_energy_consumption NUMERIC(13,6), --Per capita primary energy consumption (kWh/person)
    energy_intensity REAL, --Energy supplied to the economy per unit value of economic output. (FIXME: Documentation states the measure unit is (MJ/$2017 PPP GDP), it sounds wrong)
    co2_emissions NUMERIC(14,6), --CO2 emissions in kiloton (kt)
    percentage_primary_energy_from_renewables NUMERIC(9,6), -- percentage of equivalent primary energy that is derived from renewable sources
    gdp_growth NUMERIC(12,9), -- Gross Domestic Product yearly growth
    gdp_per_capita NUMERIC(12,6), -- Gross Domestic Product per capita
    pop_density SMALLINT, -- Population density measured in persons per square kilometer (P/Km^2)
    land_area INT, -- Total land area of the country in square kilometers (Km^2)
    latitude NUMERIC(9,6), -- Latitude of the country
    longitude NUMERIC(9,6), -- Longitude of the country
    PRIMARY KEY (country, report_year)
);

--Let's import the data (from .csv) excluding the header
\copy sustainable_energy FROM 'data/cleaned/global-data-on-sustainable-energy-C.csv' WITH (FORMAT CSV, HEADER true)

CREATE TABLE IF NOT EXISTS world_2023(
    country VARCHAR(255), -- Name of the country
    pop_density SMALLINT, -- Population density measured in persons per square kilometer (P/Km^2)
    abbreviation CHAR(2), -- Abbreviation or code representing the country
    agricultural_land NUMERIC(4,2), -- Percentage of land area used for agricultural purposes
    land_area INT, -- Total land area of the country in square kilometers (Km^2)
    armed_forces_size INT, -- Size of the armed forces in the country
    birth_rate NUMERIC(4,2), -- Number of births per 1,000 population per year
    calling_code NUMERIC(4), -- International calling code for the country
    capital_or_major_city VARCHAR(255), -- Name of the capital or major city
    co2_emissions INT, -- Carbon dioxide emissions in tons
    cpi NUMERIC(6,2), -- Consumer Price Index, a measure of inflation and purchasing power
    cpi_change NUMERIC(5,2), -- Percentage change in the Consumer Price Index compared to the previous year
    currency_code CHAR(3), -- Currency code used in the country
    fertility_rate NUMERIC(3,2), -- Average number of children born to a woman during her lifetime
    forested_area NUMERIC(4,2), -- Percentage of land area covered by forests
    gasoline_price NUMERIC(3,2), -- Price of gasoline per liter in USD
    gdp BIGINT, -- Gross Domestic Product, the total value of goods and services produced in the country
    gross_primary_education_enrollment NUMERIC(5,2), -- Gross enrollment ratio for primary education
    gross_tertiary_education_enrollment NUMERIC(5,2), -- Gross enrollment ratio for tertiary education
    infant_mortality NUMERIC(4,1), -- Number of deaths per 1,000 live births before reaching one year of age
    largest_city VARCHAR(255), -- Name of the country's largest city
    life_expectancy NUMERIC (3,1), -- Average number of years a newborn is expected to live
    maternal_mortality_ratio SMALLINT, -- Number of maternal deaths per 100,000 live births
    minimum_wage NUMERIC(4,2), -- Minimum wage level in local currency (NB,FIXME: The dataset documentation states "in local currency" but the data (in the raw dataset) reports the value in $. I think it is expressed/converted in USD)
    official_language VARCHAR(255), -- Official language(s) spoken in the country
    out_of_pocket_health_expenditure NUMERIC(5,2), -- Percentage of total health expenditure paid out-of-pocket by individuals
    physicians_per_thousand NUMERIC(4,2), -- Number of physicians per thousand people
    population BIGINT, -- Total population of the country
    labor_force_participation NUMERIC(5,2), -- Percentage of the population that is part of the labor force
    tax_revenue NUMERIC(4,2), -- Tax revenue as a percentage of GDP
    total_tax_rate NUMERIC(5,2), -- Overall tax burden as a percentage of commercial profits
    unemployment_rate NUMERIC(4,2), -- Percentage of the labor force that is unemployed
    urban_population INT, -- Percentage of the population living in urban areas
    latitude NUMERIC(9,6), -- Latitude coordinate of the country's location
    longitude NUMERIC(9,6) -- Longitude coordinate of the country's location
);

--Let's import the data (from .csv) excluding the header
\copy world_2023 FROM 'data/cleaned/world-data-2023-C.csv' WITH (FORMAT CSV, HEADER true)

--Let's add the report_year row to the table world_2023
ALTER TABLE world_2023
ADD report_year NUMERIC(4);

--Let's set all the fields of report_year(in the world_2023 table) to 2023
UPDATE world_2023
SET report_year = 2023;

--Let's add the primary key to the table world_2023
ALTER TABLE world_2023
ADD CONSTRAINT world_2023_pkey PRIMARY KEY (country, report_year);

/*Views Creation */

\echo Step 2: Creating the views...

CREATE VIEW full_data AS
    SELECT 
        COALESCE(se.country, w23.country) AS country,
        COALESCE(se.report_year, w23.report_year) AS report_year,
        COALESCE(se.co2_emissions, w23.co2_emissions) AS co2_emissions,

        se.access_to_electricity AS access_to_electricity,
        se.access_to_clean_fuels_for_cooking AS access_to_clean_fuels_for_cooking,
        se.renewable_elctricity_generation AS renewable_elctricity_generation,
        se.financial_flows_to_developing_countries AS financial_flows_to_developing_countries,
        se.renewable_energy_share AS renewable_energy_share,
        se.electricity_from_fossil_fuels AS electricity_from_fossil_fuels,
        se.electricity_from_nuclear AS electricity_from_nuclear,
        se.electricity_from_renewables AS electricity_from_renewables,
        se.low_carbon_electricity AS low_carbon_electricity,
        se.primary_energy_consumption AS primary_energy_consumption,
        se.energy_intensity AS energy_intensity,
        se.percentage_primary_energy_from_renewables AS percentage_primary_energy_from_renewables,
        se.gdp_growth AS gdp_growth,
        se.gdp_per_capita AS gdp_per_capita,

        w23.abbreviation AS abbreviation,
        w23.agricultural_land AS agricultural_land,
        w23.armed_forces_size AS armed_forces_size,
        w23.birth_rate AS birth_rate,
        w23.calling_code AS calling_code,
        w23.capital_or_major_city AS capital_or_major_city,
        w23.cpi AS cpi,
        w23.cpi_change AS cpi_change,
        w23.currency_code AS currency_code,
        w23.fertility_rate AS fertility_rate,
        w23.forested_area AS forested_area,
        w23.gasoline_price AS gasoline_price,
        w23.gdp AS gdp,
        w23.gross_primary_education_enrollment AS gross_primary_education_enrollment,
        w23.gross_tertiary_education_enrollment AS gross_tertiary_education_enrollment,
        w23.infant_mortality AS infant_mortality,
        w23.largest_city AS largest_city,
        w23.life_expectancy AS life_expectancy,
        w23.maternal_mortality_ratio AS maternal_mortality_ratio,
        w23.minimum_wage AS minimum_wage, 
        w23.official_language AS official_language,
        w23.out_of_pocket_health_expenditure AS out_of_pocket_health_expenditure,
        w23.physicians_per_thousand AS physicians_per_thousand,
        w23.labor_force_participation AS labor_force_participation,
        w23.tax_revenue AS tax_revenue,
        w23.total_tax_rate AS total_tax_rate,
        w23.unemployment_rate AS unemployment_rate,
        w23.urban_population AS urban_population,

        COALESCE((se.pop_density * se.land_area), w23.population) AS population, -- we calculate the population for the se table also, adding the result to the population column
        COALESCE(se.pop_density, w23.pop_density) AS pop_density,
        COALESCE(se.land_area, w23.land_area) AS land_area,
        COALESCE(se.latitude, w23.latitude) AS latitude,
        COALESCE(se.longitude, w23.longitude) AS longitude
    FROM sustainable_energy se
    FULL OUTER JOIN world_2023 w23 
    ON se.country = w23.country
    AND se.report_year = w23.report_year
    ORDER BY country, report_year;

--SECTION 1, Setup - END */

/* SECTION 2, Analysis */

\echo Step 3: Performing analysis...

--check tables and views
SELECT * FROM sustainable_energy LIMIT(25);
SELECT * FROM world_2023 LIMIT(25);
SELECT * FROM full_data LIMIT(50);

-- example select data from a single country
SELECT * FROM full_data WHERE country = 'Italy';

-- list of all available countries
SELECT DISTINCT country FROM full_data;


-- world's total (all countries combined) primary energy consumption, per year 
SELECT
    report_year,
    SUM(primary_energy_consumption) AS total_primary_energy_consumption
FROM
    sustainable_energy
GROUP BY
    report_year
ORDER BY
    report_year;

-- world's total (all countries combined) CO2 emissions, per year
SELECT
    report_year,
    SUM(co2_emissions) AS total_co2_emissions
FROM
    full_data
GROUP BY
    report_year
ORDER BY
    report_year;

-- countries that have reduced their emissions since 2000 (at 2023 report date)
SELECT 
    country --our result will include only the countries names
FROM 
    full_data --we use the view full_data
WHERE 
    report_year = 2000 OR report_year = 2023 --we select only the 2 years we are comparing
GROUP BY 
    country --we group the result by country
HAVING -- this clause says: if the sum of co2_emissions in 2020 and the negative of co2_emissions in 2023 is > 0, then include the result in the result set
        SUM(CASE 
            WHEN report_year = 2000 THEN co2_emissions 
            WHEN report_year = 2023 THEN -co2_emissions 
        END) > 0; 


/* Analysis: How(if) international financial flows influenced development indexes */
-- Big query ahead, get prepared!, we will navigate it together
WITH funding_per_country AS ( --First CTE to calculate and extract the total international funding received for country through the years between 2000 and 2019
    SELECT 
        country, 
        SUM(financial_flows_to_developing_countries) AS total_international_funding
    FROM
        sustainable_energy
    WHERE 
        financial_flows_to_developing_countries IS NOT NULL --we use this to select only the countries who received funding
        AND report_year BETWEEN 2000 AND 2019
    GROUP BY 
        country
),
major_indexes_percentage_change AS ( -- Second CTE, used to calculate the percent change of selected indexes
    SELECT 
        country,
        --the change is calculated as (end value - initial value) since the values are already expressed in percentage
        (end_access_to_electricity - start_access_to_electricity) AS access_to_electricity_percentage_change,
        (end_access_to_clean_fuels_for_cooking - start_access_to_clean_fuels_for_cooking) AS access_to_clean_fuels_for_cooking_percentage_change,
        (end_renewable_energy_share - start_renewable_energy_share) AS renewable_energy_share_percentage_change
    FROM 
        (
            SELECT 
                country,
                --we use the MAX() function to extract the only value that's resulting from the expression.
                MAX(CASE WHEN report_year = 2000 THEN access_to_electricity END) AS start_access_to_electricity,
                MAX(CASE WHEN report_year = 2019 THEN access_to_electricity END) AS end_access_to_electricity,
                MAX(CASE WHEN report_year = 2000 THEN access_to_clean_fuels_for_cooking END) AS start_access_to_clean_fuels_for_cooking,
                MAX(CASE WHEN report_year = 2019 THEN access_to_clean_fuels_for_cooking END) AS end_access_to_clean_fuels_for_cooking,
                MAX(CASE WHEN report_year = 2000 THEN renewable_energy_share END) AS start_renewable_energy_share,
                MAX(CASE WHEN report_year = 2019 THEN renewable_energy_share END) AS end_renewable_energy_share
            FROM 
                sustainable_energy
            WHERE 
                report_year IN (2000, 2019) --we take in consideration only the entries for 2000 and 2019
                AND financial_flows_to_developing_countries IS NOT NULL -- only for the countries that have financial flows
            GROUP BY 
                country
        )
)
SELECT 
    --fpc and mipc aliases are created in the FROM and JOIN clauses below
    fpc.country,
    mipc.access_to_electricity_percentage_change,
    mipc.access_to_clean_fuels_for_cooking_percentage_change,
    mipc.renewable_energy_share_percentage_change,
    fpc.total_international_funding
FROM 
    funding_per_country AS fpc
JOIN 
    major_indexes_percentage_change AS mipc ON fpc.country = mipc.country
ORDER BY fpc.total_international_funding DESC; --it can be changed to see the data ordered by each of the columns

/* Note: there's a cap on the mipc data dependent on the fact that a country may be reached 100% electricity access, or 100% access to clean fuels for cooking.
It may make sense to compare/join this table with the final value of these variables (like access_to_electricity_2019, etc...) or a reached_100% BOOL column,
or calculate the virtuosity of a country in a different way.
*/

/*Analysis p.2*/

-- total births per country in 2023
SELECT
    country,
    ROUND((birth_rate*population)/1000) AS total_births_2023
FROM
    world_2023
ORDER BY
    total_births_2023 DESC;

-- correlation between co2 emissions and fertility rate
SELECT CORR(co2_emissions, fertility_rate) AS co2_emissions_and_fertility_rate_correlation_coefficient
FROM world_2023;

-- correlation between gasoline price and co2 emissions
SELECT CORR(gasoline_price, co2_emissions) AS gasoline_price_and_co2_emissions_correlation_coefficient
FROM world_2023;

-- correlation between gross_primary_education_enrollment, gross_tertiary_education_enrollment, and gdp/population (gdp per capita).
SELECT 
    CORR(gross_primary_education_enrollment, gdp/population) AS primary_education_and_gdp_correlation_coefficient
FROM world_2023;

SELECT
    CORR(gross_tertiary_education_enrollment, gdp/population) AS tertiary_education_and_gdp_correlation_coefficient
FROM world_2023;


--Let's find out which are the most used official languages in the world
SELECT
    official_language, COUNT(*) AS countries_adopting_this_language
FROM
    world_2023
WHERE
    official_language IS NOT NULL
GROUP BY
    official_language
ORDER BY 
    countries_adopting_this_language DESC;

\echo Final Step: Printing output to file, it may take a while...

-- Thanks ;)


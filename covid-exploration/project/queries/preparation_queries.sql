---- DATA PREPARATION
---
-- Remove empty values for NULL
BEGIN;

UPDATE covid_deaths
SET continent = NULL
WHERE continent = '';

COMMIT;

-- Change data types
BEGIN;

ALTER TABLE covid_deaths
    ALTER COLUMN date TYPE DATE 
    USING to_date(date, 'DD/MM/YYYY');
	ALTER COLUMN total_cases TYPE BIGINT
		USING (total_cases::BIGINT),
	ALTER COLUMN new_cases TYPE INT
		USING (new_cases::INT),
	ALTER COLUMN total_deaths TYPE INT
		USING (total_deaths::INT),
	ALTER COLUMN population TYPE BIGINT
		USING (population::BIGINT);

COMMIT;

---- VIEW CREATION
---
-- Create view for percentage of population vaccinated
BEGIN;

CREATE VIEW PercentPopulationVaccinatedFr AS
    SELECT
        dea.continent,
        dea.location,
        dea.date,
        dea.population,
        vac.new_vaccinations,
        SUM(vac.new_vaccinations) OVER (
            PARTITION BY dea.location
            ORDER BY dea.location, dea.date
        ) AS rolling_vaccination
    FROM covid_deaths dea
    JOIN covid_vaccinations vac
        ON dea.location = vac.location
        AND dea.date = vac.date
    WHERE
        dea.continent IS NOT NULL
        AND dea.location = 'France'

COMMIT;

---- QUERIES
---
-- Looking at Total Cases vs Total Deaths
SELECT location, date, total_cases, total_deaths, ROUND((total_deaths::decimal / total_cases) * 100, 2) AS DeathPercentage
FROM covid_deaths
WHERE location = 'France'
ORDER BY 1, 2;

-- Looking at Total Cases vs Population
SELECT location, date, total_cases, population, ROUND((total_cases::decimal / population) * 100, 2) AS InfectionPercentage
FROM covid_deaths
WHERE location LIKE '%States'
ORDER BY 1, 2;

-- Countries with highest infection rate compared to population
SELECT
	location,
	population,
	MAX(total_cases) AS highestInfectionCount,
	ROUND(MAX((total_cases::decimal / population)) * 100, 2) AS populationInfectedPercentage
FROM covid_deaths
GROUP BY location, population
ORDER BY 4 DESC;

-- Countries with highest death count per population by location
SELECT
	location,
	population,
	MAX(total_deaths) AS total_death_count,
	ROUND(MAX((total_deaths::decimal / population)) * 100, 2) AS population_death_pct
FROM covid_deaths
WHERE continent IS NOT NULL AND total_deaths IS NOT NULL
GROUP BY location, population
ORDER BY 3 DESC;

-- Countries with highest death count per population by continent
SELECT
	continent,
	MAX(total_deaths) AS total_death_count,
	ROUND(MAX((total_deaths::decimal / population)) * 100, 2) AS population_death_pct
FROM covid_deaths
WHERE
	(continent IS NOT NULL)
	AND	(total_deaths IS NOT NULL)
	AND (location NOT LIKE '%income')
GROUP BY continent
ORDER BY 2 DESC;

-- Evolution of total cases by dates
SELECT
	date,
	SUM(new_cases) AS tot_cases,
	SUM(new_deaths) AS tot_deaths,
	ROUND(
		SUM(new_deaths::DECIMAL) / SUM(new_cases) * 100,
		2
	) AS death_pct
FROM covid_deaths
WHERE continent IS NOT NULL
GROUP BY date
ORDER BY 1,2;

-- Global metrics for whole period
SELECT
    SUM(new_cases) AS total_cases,
    SUM(new_deaths::INT) AS total_deaths,
    ROUND(
        SUM(new_deaths::NUMERIC) / SUM(new_cases) * 100,
        2
    ) AS mortality_rate
FROM covid_deaths
WHERE CONTINENT IS NOT NULL
ORDER BY 1, 2;

-- Total death count by continent
SELECT location, SUM(new_deaths::NUMERIC) AS total_death_count
FROM covid_deaths
WHERE
    continent IS NULL
    AND location NOT IN (
        'World',
        'European Union',
        'International',
        'High income',
        'Upper middle income',
        'Lower middle income',
        'Low income'
    )
GROUP BY location
ORDER BY total_death_count DESC;

-- Calculation of the infection rate by country and date
WITH temp_table AS (
	SELECT
		location,
		population,
		date,
		MAX(total_cases) AS infection_count,
		ROUND(
			MAX(total_cases::NUMERIC / population) * 100,
			2
		) AS infection_rate
	FROM covid_deaths
	WHERE
		location NOT IN (
			'World',
			'European Union',
			'Europe',
			'North America',
			'South America',
			'Asia',
			'Oceania',
			'Africa',
			'International'
		)
		AND location NOT LIKE '%income'
	GROUP BY location, population, date
	ORDER by infection_rate DESC
)
SELECT * FROM temp_table
WHERE infection_rate IS NOT NULL
ORDER BY 4 DESC;

-- Get time series of deaths by country
SELECT date, location, COALESCE(new_deaths, 0)
FROM covid_deaths
WHERE
	location NOT IN ('European Union', 'Europe', 'Africa', 'Oceania', 'North America', 'South America', 'Oceania', 'Asia')
	AND location NOT LIKE '%income'
ORDER BY date ASC

-- Rolling vaccination per location and date, CTE
WITH pop_vs_vac (continent, location, date, population, new_vaccinations, rolling_vaccination) AS (
    SELECT
        dea.continent,
        dea.location,
        dea.date,
        dea.population,
        vac.new_vaccinations,
        SUM(vac.new_vaccinations) OVER (
            PARTITION BY dea.location
            ORDER BY dea.location, dea.date
        ) AS rolling_vaccination
    FROM covid_deaths dea
    JOIN covid_vaccinations vac
        ON dea.location = vac.location
        AND dea.date = vac.date
    WHERE
        dea.continent IS NOT NULL
        AND dea.location = 'France'
)
SELECT *, (rolling_vaccination / population) * 100
FROM pop_vs_vac
ORDER BY 2, 3;

-- Rolling vaccination per location and date, temporary table
DROP TABLE IF EXISTS #PercentPopulationVaccinated
CREATE TABLE #PercentPopulationVaccinated (
    continent VARCHAR(60),
    location VARCHAR(60),
    date DATE,
    population NUMERIC,
    new_vaccinations NUMERIC,
    rolling_vaccination NUMERIC
)
INSERT INTO #PercentPopulationVaccinated
    SELECT
        dea.continent,
        dea.location,
        dea.date,
        dea.population,
        vac.new_vaccinations,
        SUM(vac.new_vaccinations) OVER (
            PARTITION BY dea.location
            ORDER BY dea.location, dea.date
        ) AS rolling_vaccination
    FROM covid_deaths dea
    JOIN covid_vaccinations vac
        ON dea.location = vac.location
        AND dea.date = vac.date
    WHERE
        dea.continent IS NOT NULL
        AND dea.location = 'France'
SELECT *, (rolling_vaccination / population) * 100
FROM #PercentPopulationVaccinated;
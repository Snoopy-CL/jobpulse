--staging

SELECT
    job_id,
    INITCAP(title) AS clean_title,
    LOWER(company) AS clean_company,
    CASE
        WHEN location ILIKE '%remote%' THEN 'Remote'
        ELSE INITCAP(location)
    END AS standardized_location,
    TRY_CAST(publication_date AS TIMESTAMP) AS publication_ts,
    salary,
    job_type,
    url,
    skills
FROM JOBPULSE_DB.PUBLIC.job_postings
WHERE job_id IS NOT NULL
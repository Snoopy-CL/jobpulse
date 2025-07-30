import requests
import json
import os
from dotenv import load_dotenv
import snowflake.connector

# open skills text file
def load_skills():
    with open('skill_list.txt', 'r', encoding = 'utf-8') as f:
        skills = []
        for line in f:
            stripped = line.strip()
            if stripped:
                skills.append(stripped)
    return skills

# get skills that exist in skill list text file
def extract_skills(description, skills):
    if not description:
        return []
    description_lower = description.lower()
    found_skills = []
    for skill in skills:
        if skill.lower() in description_lower:
            found_skills.append(skill)
    return found_skills

# get jobs from website
def get_jobs(category='data'):
    url = f'https://remotive.com/api/remote-jobs?category={category}'
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        return data.get('jobs', [])
    else:
        print('Failed to retrieve data', response.status_code)
        return []

# organize jobs
def organize_jobs(job, skills):
    description = job.get('description', '')
    skills_found = extract_skills(description, skills)
    organize_job = {
        'job_id':job.get('id'),
        'title': job.get('title'),
        'company': job.get('company_name'),
        'location': job.get('candidate_required_location'),
        "publication_date": job.get("publication_date"),
        "salary": job.get("salary"),
        "description": description,
        "job_type": job.get("job_type"),
        "url": job.get("url"),
        'skills': ', '.join(skills_found)
    }
    return organize_job

# save organized jobs to json
def save_json(jobs, filename='remotive_jobs.json'):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(jobs, f, ensure_ascii=False, indent=4)

if __name__ == '__main__':
    skills_list = load_skills()
    raw_jobs = get_jobs()
    cleaned_jobs = []
    for job in raw_jobs:
        organized = organize_jobs(job, skills_list)
        cleaned_jobs.append(organized)
    save_json(cleaned_jobs)
    print(f'{len(cleaned_jobs)} jobs to remotive_jobs.json')

# access .env
load_dotenv()

#access credentials for snowflake
user=os.getenv('SNOWFLAKE_USER')
password=os.getenv('SNOWFLAKE_PASSWORD')
account=os.getenv('SNOWFLAKE_ACCOUNT')
warehouse=os.getenv('SNOWFLAKE_WAREHOUSE')
database=os.getenv('SNOWFLAKE_DATABASE')
schema=os.getenv('SNOWFLAKE_SCHEMA')
role=os.getenv('SNOWFLAKE_ROLE')

# connect to snowflake
conn = snowflake.connector.connect(
    user=user,
    password=password,
    account=account,
    warehouse=warehouse,
    database=database,
    schema=schema,
    role=role
)
# create cursor for execution
cursor=conn.cursor()

# create table only if it doesnt exist
create_table="""
    CREATE OR REPLACE TABLE PUBLIC.job_postings(
    job_id STRING,
    title STRING,
    company STRING,
    location STRING,
    publication_date STRING,
    salary STRING,
    description STRING,
    job_type STRING,
    url STRING,
    skills STRING
)
"""
cursor.execute(create_table)

# Load json file into snowflake
with open('remotive_jobs.json', 'r', encoding='utf-8') as f:
    job_data = json.load(f)

# add data from json to table
transfer = """
INSERT INTO job_postings(
    job_id,
    title,
    company,
    location,
    publication_date,
    salary,
    description,
    job_type,
    url,
    skills
)VALUES (
    %(job_id)s,
    %(title)s,
    %(company)s,
    %(location)s,
    %(publication_date)s,
    %(salary)s,
    %(description)s,
    %(job_type)s,
    %(url)s,
    %(skills)s
)
"""

transferred_amount = 0
for job in job_data:
    try:
        cursor.execute(transfer, job)
        transferred_amount += 1
    except Exception as e:
        print(f'failed to transfer job id {job.get("job_id")}: {e}')

# close snowflake
cursor.close()
conn.close()
print(f'Successfully transferred {transferred_amount} jobs to Snowflake.')
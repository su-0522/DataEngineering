# Project2: Data Warehouse

- Youtube video I watched
  - https://www.youtube.com/watch?v=VFoYw6sDCEg&list=PLBJe2dFI4sgukOW6O0B-OVyX9c6fQKJ2N&index=4&ab_channel=DarshilParmar
  - data: https://www.postgresqltutorial.com/postgresql-getting-started/postgresql-sample-database/
  ![image](https://github.com/su-0522/DataEngineering/assets/166189404/406d6d15-7381-4e7f-b11f-869bb61c1e61)
  ![image](https://github.com/su-0522/DataEngineering/assets/166189404/2537837a-3a4a-4c26-a4f7-ce2db989952a)

## Data Warehouse
- Database: not good for analytic
  - too slow
  - too many joins
  - hard to understand
- Data warehouse
  - copy of transaction data specifically structured for query and analysis 
  - enable us to support analytic (OLAP) process
  - datasource -> ETL -> dimensional model
    - dimanesional model: make it easy for business users to work with data and improve queries performance
      - ex) star schema (fact table + dimension table)
      - fact table: record of business events (in quantifiable metrics)
      - dimension table: record of context of the business events

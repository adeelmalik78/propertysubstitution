# liquibase-Oracle-demo
This repo shows Azure Pipelines. There are three pipelines:
* CI pipeline --> azure-pipelines.yml
* UAT pipeline --> azure-pipelines-uat.yml
* PROD pipeline --> azure-pipelines-prod.yml

## CI Pipeline
CI pipeline is run as part of new PR validation for changes merging from `dev` branch to `release` branch. This pipeline executes `liquibase checks run` on incoming changes.

## UAT Pipeline
UAT pipeline implements three (3) stages:
* CreateArtifact - Publishes a new pipeline artifact
* UAT_Update - Deploys changes from newly created artifact to the UAT database
* UAT_Rollback - Rolls back UAT database by running `liquibase rollback <tag>` command. User is prompted for a specific artifact to use

  ## PROD Pipeline
  PROD pipeline implements two (2) stages:
  * PROD_Update - Deploys changes from a user-specified artifact to PROD database
  * PROD_Rollback - Rolls back PROD database by running `liquibase rollback <tag>` command. User is prompted for a specific artifact to use
 
## Database Connections
All information about UAT and PROD databases is stored in Azure Pipeline library as a variable group `Liquibase_Variables`. It includes connection information such as database URLs, usernames, passwords, etc.

## Liquibase Properties
Liquibase properties are stored in Azure Pipeline library as a variable group `Liquibase_Variables`. For example, Liquibase Pro license keys are stored here.


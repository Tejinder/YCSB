<!--
Copyright (c) 2012 - 2015 YCSB contributors. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You
may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. See accompanying
LICENSE file.
-->

## Quick Start

This section describes how to run YCSB on C8DB as Docker Container. 

### 1. Download the Docker File


### 2. Builder the docker image using Docker and below command

    docker build -t ycsbapp .
    

### 3. Now you are ready! First run the container for loading the data in C8DB

	docker run ycsbapp load c8db -s -P /usr/local/YCSB/YCSB/workloads/workload-measurements2-W1 -p c8db.host=<host-name> -p c8db.tenantName=ycsb-tenant_test.com -p c8db.databaseName=ycsb -p c8db.dropDBBeforeRun=true -p c8db.waitForSync=false -threads 50 -s 

Then, run the workload:

    docker run ycsbapp run c8db -s -P /usr/local/YCSB/YCSB/workloads/workload-measurements2-W1 -p c8db.host=<host-name> -p c8db.tenantName=ycsb-tenant_test.com -p c8db.databaseName=ycsb -p c8db.dropDBBeforeRun=true -p c8db.waitForSync=false -threads 50 -s 


## C8DB Configuration Parameters

- `c8db.host`
  - Default value is `localhost`

- `c8db.tenantName`
  - Tenant Name for which tests need to run.

- `c8db.databaseName`
  - Database name for the tests

- `c8db.waitForSync`
  - Default value is `true`.
  

- `c8db.dropDBBeforeRun`
  - Default value is `false`.
  
- `c8db.user`
  - User name to run the tests.
  
- `c8db.password`
  - Password for the user to run the tests.

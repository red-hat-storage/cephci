## Cron Pipeline
Cron pipeline job will execute long-running cephci test suites.
This job is scheduled to run weekly once on every Friday.

## Directory Structure
```
pipeline
    |_scripts
        |_cron
            |_5
               |_1
                  |_stage-0
                       |_ {suite_name}.sh
                       |_ {suite_name}.sh
                       |_ {suite_name}.sh
                       |_ {suite_name}.sh
                  |_stage-1
                        |_ {suite_name}.sh
                        |_ {suite_name}.sh
            |_4
               |_3
                  |_stage-0
                        |_ {suite_name}.sh
                        |_ {suite_name}.sh
                        |_ {suite_name}.sh
                        |_ {suite_name}.sh
```

## Guidelines
- Place the long-running test suite files under cron folder.
- Each stage is restricted to have 4 test suites (.sh files).
- Ensure the test suite names are descriptive.


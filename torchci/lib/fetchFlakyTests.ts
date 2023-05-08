import getRocksetClient from "./rockset";
import rocksetVersions from "rockset/prodVersions.json";

import { FlakyTestData } from "./types";

export default async function fetchFlakyTests(
  numHours: string = "3",
  testName: string = "%",
  testSuite: string = "%",
  testFile: string = "%"
): Promise<FlakyTestData[]> {
  const rocksetClient = getRocksetClient();
  const flakyTestQuery = await rocksetClient.queryLambdas.executeQueryLambda(
    "commons",
    "flaky_tests",
    rocksetVersions.commons.flaky_tests,
    {
      parameters: [
        {
          name: "numHours",
          type: "int",
          value: numHours,
        },
        {
          name: "name",
          type: "string",
          value: `%${testName}%`,
        },
        {
          name: "suite",
          type: "string",
          value: `%${testSuite}%`,
        },
        {
          name: "file",
          type: "string",
          value: `%${testFile}%`,
        },
      ],
    }
  );
  return flakyTestQuery.results ?? [];
}

export async function fetchFlakyTestsAcrossJobs(
  numHours: string = "3",
  threshold: number = 1,
  ignoreMessages: string = "No CUDA GPUs are available"
): Promise<FlakyTestData[]> {
  const rocksetClient = getRocksetClient();
  const flakyTestQuery = await rocksetClient.queryLambdas.executeQueryLambda(
    "commons",
    "flaky_tests_across_jobs",
    rocksetVersions.commons.flaky_tests_across_jobs,
    {
      parameters: [
        {
          name: "numHours",
          type: "int",
          value: numHours,
        },
        {
          name: "threshold",
          type: "int",
          value: threshold.toString(),
        },
        {
          name: "ignoreMessages",
          type: "string",
          value: ignoreMessages,
        },
      ],
    }
  );
  return flakyTestQuery.results ?? [];
}

export async function fetchFlakyTestsAcrossFileReruns(
  numHours: string = "3"
): Promise<FlakyTestData[]> {
  const rocksetClient = getRocksetClient();
  const failedTestsQuery = `
select
  t.name,
  t.file,
  t.invoking_file,
  t.classname,
  t.job_id,
  t._id,
from
  commons.test_run_s3 t
where
  t._event_time > CURRENT_TIMESTAMP() - HOURS(:numHours)
  and t.rerun is not null
  and (
      t.failure is not null
      or t.error is not null
  )
`;

  const checkEveryTestQuery = `
select
   t._id
from
    commons.test_run_s3 t
where
    t.name = :name
    and t.classname = :classname
    and t.job_id = :job_id
    and t.invoking_file = :invoking_file
    and t.failure is null
    and t.error is null
`;

  // Get every failed test on master (usually not a lot)
  const failedTests = await rocksetClient.queries.query({
    sql: {
      query: failedTestsQuery,
      parameters: [
        {
          name: "numHours",
          type: "int",
          value: numHours,
        },
      ],
    },
  });

  // for every failed test, query rockset to see if there is a rerun of the test
  // that succeeded on that same job.  This means that the test was rerun at the
  // file level and passed.  Do every test separately because test_run_s3 is so
  // big that it can't handle a join.  Output is a list of _ids for the test in
  // the table
  const res = await Promise.all(
    (failedTests.results ?? []).map(async (e) => {
      const a = await rocksetClient.queries.query({
        sql: {
          query: checkEveryTestQuery,
          parameters: [
            {
              name: "name",
              type: "string",
              value: e.name,
            },
            {
              name: "classname",
              type: "string",
              value: e.classname,
            },
            {
              name: "job_id",
              type: "int",
              value: e.job_id,
            },
            {
              name: "invoking_file",
              type: "string",
              value: e.invoking_file,
            },
            {
              name: "file",
              type: "string",
              value: e.file,
            },
          ],
        },
      });
      if (a.results?.length && a.results?.length > 0) {
        const ids = a.results?.map((e) => e._id);
        ids?.push(e._id);
        return ids;
      }
      return [];
    })
  );

  // Flatten the list of _ids for querying.  Put into format of
  // 'id1','id2','id3' since you can't have an array as parameter in rockset.
  // Query is copied from flaky_tests.sql
  const ids = res.flat().join(`','`);

  const flakyTestsQuery = `
select
    test_run.name,
    test_run.classname as suite,
    test_run.file,
    test_run.invoking_file,
    COUNT(*) as numGreen,
    SUM(
        if(
            TYPEOF(test_run.rerun) = 'object',
            1,
            Length(test_run.rerun)
        )
    ) as numRed,
    ARRAY_AGG(job.name) as jobNames,
    ARRAY_AGG(job.id) as jobIds,
    ARRAY_AGG(workflow.id) as workflowIds,
    ARRAY_AGG(workflow.name) as workflowNames,
    ARRAY_AGG(workflow.head_branch) as branches,
    ARRAY_AGG(test_run.workflow_run_attempt) as runAttempts
FROM
    commons.workflow_job job
    INNER JOIN commons.test_run_s3 test_run ON test_run.job_id = job.id HINT(join_strategy = lookup)
    INNER JOIN commons.workflow_run workflow ON job.run_id = workflow.id
where
    test_run._id in ('${ids}')
GROUP BY
    name,
    suite,
    file,
    invoking_file
  `;
  const flakyTests = await rocksetClient.queries.query({
    sql: {
      query: flakyTestsQuery,
    },
  });
  return flakyTests.results ?? [];
}

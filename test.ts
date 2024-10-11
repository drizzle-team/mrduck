import { mrduck } from ".";



const main = async () => {
    console.time("mrduckSql");
    const sql = mrduck({ dbUrl: "./db", max: 3, accessMode: "read_write" });

    const tasks = []
    const n = 1;
    for (let i = 0; i < n; i++) {
        let task = sql`
    SELECT
        blob4 as browser,
        count(DISTINCT blob3) AS visitorsCount
      FROM "orm.drizzle.test"
      WHERE timestamp >= 
        1726531200000  AND timestamp <= 1727098308012
    AND 1 = 1
      GROUP BY browser
      ORDER BY visitorsCount DESC
      LIMIT ALL;
      `;
        tasks.push(task);
        // console.log("sql func result:", result);

        task = sql`
    select
        count(distinct visitor_id) as uniqueVisitors,
        sum(entries) as pageViews,
        sum(case when entries = 1 then 1 else 0 end) as bounces,
        sum(case when entries >= 1 then 1 else 0 end) as visits,
        (bounces / visits) as rate,
        avg(max_time - min_time) as avgTime
from (
        select
                blob3 as visitor_id,
                blob12,
                count(timestamp) as entries,
                min(timestamp) as min_time,
                max(timestamp) as max_time
        from "orm.drizzle.test"
        where  timestamp >= 
        1726531200000  AND timestamp <= 1727098307195
        group by blob3, blob12
);
      `;
        tasks.push(task);
        // console.log("sql func result:", result);

        task = sql`
    SELECT
        blob11 as referrer,
        count(*) AS viewsCount
      FROM "orm.drizzle.test"
      WHERE timestamp >= 
        1726531200000  AND timestamp <= 1727098305499
    AND 1 = 1
      GROUP BY referrer
      ORDER BY viewsCount DESC
      LIMIT ALL
      `;
        tasks.push(task);
        // console.log("sql func result:", result);
    }

    const results = await Promise.all(tasks)
    // console.log(results);

    console.timeEnd("mrduckSql");
}

main();
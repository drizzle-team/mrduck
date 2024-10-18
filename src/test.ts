import { mrduck } from './index.ts';
// import duckdb from "duckdb";

// const connection = new duckdb.Database("./db", {
//     "access_mode": "read_only",
//     "max_memory": "512MB",
//     "threads": "4"
// }, (err) => {
//     if (err) {
//         console.error(err);
//     }
// });

const main = async () => {
	// const stream = connection.stream(`select blob1, blob2 from "orm.drizzle.test" limit 3;`)

	// const iterator = stream[Symbol.asyncIterator]();

	// let result = await iterator.next();
	// while (!result.done) {
	//     const row = result.value;
	//     // Process each row
	//     console.log(row);

	//     result = await iterator.next();
	// }

	console.time('mrduckSql');
	const sql = mrduck({ dbUrl: './db', max: 10, accessMode: 'read_write' });

	// console.log(await sql`select blob1, blob2 from "orm.drizzle.test" limit 3;`)
	// const gen = sql<{ blob1: string, blob2: string }>`select blob1, blob2 from "orm.drizzle.test" limit 8;`.chunked({ chunkSize: 3 });
	const gen = sql<{ blob1: string; blob2: string }>`select blob1, blob2 from "orm.drizzle.test" limit 8;`.stream();
	for await (const row of gen) {
		console.log(row);
	}

	// let result = await gen.next();
	// while (!result.done) {
	//     const rows = result.value;
	//     // Process each row
	//     console.log(rows);

	//     result = await gen.next();
	// }

	// @ts-ignore
	// let res = await sql`select ${Symbol("foooo")};`

	// const res = sql`insert into users (id, name) value ${sql.values([[sql.default, "name1"]])};`.toSQL();
	// @ts-ignore
	// res = await sql`insert into test_table (isActive) values (${true});`
	// let res = await sql`insert into test_table (date_) values (${new Date()});`
	// let res = await sql`insert into test_table (timestamp_) values (${(new Date())});`
	// console.log(res);
	// sql`select * from ${sql.identifier({})}`

	// const result1 = await sql<{ browser: string, visitorsCount: bigint }>`
	// SELECT
	//     ${sql.identifier(
	//     { schema: "main", table: "orm.drizzle.test", column: "blob4", as: "browser" },
	// )},
	//     count(DISTINCT ${sql.identifier("blob3")}) AS visitorsCount
	// FROM ${sql.identifier({ schema: "main", table: "orm.drizzle.test" })}
	// WHERE timestamp >= ${1726531200000} AND timestamp <= ${1727098308012}
	//     AND 1 = 1
	// GROUP BY browser
	// ORDER BY visitorsCount DESC
	// LIMIT ALL;
	// `;
	// console.log(result1);

	// const limit = 4;
	// const result2 = await sql<{ blob1: string, blob2: string, blob4: string }>`
	// SELECT ${sql.identifier([
	//     { column: "blob1" },
	//     { column: "blob2" },
	//     { schema: "main", table: "orm.drizzle.test", column: "blob4", as: "browser" },
	//     { schema: "main", table: "orm.drizzle.test", column: "blob6", as: "platform" }
	// ])},
	// FROM ${sql.identifier({ schema: "main", table: "orm.drizzle.test" })}
	// LIMIT ${limit};
	// `;
	// console.log(result2);

	// sql`select ${sql.identifier({ schema: "public", column: "name" })}, "email", "phone" from "public"."users";`.toSQL()
	// sql`select ${sql.identifier([])} from users;`.toSQL()

	// console.log(sql`insert into values ${sql.values([[1, 'Oleksii', true], [2, 'Alex']])};`.toSQL())
	// const tasks = []
	// const n = 100;
	// for (let i = 0; i < n; i++) {
	//     let task = sql`
	// SELECT
	//     blob4 as browser,
	//     count(DISTINCT blob3) AS visitorsCount
	//   FROM "orm.drizzle.test"
	//   WHERE timestamp >=
	//     ${1726531200000}  AND timestamp <= 1727098308012
	// AND 1 = 1
	//   GROUP BY browser
	//   ORDER BY visitorsCount DESC
	//   LIMIT ALL;
	//   `;
	//     tasks.push(task);
	//     // console.log("sql func result:", result);

	//     task = sql`
	//     select
	//         count(distinct visitor_id) as uniqueVisitors,
	//         sum(entries) as pageViews,
	//         sum(case when entries = 1 then 1 else 0 end) as bounces,
	//         sum(case when entries >= 1 then 1 else 0 end) as visits,
	//         (bounces / visits) as rate,
	//         avg(max_time - min_time) as avgTime
	// from (
	//         select
	//                 blob3 as visitor_id,
	//                 blob12,
	//                 count(timestamp) as entries,
	//                 min(timestamp) as min_time,
	//                 max(timestamp) as max_time
	//         from "orm.drizzle.test"
	//         where  timestamp >=
	//         1726531200000  AND timestamp <= 1727098307195
	//         group by blob3, blob12
	// );
	//       `;
	//     tasks.push(task);
	//     // console.log("sql func result:", result);

	//     task = sql`
	//     SELECT
	//         blob11 as referrer,
	//         count(*) AS viewsCount
	//       FROM "orm.drizzle.test"
	//       WHERE timestamp >=
	//         1726531200000  AND timestamp <= 1727098305499
	//     AND 1 = 1
	//       GROUP BY referrer
	//       ORDER BY viewsCount DESC
	//       LIMIT ALL
	//       `;
	//     tasks.push(task);
	//     // console.log("sql func result:", result);
	// }

	// const results = await Promise.all(tasks)
	// console.log(results);

	console.timeEnd('mrduckSql');
	// console.log(await sql`select 1;`);

	// console.log(sql`select ${100};`.toSQL())
};
// sql`select "id", "name" from ${sql.identifier(tableName)}`
main();

// type Identifier = string | { schema: string, table: string, column: string };

// const func = (table: Identifier, columns: Identifier[]) => {
//     sql`select ${sql.identifier(columns)} from ${sql.identifier(table)};`
//     sql`select ${sql.columns(columns)} from ${sql.identifier(table)};`

//     // as imported entity
//     sql`select ${columns.map(sql.identifier).join(", ")} from ${sql.identifier(table)};`

//     //
//     // sql`select ${columns(columns)} from ${identifier(schema)}.${identifier(table)};`

//     // sql`select ${columns(columns)} from ${identifier(schema)}.${identifier(table)};`

//     sql`insert into ${sql.identifier(table)} (${sql.identifier(columns)}) values (1, 'Oleksii'), (2, 'Alex');`

//     sql`insert into ${sql.identifier(table)} (${sql.identifier(columns)}) values ${sql.values([[1, 'Oleksii'], [2, 'Alex']])};`

//     sql`select ${sql.raw([1,2])};`

//     sql.unsafe(sqlString, ["params1", ...], { mode: "raw" | "default" })
// }

// func("users", ["id", "name"])
// func("users", ["id", "name", "email"])

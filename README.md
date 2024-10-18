# mrduck

list:

- add returning types to sql +
- implement funcs\methods which will be injected in sql as template parameters(features in postgres js): select list of columns for table +
  sql`select "id", "name" from ${sql.identifier()}`
  sql`select "id", "name" from "users"`
- add crud unittests
- now it's imposible to insert table/column name in query dynamically +

check where Date type can be used in mrduck
if date was given as param,
date in raw - toIsoString - no need, cause
date in values - toIsoString
sql`select *`.stream()
sql`select *`.chunked()

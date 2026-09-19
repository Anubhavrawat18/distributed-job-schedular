import { Pool, types } from "pg";
import { config } from "../config";

// pg returns BIGINT (int8) as a string to avoid precision loss. Job ids will
// never approach 2^53, so parse them as numbers to keep the types simple.
types.setTypeParser(types.builtins.INT8, (value) => Number(value));

// we are going ahead with the default pool of 10 connections
// you can also pass min and max as attributes in the Pool() method
export const pool = new Pool({
  connectionString: config.databaseUrl,
  //   max:20,
  //   min: 5
});

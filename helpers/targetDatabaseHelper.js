const sql = require("mssql");

// Funzione per connettersi al database
async function connectToDB(targetDB) {
  const { username, password, server, targetDatabase } = targetDB;

  const config = {
    user: username,
    password,
    server,
    database: targetDatabase,
    options: {
      encrypt: false,
      trustServerCertificate: true,
    },
    pool: {
      max: 20,
      min: 0,
      idleTimeoutMillis: 30000,
    },
  };

  try {
    const pool = await sql.connect(config);
    console.log("Connected to the database successfully.");
    return pool;
  } catch (err) {
    console.error("Database connection error:", err.message);
    throw new Error(`Failed to connect to database: ${err.message}`);
  }
}

async function checkRecord(targetDB, tableObject, record, pool) {
  const keysArray = tableObject.uniqueKey
    .split(",")
    .filter((key) => record[key] !== undefined);

  if (keysArray.length === 0) {
    // console.error("No valid keys provided for record check.");
    return false;
  }

  const whereClauses = keysArray.map(
    (key) => `${tableObject.columnPrefix}${key} = @${key}`
  );
  const readQuery = `
    SELECT COUNT(*) AS count 
    FROM ${targetDB.targetSchema}.${tableObject.objectName}
    WHERE ${whereClauses.join(" AND ")}
  `;

  const request = pool.request();

  keysArray.forEach((key) => {
    request.input(key, record[key]);
  });

  try {
    const result = await request.query(readQuery);
    return result.recordset[0].count > 0;
  } catch (err) {
    console.error("Error in checkRecord query:", err);
    throw err;
  }
}

// Funzione per fare un'operazione di "upsert" (insert, update or delete)
async function upsertRecord(targetDB, tableObject, record, pool) {
  let result;

  try {
    const recordExists = await checkRecord(targetDB, tableObject, record, pool);

    if (recordExists) {
      if (record.deleted) {
        console.log("Record exists, proceeding to delete...");
        await deleteRecord(targetDB, tableObject, record, pool);
        result = -1; // Record eliminato
      } else {
        console.log("Record exists, proceeding to update...");
        await updateRecord(targetDB, tableObject, record, pool);
        result = 0; // Record aggiornato
      }
    } else if (!record.deleted) {
      console.log("Record not found, proceeding to insert...");
      await insertRecord(targetDB, tableObject, record, pool);
      result = 1; // Record inserito
    }
  } catch (err) {
    console.error("Error during upsert operation:", err);
    throw err;
  }

  return result;
}

async function deleteRecord(targetDB, tableObject, record, pool) {
  const keysArray = tableObject.uniqueKey
    .split(",")
    .filter((key) => record[key] !== undefined);

  if (keysArray.length === 0) {
    console.error("No valid keys provided for deletion.");
    return;
  }

  const whereClauses = keysArray.map(
    (key) => `${tableObject.columnPrefix}${key} = @${key}`
  );
  const deleteQuery = `
    DELETE FROM ${targetDB.targetSchema}.${tableObject.objectName}
    WHERE ${whereClauses.join(" AND ")}
  `;

  const request = pool.request();
  keysArray.forEach((key) => request.input(key, record[key]));

  try {
    await request.query(deleteQuery);
    console.log("Record deleted successfully.");
  } catch (err) {
    console.error("Error in deleteRecord query:", err);
    throw err;
  }
}

async function updateRecord(targetDB, tableObject, record, pool) {
  const keysArray = tableObject.uniqueKey
    .split(",")
    .filter((key) => record[key] !== undefined);

  if (keysArray.length === 0) {
    console.error("No valid keys provided for update.");
    return;
  }

  const setClauses = Object.entries(record)
    .filter(
      ([key]) => tableObject.columns.includes(key) && record[key] !== undefined
    )
    .map(([key]) => `${tableObject.columnPrefix}${key} = @${key}`);

  if (setClauses.length === 0) {
    console.error("No fields to update.");
    return;
  }

  const whereClauses = keysArray.map(
    (key) => `${tableObject.columnPrefix}${key} = @${key}`
  );
  const updateQuery = `
    UPDATE ${targetDB.targetSchema}.${tableObject.objectName}
    SET ${setClauses.join(", ")}
    WHERE ${whereClauses.join(" AND ")}
  `;

  const request = pool.request();

  Object.entries(record).forEach(([key, value]) => {
    if (tableObject.columns.includes(key) && value !== undefined) {
      request.input(key, value);
    }
  });

  try {
    await request.query(updateQuery);
    console.log("Record updated successfully.");
  } catch (err) {
    console.error("Error in updateRecord query:", err);
    throw err;
  }
}

async function insertRecord(targetDB, tableObject, record, pool) {
  const excludedKeys = new Set([
    "accountingEntity",
    "variationNumber",
    "timestamp",
    "deleted",
  ]);

  const columns = Object.entries(record)
    .filter(
      ([key]) =>
        tableObject.columns.includes(key) &&
        !excludedKeys.has(key) &&
        record[key] !== undefined
    )
    .map(([key]) => tableObject.columnPrefix + key);

  const values = Object.entries(record)
    .filter(
      ([key]) =>
        tableObject.columns.includes(key) &&
        !excludedKeys.has(key) &&
        record[key] !== undefined
    )
    .map(([_, value]) => value);

  if (columns.length === 0) {
    console.error("No fields to insert.");
    return;
  }

  const insertQuery = `
    INSERT INTO ${targetDB.targetSchema}.${tableObject.objectName}
    (${columns.join(", ")}) 
    VALUES (${columns.map((_, index) => `@value${index}`).join(", ")});
  `;

  const request = pool.request();
  columns.forEach((_, index) => request.input(`value${index}`, values[index]));

  try {
    await request.query(insertQuery);
    console.log("Record inserted successfully.");
  } catch (err) {
    console.error("Error in insertRecord query:", err);
    throw err;
  }
}

module.exports = {
  connectToDB,
  upsertRecord,
  deleteRecord,
  updateRecord,
  insertRecord,
};

const sqlite3 = require("sqlite3").verbose();

function openDatabase(readonly = true) {
  try {
    const mode = readonly ? sqlite3.OPEN_READONLY : sqlite3.OPEN_READWRITE;
    const db = new sqlite3.Database("./dataLake.db", mode, (err) => {
      if (err) {
        throw new Error(`Error connecting to SQLite database: ${err.message}`);
      }
    });
    return db;
  } catch (error) {
    console.error("Failed to open the database:", error.message);
    return null; // Restituisce null in caso di errore
  }
}

async function getTargetDB(environment) {
  const db = openDatabase();
  if (!db) return null;

  const query = `SELECT server, username, password, targetDatabase, targetSchema FROM targetDatabase WHERE id = ?`;

  try {
    const targetDB = await new Promise((resolve, reject) => {
      db.get(query, [environment], (err, row) => {
        if (err) {
          reject(new Error(`Error retrieving target DB: ${err.message}`));
        } else if (!row) {
          reject(new Error(`No target DB found with ID ${environment}`));
        } else {
          resolve(row);
        }
      });
    });
    return targetDB;
  } catch (err) {
    console.error(err.message);
    return null; // Restituisce null in caso di errore
  } finally {
    db.close();
  }
}

async function getTablesByEnvironment(environmentId) {
  const db = openDatabase();
  if (!db) return [];

  const query = `SELECT dataLakeObjectID, environmentID, lastUpdate FROM dataLakeEnvironment WHERE activeYN=1 and environmentID = ?`;

  try {
    const tables = await new Promise((resolve, reject) => {
      db.all(query, [environmentId], (err, rows) => {
        if (err) {
          reject(new Error(`Error retrieving tables: ${err.message}`));
        } else if (rows.length === 0) {
          reject(
            new Error(`No tables found for environment ID: ${environmentId}`)
          );
        } else {
          resolve(rows);
        }
      });
    });
    return tables;
  } catch (err) {
    console.error(err.message);
    return []; // Restituisce un array vuoto in caso di errore
  } finally {
    db.close();
  }
}

async function getObjectByID(objectID) {
  const db = openDatabase();
  if (!db) return null;

  const query = `SELECT id, objectName, columnPrefix, uniqueKey, columns FROM dataLakeObjects WHERE id = ?`;

  try {
    const dataLakeObject = await new Promise((resolve, reject) => {
      db.get(query, [objectID], (err, row) => {
        if (err) {
          reject(new Error(`Error retrieving object by ID: ${err.message}`));
        } else if (!row) {
          reject(new Error(`No dataLakeObject found with ID ${objectID}`));
        } else {
          resolve(row);
        }
      });
    });
    return dataLakeObject;
  } catch (err) {
    console.error(err.message);
    return null; // Restituisce null in caso di errore
  } finally {
    db.close();
  }
}

async function getDataLakeEnvironmentLogsTimestamp(
  dataLakeEnvironmentID,
  objectID
) {
  const db = openDatabase(false); // Apertura del database in modalità di scrittura
  if (!db) return 0;

  const query = `SELECT MAX(timeUsedMs) AS maxTimestamp FROM dataLakeEnvironmentLogs WHERE dataLakeEnvironmentID = ? AND objectID = ?`;

  try {
    const maxTimestamp = await new Promise((resolve, reject) => {
      db.get(query, [dataLakeEnvironmentID, objectID], (err, row) => {
        if (err) {
          reject(new Error(`Error retrieving max timestamp: ${err.message}`));
        } else {
          resolve(row.maxTimestamp || 0); // Se non viene trovato alcun timestamp, restituisce 0
        }
      });
    });
    return maxTimestamp;
  } catch (err) {
    console.error(err.message);
    return 0; // Restituisce 0 in caso di errore
  } finally {
    db.close();
  }
}

async function insertDataLakeEnvironmentLogs(record) {
  const db = openDatabase(false); // Aperto in modalità di scrittura
  if (!db) return;

  try {
    const maxID = await new Promise((resolve, reject) => {
      const query = "SELECT MAX(id) AS maxID FROM dataLakeEnvironmentLogs";
      db.get(query, (err, row) => {
        if (err) {
          reject(new Error(`Error retrieving max ID: ${err.message}`));
        } else {
          resolve(row.maxID || 0);
        }
      });
    });

    const newID = maxID + 1;

    const insertQuery = `
      INSERT INTO dataLakeEnvironmentLogs 
      (id, dataLakeEnvironmentID, timestamp, objectID, timeUsedMs, recordsRead, recordsInserted, recordsUpdated, recordsDeleted)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `;

    await new Promise((resolve, reject) => {
      db.run(
        insertQuery,
        [
          newID,
          record.dataLakeEnvironmentID,
          record.timestamp,
          record.objectID,
          record.timeUsedMs,
          record.recordsRead,
          record.recordsInserted,
          record.recordsUpdated,
          record.recordsDeleted,
        ],
        function (err) {
          if (err) {
            reject(new Error(`Error inserting record: ${err.message}`));
          } else {
            resolve();
          }
        }
      );
    });

    console.log("Record inserted successfully with ID:", newID);
  } catch (err) {
    console.error("Error during insert operation:", err.message);
  } finally {
    db.close();
  }
}

async function updateLastUpdate(
  lastUpdateValue,
  dataLakeObjectID,
  environmentID
) {
  const db = openDatabase(false); // Aperto in modalità di scrittura
  if (!db) return;

  const query = `UPDATE dataLakeEnvironment SET lastUpdate = ? WHERE dataLakeObjectID = ? AND environmentID = ?`;

  try {
    await new Promise((resolve, reject) => {
      db.run(
        query,
        [lastUpdateValue, dataLakeObjectID, environmentID],
        function (err) {
          if (err) {
            reject(new Error(`Error updating lastUpdate: ${err.message}`));
          } else {
            resolve();
          }
        }
      );
    });

    console.log("Last update value updated successfully.");
  } catch (err) {
    console.error("Error updating lastUpdate:", err.message);
  } finally {
    db.close();
  }
}

module.exports = {
  getTablesByEnvironment,
  getObjectByID,
  getTargetDB,
  insertDataLakeEnvironmentLogs,
  getDataLakeEnvironmentLogsTimestamp,
  openDatabase,
  updateLastUpdate,
};

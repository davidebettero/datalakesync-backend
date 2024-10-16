const sqlite3 = require("sqlite3").verbose();
const fs = require("fs");
const jsonFilePath = "./mauri_prd.ionapi";

function sampleDB() {
  return new Promise((resolve, reject) => {
    let db = new sqlite3.Database(
      "./datalake.db",
      sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE,
      (err) => {
        if (err) {
          console.error("Error connecting to the database:", err.message);
          return reject(err); // Se c'è un errore, rifiuta la Promise
        } else {
          console.log("Connected to the database.");
        }
      }
    );

    // Creazione delle tabelle se non esistono
    db.run(`CREATE TABLE IF NOT EXISTS environment (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ti TEXT NOT NULL,
      cn TEXT NOT NULL,
      dt TEXT,
      ci TEXT,
      cs TEXT,
      iu TEXT,
      pu TEXT,
      oa TEXT,
      ot TEXT,
      revocation TEXT,
      ev TEXT,
      v TEXT,
      saak TEXT,
      sask TEXT
    )`);

    db.run(`CREATE TABLE IF NOT EXISTS dataLakeObjects (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      objectName TEXT NOT NULL,
      columnPrefix TEXT NOT NULL,
      uniqueKey TEXT,
      excludedColumn TEXT
    )`);

    db.run(`CREATE TABLE IF NOT EXISTS dataLakeEnvironment (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      dataLakeObjectID INTEGER NOT NULL,
      environmentID INTEGER NOT NULL,
      schedulation TEXT,
      activeYN BOOLEAN DEFAULT 1,
      lastID TEXT,
      lastUpdate DATETIME,
      recordSchedulation int,
      FOREIGN KEY (dataLakeObjectID) REFERENCES dataLakeObjects(id),
      FOREIGN KEY (environmentID) REFERENCES environment(id)
    )`);

    db.run(`CREATE TABLE IF NOT EXISTS dataLakeEnvironmentLogs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      dataLakeEnvironmentID INTEGER NOT NULL,
      timestamp DATETIME NOT NULL,
      objectID TEXT,
      timeUsedMs INTEGER,
      recordsRead INTEGER,
      recordsInserted INTEGER,
      recordsUpdated INTEGER,
      recordsDeleted INTEGER,
      FOREIGN KEY (dataLakeEnvironmentID) REFERENCES dataLakeEnvironment(id)
    )`);

    db.run(`CREATE TABLE IF NOT EXISTS errorLog (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      dataLakeLogID INTEGER NOT NULL,
      timestamp DATETIME NOT NULL,
      errorCode TEXT,
      severity TEXT,
      errorMessage TEXT,
      FOREIGN KEY (dataLakeLogID) REFERENCES dataLakeEnvironmentLogs(id)
    )`);

    db.run(`CREATE TABLE IF NOT EXISTS targetDatabase (
      id TEXT PRIMARY KEY,
      server TEXT NOT NULL,
      username TEXT NOT NULL,
      password TEXT NOT NULL,
      targetDatabase TEXT NOT NULL,
      targetSchema TEXT NOT NULL
    )`);

    // Funzione per leggere il file JSON e inserire i dati nel database
    fs.readFile(jsonFilePath, "utf8", (err, data) => {
      if (err) {
        console.error("Error reading file:", err.message);
        return reject(err); // Se c'è un errore, rifiuta la Promise
      }

      const jsonData = JSON.parse(data);

      const { ti, cn, dt, ci, cs, iu, pu, oa, ot, ev, v, saak, sask } =
        jsonData;
      const revocation = jsonData.or; // 'or' va in 'revocation'

      // Controlla se la tabella `environment` contiene già dati
      db.get("SELECT COUNT(*) AS count FROM environment", [], (err, row) => {
        if (err) {
          console.error("Error checking environment table:", err.message);
          return reject(err); // Se c'è un errore, rifiuta la Promise
        }

        if (row.count === 0) {
          const insertQuery = `
            INSERT INTO environment (ti, cn, dt, ci, cs, iu, pu, oa, ot, revocation, ev, v, saak, sask)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          `;

          db.run(
            insertQuery,
            [ti, cn, dt, ci, cs, iu, pu, oa, ot, revocation, ev, v, saak, sask],
            function (err) {
              if (err) {
                return reject(err); // Rifiuta la Promise in caso di errore
              }
              console.log(`Row inserted with rowid ${this.lastID}`);
              resolve(); // Risolvi la Promise dopo l'inserimento
            }
          );
        } else {
          console.log(
            "Table `environment` already contains data. No insertion executed."
          );
          resolve(); // Risolvi la Promise se i dati sono già presenti
        }
      });
    });
  });
}

module.exports = { sampleDB };

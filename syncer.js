const express = require("express");
//const app = express();
const { getBearerToken } = require("./helpers/oauthHelper");
const { sampleDB } = require("./helpers/sampleDB");
const {
  getTablesByEnvironment,
  getObjectByID,
  getTargetDB,
  insertDataLakeEnvironmentLogs,
  getDataLakeEnvironmentLogsTimestamp,
  updateLastUpdate,
} = require("./helpers/dbHelper");
const { getEventsByTable, getEventsByID } = require("./helpers/dataLakeHelper");
const { upsertRecord, connectToDB } = require("./helpers/targetDatabaseHelper");
const {
  getActualUTCISOString,
  convertToTimestamp,
  getGreaterTimestamp,
} = require("./helpers/timeFunction");

require("dotenv").config();

// Recupera l'ambiente da riga di comando
if (process.argv.length < 3) {
  console.error("No environment specified. Please provide an environment ID.");
  process.exit(1); // Termina il processo in caso di errore
}

const env = process.argv[2];

// Funzione principale per il download e l'elaborazione dei dati
async function downloadData(environment) {
  try {
    // Recupera le credenziali del database di destinazione
    const targetDBData = await getTargetDB(environment);
    if (!targetDBData) {
      throw new Error("Failed to retrieve target DB data.");
    }

    const targetDB = {
      server: targetDBData.server,
      username: targetDBData.username,
      password: targetDBData.password,
      targetDatabase: targetDBData.targetDatabase,
      targetSchema: targetDBData.targetSchema,
    };

    // Recupera il token Bearer per l'ambiente specificato
    const environmentData = await getBearerToken(environment);
    if (!environmentData || !environmentData.token) {
      throw new Error("Invalid environment data or token retrieved.");
    }

    // Recupera tutte le tabelle "attive"
    const tables = await getTablesByEnvironment(environmentData.id);
    if (!tables.length) {
      console.log(
        `No active tables found for environment: ${environmentData.id}`
      );
      return;
    }

    // Cicla su ogni tabella
    for (const table of tables) {
      try {
        const pool = await connectToDB(targetDB);
        if (!pool) {
          throw new Error("Failed to connect to target DB.");
        }

        const tableObject = await getObjectByID(table.dataLakeObjectID);
        if (!tableObject) {
          throw new Error(
            `Failed to retrieve object for ID: ${table.dataLakeObjectID}`
          );
        }

        // Legge gli eventi per la tabella
        const events = await getEventsByTable(environmentData, tableObject);
        if (!events || !events.data.fields || events.data.fields.length === 0) {
          console.log(`No events found for table ${tableObject.objectName}`);
          continue;
        }

        let inserted = 0;
        let updated = 0;
        let deleted = 0;
        const actualUTCISOString = getActualUTCISOString();
        let maxTimestamp = "";

        // Elabora ogni evento
        for (const eventField of events.data.fields) {
          try {
            const records = await getEventsByID(environmentData, eventField);
            if (!records || records.length === 0) {
              console.log(`No records found for event ${eventField}`);
              continue;
            }

            const timestamp = await getDataLakeEnvironmentLogsTimestamp(
              table.environmentID,
              tableObject.id
            );
            maxTimestamp = getGreaterTimestamp(
              maxTimestamp,
              records[0].timestamp
            );

            // Elabora ogni record
            for (const record of records) {
              const result = await upsertRecord(
                targetDB,
                tableObject,
                record,
                pool
              );
              if (result === 1) {
                inserted++;
              } else if (result === 0) {
                updated++;
              } else if (result === -1) {
                deleted++;
              }
            }
          } catch (error) {
            console.error(
              `Error processing event field ${eventField}:`,
              error.message
            );
          }
        }

        // Aggiorna i log di DataLake dopo aver elaborato tutti i record
        const log = {
          dataLakeEnvironmentID: table.environmentID,
          timestamp: actualUTCISOString,
          objectID: tableObject.id,
          timeUsedMs: convertToTimestamp(actualUTCISOString),
          recordsRead: inserted + updated + deleted,
          recordsInserted: inserted,
          recordsUpdated: updated,
          recordsDeleted: deleted,
        };

        await insertDataLakeEnvironmentLogs(log);
        await updateLastUpdate(
          maxTimestamp,
          tableObject.id,
          environmentData.id
        );
      } catch (error) {
        console.error(
          `Error processing table ${table.dataLakeObjectID}:`,
          error.message
        );
      }
    }
  } catch (error) {
    console.error("Error during downloadData operation:", error.message);
  }
}

// Inizializza il database se non esiste
sampleDB()
  .then(() => {
    // Avvia il download dei dati
    downloadData(env).catch((error) => {
      console.error("Failed to download data:", error.message);
    });
  })
  .catch((error) => {
    console.error("Error initializing the database:", error.message);
  });

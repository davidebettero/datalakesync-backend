const axios = require("axios");
const { openDatabase } = require("./dbHelper");

function getApiUrl(environmentData, endpoint, id = "") {
  try {
    if (!environmentData.iu || !environmentData.ti) {
      throw new Error("Missing required environment data for API URL.");
    }
    return `${environmentData.iu}/${environmentData.ti}/DATAFABRIC/datalake/v2/${endpoint}${id}`;
  } catch (error) {
    console.error("Error generating API URL:", error.message);
    return null; // Restituisce null in caso di errore
  }
}

async function getRecordSchedulationAndLastUpdate(
  dataLakeObjectID,
  environmentID
) {
  const db = openDatabase();
  if (!db) return null;

  const query = `SELECT recordSchedulation, lastUpdate FROM dataLakeEnvironment WHERE dataLakeObjectID = ? AND environmentID = ?`;

  try {
    const result = await new Promise((resolve, reject) => {
      db.get(query, [dataLakeObjectID, environmentID], (err, row) => {
        if (err) {
          reject(
            new Error(
              `Error retrieving schedulation and last update: ${err.message}`
            )
          );
        } else if (!row) {
          reject(
            new Error(
              `No data found for dataLakeObjectID: ${dataLakeObjectID} and environmentID: ${environmentID}`
            )
          );
        } else {
          resolve(row);
        }
      });
    });
    return result;
  } catch (error) {
    console.error(
      "Error in getRecordSchedulationAndLastUpdate:",
      error.message
    );
    return null; // Restituisce null in caso di errore
  } finally {
    db.close();
  }
}

async function getEventsByTable(environmentData, table) {
  const { recordSchedulation, lastUpdate } =
    await getRecordSchedulationAndLastUpdate(table.id, environmentData.id);
  if (!recordSchedulation) return null;

  const schedulation = recordSchedulation || 50;
  const filter = lastUpdate ? `and (event_date gt '${lastUpdate}')` : "";

  const apiUrl = getApiUrl(
    environmentData,
    "dataobjects",
    `?sort=event_date:asc&records=${schedulation}&filter=(dl_document_name eq '${table.objectName}') ${filter}`
  );

  if (!apiUrl) return null;

  const config = {
    headers: {
      Authorization: `Bearer ${environmentData.token}`,
    },
  };

  try {
    const response = await axios.get(apiUrl, config);
    return { data: response.data, lastUpdate };
  } catch (error) {
    console.error("Error in getEventsByTable:", error.message);
    return null; // Restituisce null in caso di errore
  }
}

async function getEventsByID(environmentData, event) {
  const apiUrl = getApiUrl(environmentData, "dataobjects/", event.dl_id);
  if (!apiUrl) return null;

  const config = {
    headers: {
      Authorization: `Bearer ${environmentData.token}`,
    },
    responseType: "arraybuffer",
  };

  try {
    const response = await axios.get(apiUrl, config);
    let jsonString = Buffer.from(response.data, "binary").toString("utf-8");

    // Garantisce la formattazione corretta della stringa JSON.
    jsonString = `[${jsonString.trim().replace(/\n/g, ",")}]`;
    const jsonArray = JSON.parse(jsonString);

    return jsonArray;
  } catch (error) {
    console.error("Error in getEventsByID:", error.message);
    return null; // Restituisce null in caso di errore
  }
}

module.exports = { getEventsByTable, getEventsByID };

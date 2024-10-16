const { ResourceOwnerPassword } = require("simple-oauth2");
const { openDatabase } = require("./dbHelper");

async function getBearerToken(environmentId) {
  const db = openDatabase();
  if (!db) return null;

  const query = `SELECT id, ti, ci, cs, iu, pu, oa, ot, saak, sask, revocation FROM environment WHERE ti = ?`;

  try {
    const environmentData = await new Promise((resolve, reject) => {
      db.get(query, [environmentId], (err, row) => {
        if (err) {
          reject(
            new Error(`Error retrieving environment data: ${err.message}`)
          );
        } else if (!row) {
          reject(
            new Error(`No environment data found for ID: ${environmentId}`)
          );
        } else {
          resolve(row);
        }
      });
    });

    if (
      !environmentData.ci ||
      !environmentData.cs ||
      !environmentData.iu ||
      !environmentData.pu
    ) {
      throw new Error("Missing required OAuth configuration fields.");
    }

    const oauth2Config = {
      client: {
        id: environmentData.ci,
        secret: environmentData.cs,
      },
      auth: {
        tokenHost: environmentData.iu,
        tokenPath: `${environmentData.pu}${environmentData.ot}`,
      },
    };

    const client = new ResourceOwnerPassword(oauth2Config);

    const tokenParams = {
      username: environmentData.saak,
      password: environmentData.sask,
    };

    // Tentativo di recuperare il token di accesso
    const accessToken = await client.getToken(tokenParams);

    if (!accessToken.token || !accessToken.token.access_token) {
      throw new Error("Invalid access token received.");
    }

    // Restituisce i dati dell'ambiente con il token
    environmentData.token = accessToken.token.access_token;
    return environmentData;
  } catch (error) {
    console.error("Error obtaining Bearer token:", error.message);
    return null; // Restituisce null in caso di errore
  } finally {
    db.close();
  }
}

module.exports = { getBearerToken };

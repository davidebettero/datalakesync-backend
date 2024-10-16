// Funzione per convertire una data (stringa) nel formato yyyy-MM-ddThh:mm:ss.mmmZ
// in un timestamp Unix (in millisecondi)
function convertToTimestamp(dateString) {
  try {
    const timestamp = new Date(dateString).getTime();

    if (isNaN(timestamp)) {
      throw new Error(`Invalid date string: ${dateString}`);
    }

    return timestamp;
  } catch (error) {
    console.error("Error converting to timestamp:", error.message);
    return null; // Restituisce null se la data non è valida
  }
}

// Funzione per convertire un timestamp Unix (in millisecondi)
// nel formato ISO 8601 yyyy-MM-ddThh:mm:ss.mmmZ
function convertToISOString(timestamp) {
  try {
    const isoString = new Date(timestamp).toISOString();

    if (!isoString) {
      throw new Error(`Invalid timestamp: ${timestamp}`);
    }

    return isoString;
  } catch (error) {
    console.error("Error converting to ISO string:", error.message);
    return null; // Restituisce null se il timestamp non è valido
  }
}

// Restituisce l'attuale data e ora UTC nel formato yyyy-MM-ddThh:mm:ss.mmmZ
function getActualUTCISOString() {
  return new Date().toISOString();
}

// Funzione per confrontare due timestamp e restituire quello maggiore
function getGreaterTimestamp(timestamp1, timestamp2) {
  if (!timestamp1) return timestamp2;
  if (!timestamp2) return timestamp1;

  try {
    const date1 = new Date(timestamp1);
    const date2 = new Date(timestamp2);

    if (isNaN(date1.getTime()) || isNaN(date2.getTime())) {
      throw new Error(`Invalid timestamp: ${timestamp1} or ${timestamp2}`);
    }

    return date1 > date2 ? timestamp1 : timestamp2;
  } catch (error) {
    console.error("Error comparing timestamps:", error.message);
    return null; // Restituisce null se il timestamp non è valido
  }
}

module.exports = {
  convertToTimestamp,
  convertToISOString,
  getActualUTCISOString,
  getGreaterTimestamp,
};

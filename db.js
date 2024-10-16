const mysql = require("mysql2");

// Crea la connessione al database
const db = mysql.createConnection({
  host: "db",
  user: "root", // Inserisci il tuo utente MySQL
  password: "root", // Inserisci la tua password MySQL
  database: "backend", // Il nome del database che hai creato
});

// Connessione al database
db.connect((err) => {
  if (err) {
    console.error("Errore di connessione al database:", err);
    return;
  }
  console.log("Connesso al database MySQL");
});

module.exports = db;

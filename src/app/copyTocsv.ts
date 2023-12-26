import fs from 'fs';
// const pgp = require('pg-promise')();

// // PostgreSQL database connection details
// const db1 = pgp({
//     database: 'postgres',
//     user: 'postgres',
//     password: '2001',
//     host: 'localhost',
//     port: 5432
// });

// Define the CSV to PostgreSQL transformation function
async function csvToPostgres(csvFilePath: string, tableName: string) {
    try {
        // Read CSV file

        const csvData = fs.readFileSync(csvFilePath, 'utf8');

        // Parse CSV data into an array of objects
        const csvRows = csvData.split('\n').map((row: string) => row.split(','));
        const columns = csvRows[0].map((column: string) => column.trim());
        const values = csvRows.slice(1).map((row: any[]) => row.map((value: string) => value.trim()));

        // Create the table in PostgreSQL
        await db.none(`CREATE TABLE ${tableName} (${columns.map((column: any) => `${column} VARCHAR(255)`).join(', ')});`);

        // Insert data into the table
        await db.none(`INSERT INTO ${tableName} (${columns.join(', ')}) VALUES ${values.map((row: any[]) => `(${row.map((value: any) => `'${value}'`).join(', ')})`).join(', ')};`);

        console.log(`CSV data successfully transformed and loaded into the ${tableName} table.`);
    } catch (error) {
        console.error('Error:', error);
    } finally {
        pgp.end();
    }
}


csvToPostgres('movies.csv', 'movies');
csvToPostgres('ratings.csv', 'ratings');

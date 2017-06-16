require('dotenv').config();
const mysql = require('mysql');
var parse = require('csv-parse/lib/sync');
const iconv = require('iconv-lite');
const fs = require('fs');

var connection = mysql.createConnection({
    host: process.env.MYSQL_HOST,
    user: process.env.MYSQL_USER,
    password:process.env.MYSQL_PASSWORD,
    database: process.env.MYSQL_DB
});

connection.connect();

fillBairro();
// fillEscola();
// fillPontoTuristico();

connection.end();


// -------------------------------------------------------
function convertCSV(filename) {
    var buffer = fs.readFileSync(filename);

    // Convert encoding to utf8
    var str = iconv.decode(buffer, 'win1252');

    return parse(str, {columns: true});
}

function fillBairro() {
    var rows = convertCSV('./csv/bairros_.csv');

    var sql = 'insert into Bairro values (?, ?, ?)';
    for(let row of rows) {
        let inserts = [row.Bairro, row.Latitude, row.Longitude];
        connection.query(mysql.format(sql, inserts) , (err, rows, fields) => {
            if(err)
                throw err; 
        });
    }

    console.log('Table Bairro filled');
}

function fillEscola() {
    var rows = convertCSV('./csv/escolas__.csv');

    var sql = 'insert into escola values (?, ?, ?, ?, ?, ?, ?, ?)';
    for(let row of rows) {
        if(row.Bairro === '')
            break;

        row.Bairro = row.Bairro.replace(/ -.*/, '');
        if(row.Logradouro === '')
            row.Logradouro = 'NULL';
        if(!isNormalInteger(row['Número']))
            row['Número'] = 'NULL';

        if(row['IDEB 1º Segmento'] === '')
            row['IDEB 1º Segmento'] = 'NULL';
        if(row['IDEB 2º Segmento'] === '')
            row['IDEB 2º Segmento'] = 'NULL';

        let inserts = [row['Designação'], row.Nome, row.Logradouro, row['Número'], row.Bairro, row.CEP, row['IDEB 1º Segmento'], row['IDEB 2º Segmento']];
        let query = mysql.format(sql, inserts);
        connection.query(query, (err, rows, fields) => {
            if(err)
                throw err;
        });
    }

    console.log('Table Escola filled');
}

function fillPontoTuristico() {
    var rowsTeatros = convertCSV('./csv/teatros.csv');
    var rowsMuseus = convertCSV('./csv/museus_.csv');

    for(let row of rowsTeatros) {
        if(!isNormalInteger(row['Número']))
            row['Número'] = 'NULL';
        connection.query(`INSERT INTO PTuristico VALUES ("${row.Nome}", "${row['Endereço']}", ${row['Número']}, "${row.Bairro}")`, (err, rows, fields) => {
            if(err)
                throw err; 
        });
    }

    for(let row of rowsMuseus) {
         if(!isNormalInteger(row['Número']))
            row['Número'] = 'NULL';
        connection.query(`INSERT INTO PTuristico VALUES ("${row.Nome}", "${row['Endereço']}", ${row['Número']}, "${row.Bairro}")`, (err, rows, fields) => {
            if(err)
                throw err; 
        });
    }

    console.log('Table PTuristico filled');
}

function isNormalInteger(str) {
    return /^\+?(0|[1-9]\d*)$/.test(str);
}
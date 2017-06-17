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

// fillBairro();
// fillEscola();
// fillPontoTuristico();
// fillDisciplina();
// fillEscolaDisciplina();

fillTelefone();

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
				console.log(err);
				// throw err; 
		});
	}

	console.log('Table Bairro filled');
}

function fillEscola() {
	var rows = convertCSV('./csv/escolas__.csv');
	var sql = 'insert into Escola values (?, ?, ?, ?, ?, ?, ?, ?, ?)';
	for(let row of rows) {
		if(row.Bairro === '')
			continue;

		row.Bairro = row.Bairro.replace(/ -.*/, '');
		if(row.Logradouro === '')
			row.Logradouro = null;
		if(!isNormalInteger(row['Número']))
			row['Número'] = null;

		if(row['IDEB 1º Segmento'] === '')
			row['IDEB 1º Segmento'] = null;
		if(row['IDEB 2º Segmento'] === '')
			row['IDEB 2º Segmento'] = null;

		let inserts = [row['Designação'], row.Logradouro, row['Número'], row.Nome,
			row['IDEB 1º Segmento'], row['IDEB 2º Segmento'], row.Bairro, row.Latitude, row.Longitude];
		let query = mysql.format(sql, inserts);
		connection.query(query, (err, rows, fields) => {
			if(err)
				console.log(err);
				// throw err; 
		});
	}

	console.log('Table Escola filled');
}

function fillPontoTuristico() {
	var rowsTeatros = convertCSV('./csv/teatros.csv');
	var rowsMuseus = convertCSV('./csv/museus_.csv');

	for(let row of rowsTeatros) {
		if(!isNormalInteger(row['Número']))
			row['Número'] = null;
		connection.query(`INSERT INTO Ponto_Turistico VALUES ("${row.Nome}", "${row['Endereço']}", ${row['Número']}, 'Teatro', "${row.Bairro}")`, (err, rows, fields) => {
			if(err)
				console.log(err);
				// throw err; 
		});
	}

	for(let row of rowsMuseus) {
		 if(!isNormalInteger(row['Número']))
			row['Número'] = null;
		connection.query(`INSERT INTO Ponto_Turistico VALUES ("${row.Nome}", "${row['Endereço']}", ${row['Número']}, 'Museu', "${row.Bairro}")`, (err, rows, fields) => {
			if(err)
				console.log(err);
				// throw err; 
		});
	}

	console.log('Table PTuristico filled');
}

function fillDisciplina() {
	var buffer = fs.readFileSync('./csv/ProfessoresEscola.csv');
	var str = iconv.decode(buffer, 'win1252');

	var sql = 'insert into Disciplina (nome) values (?)';
	var nomes = str.split('\n')[0].split(',').slice(3, -3);

	for(let nome of nomes) {
		connection.query(`insert into Disciplina (nome) values ("${nome}")`, (err, rows, fields) => {
			if(err)
				console.log(err);
				// throw err; 
		});
	}

	console.log("Table Disciplina filled");
}

function fillEscolaDisciplina() {
	var entries = convertCSV('./csv/ProfessoresEscola.csv');
	var disciplinas = Object.keys(entries[0]).slice(3, -3);

	for(let entry of entries) {
		let idEscola = entry['Designação'];

		console.log(`- idEscola: ${idEscola}`);
		for(let i = 0; i < disciplinas.length; i++) {
			if(entry[disciplinas[i]] > 0) {
				connection.query(`insert into Escola_Disciplina values (${i + 1}, ${idEscola}, ${entry[disciplinas[i]]})`, (err, rows, fields) => {
					if(err)
						console.log(err);
				});	
			}
		}
	}
}

function fillTelefone() {
	var rowsTeatros = convertCSV('./csv/teatros.csv');
	var rowsMuseus = convertCSV('./csv/museus_.csv');

	for(let row of rowsTeatros) {
		connection.query(`INSERT INTO Telefone VALUES ("${row.Telefone}", "${row.Nome}")`, (err, rows, fields) => {
			if(err)
				console.log(err);
				// throw err; 
		});

		connection.query(`INSERT INTO Telefone VALUES ("${ramdomPhone()}", "${row.Nome}")`, (err, rows, fields) => {
			if(err)
				console.log(err);
				// throw err; 
		});
	}

	for(let row of rowsMuseus) {
		connection.query(`INSERT INTO Telefone VALUES ("${row.Telefone}", "${row.Nome}")`, (err, rows, fields) => {
			if(err)
				console.log(err);
				// throw err; 
		});

		connection.query(`INSERT INTO Telefone VALUES ("${ramdomPhone()}", "${row.Nome}")`, (err, rows, fields) => {
			if(err)
				console.log(err);
				// throw err; 
		});
	}
}

function isNormalInteger(str) {
	return /^\+?(0|[1-9]\d*)$/.test(str);
}

function ramdomPhone() {
	return Math.floor(Math.random()*8999+1000) + '-' + Math.floor(Math.random()*8999+1000);
}
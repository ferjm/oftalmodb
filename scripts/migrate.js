/**
 * Time consuming script to migrate from old legacy DSM oftalmo database schema
 * to the new shinny oftalmo.js one. This data walked through the painfull
 * path from dbf to sql and it's waiting to rest under mongodb's hands.
 */

require('./latinise');

// A little bit of color for the output.
var cconsole = require('colorize').console;

function throwError(error) {
  cconsole.log('#red[' + error +']');
  throw error;
}

// ** Database functions **

// Database connections
var mysqlConnection;
var mongoConnection;

function connectToMysql(password, callback) {
  console.log('Connecting to mysql database...');
  var mysql = require('mysql');
  mysqlConnection = mysql.createConnection({
    host: process.argv[2],
    port: process.argv[3],
    database: process.argv[4],
    user: process.argv[5],
    password: password
  });

  mysqlConnection.connect();
  if (callback) {
    callback();
  }
}

function createMongo(callback) {
  console.log('Creating mongodb...');
  var database = 'oftalmo';
  var collections = ['patients'];
  mongoConnection = require('mongojs').connect(database, collections);
  if (callback) {
    callback();
  }
}

//** Normalizations and checks **

String.prototype.reduceWhiteSpace = function() {
  return this.replace(/\s+/g, ' ');
};

// Each doctor would expect different companies. We use this values to
// validate the stored data about patient's companies.
// TODO: get from external config file.
var expectedCompanies = [
  'ONCE',
  'ADESLAS',
  'ASISA',
  'PRIVADO',
  'CASER',
  'ABOGADOS'
];
function checkCompany(company) {
  return true;
//  return (company.length == 0 ||
//          expectedCompanies.indexOf(company) != -1);
}

function normalizePatientData(patient) {
  if (!checkCompany(patient.NOMBRE)) {
    throwError('Unknown company ' + patient.NOMBRE);
  }
  // We might find the "SURNAME, NAME" format.
  var patientName = patient.PACIENTE.split(',');
  patient.PACIENTE = patientName[1].trim();
  patient.SURNAME = patientName[0];

  // To normalize the patient data we match 'weird' characters to its
  // corresponding latin one for each of the string parameters.
  for (var key in patient) {
    if (patient.hasOwnProperty(key)) {
      if (typeof patient[key] === 'string') {
        patient[key] = patient[key].latinise();
      }
    }
  }
  return patient;
}

function parsePatient(patient) {
  return {
    codPac: patient.COD_PAC,
    name: patient.PACIENTE,
    surname: patient.SURNAME,
    postal_address: [{
      address: patient.DIRECCION,
      city: patient.POBLACION,
      state: patient.PROVINCIA,
      postal_code: patient.CODPOS,
      country: patient.PAIS
    }],
    phone: [{
      type: 'personal',
      number: patient.TELEFONO
    }],
    date_birth: patient.FECHAN,
    job: patient.PROFESION,
    referred_by: patient.PRESENTADO,
    id: patient.DNI,
    company: patient.NOMBRE
  };
}

function parseDate(date) {
  var dateParts;
  var meses = {
    'enero': '1',
    'febrero': '2',
    'marzo': '3',
    'abril': '4',
    'mayo': '5',
    'junio': '6',
    'julio': '7',
    'agosto': '8',
    'septiembre': '9',
    'octubre': '10',
    'noviembre': '11',
    'diciembre': '12'
  };

  if (date.indexOf('de') != -1) {
    dateParts = date.split('de');
    dateParts[1] = dateParts[1].trim();
    if (dateParts[1].indexOf(' ') != -1) {
      var parts = dateParts[1].split(' ');
      dateParts.pop();
      parts.forEach(function(item) {
        dateParts.push(item);
      });
    }
    if (meses[dateParts[1].toLowerCase()]) {
      dateParts[1] = meses[dateParts[1].toLowerCase()];
    }

  } else if (date.indexOf('-') != -1) {
    dateParts = date.split('-');
  } else {
    dateParts = date.split(' ');
  }
  var result;
  result = new Date(Date.UTC(dateParts[2], (dateParts[1] - 1), dateParts[0]));
  if (!result || result == 'Invalid date') {
    throwError('No valid date ' + date);
  }
  return result;
}

function parseTonometry(clinic) {
  var tonometry = [];
  var regExp = /uTONOMETRIAu Fecha \d+\D\d+\D\d+ Hora (\d+:\d+) PIO OD (\d+) PIO OI (\d+)/;
  do {
    var matches = regExp.exec(clinic);
    if (!matches) {
      break;
    }
    clinic = clinic.replace(regExp, '');
    tonometry.push({
      time: matches[1],
      od: matches[2],
      oi: matches[3],
      clinic: clinic
    });
  } while(clinic.search(regExp));
  return tonometry;
}

function parseDonder(clinic) {
  // AVSC ESFVL CIL EJE AVCC ESFVP AV
  var regExp = /uDONDERSu AV ESF VL CIL EJE AV ESF VP AV úOD ([-+]?[0-9]*\.?[0-9]*) ([-+]?[0-9]*\.?[0-9]*) ([-+]?[0-9]*\.?[0-9]*) ([-+]?[0-9]*\.?[0-9]*)=([-+]?[0-9]*\.?[0-9]*) ([-+]?[0-9]*\.?[0-9]*) ([-+]?[0-9]*\.?[0-9]*)ú úOI ([-+]?[0-9]*\.?[0-9]*) ([-+]?[0-9]*\.?[0-9]*) ([-+]?[0-9]*\.?[0-9]*) ([-+]?[0-9]*\.?[0-9]*)=([-+]?[0-9]*\.?[0-9]*) ([-+]?[0-9]*\.?[0-9]*) ([-+]?[0-9]*\.?[0-9]*)ú/;
  var matches = regExp.exec(clinic);
  if (!matches) {
    return;
  }
  return {
    od: {
      avsc: matches[1],
      evl: matches[2],
      cil: matches[3],
      eje: matches[4],
      avcc: matches[5],
      evp: matches[6],
      avcc2: matches[7]
    },
    oi: {
      avsc: matches[8],
      evl: matches[9],
      cil: matches[10],
      eje: matches[11],
      avcc: matches[12],
      evp: matches[13],
      avcc2: matches[14]
    },
    clinic: clinic.replace(regExp, '')
  };
}

var visitGetters = {
  getTonometries: function getTonometries(patient, callback) {

    function processTonometry(visit, callback) {
      return function(error, rows, fields) {
        if (error) {
          throwError(error);
        }

        if (rows && rows.length) {
          visit.tonometry = [];
          for (var j = 0; j < rows.length; j++) {
            visit.tonometry.push({
              od: rows[j].OD,
              oi: rows[j].OI,
              time: rows[j].HORA
            });
          }
          callback(visit);
        } else{
          callback(visit);
        }
      };
    }

    var visits = patient.visits;
    var codPac = patient.codPac;
    require('./date.js');
    if (!visits.length) {
      callback(visits);
    }

    var result = [];
    var asyncCalls = 0;
    for (var i = 0; i < visits.length; i++) {
      var query = 'SELECT OD, OI, HORA FROM TONOS WHERE COD_PAC=' + codPac +
                  ' AND FECHA=\"' + visits[i].date.toString('yyyy-MM-d') + '\"';
      asyncCalls++;
      mysqlConnection.query(query, processTonometry(visits[i], function(visit) {
        result.push(visit);
        asyncCalls--;
        if (!asyncCalls) {
          callback(result);
        }
      }));
    }
  },

  getRecords: function getRecords(patient, callback) {

    function processRecord(visit, callback) {
      return function(error, rows, fields) {
        if (error) {
          // TODO uncomment with final data. We need to catch missing tables.
          //throwError(error);
          callback(visit);
          return;
        }

        // Process each record.
        if (rows && rows.length) {
          visit.record = [];
        } else {
          callback(visit);
          return;
        }

        for (var i = 0; i < rows.length; i++) {
          var record = {
            title: rows[i].TITULO.latinise(),
            content: rows[i].CARTA.latinise()
          };
          visit.record.push(record);
        }

        callback(visit);
      };
    };

    var visits = patient.visits;
    var codPac = patient.codPac;

    require('./date.js');

    var result = [];
    var asyncCalls = 0;
    for (var i = 0; i < visits.length; i++) {
      var visit = visits[i];

      if (isNaN(visit.date.getFullYear())) {
        throwError('NaN ' + visit.date.toString());
      }

      var table = '$' + visit.date.getFullYear();

      var query = 'SELECT TITULO, FECHA, CARTA FROM ' + table + ' WHERE COD_PAC=' +
                  codPac + ' AND FECHA=\"' + visit.date.toString('yyyy-M-d') + '\"';
      asyncCalls++;
      mysqlConnection.query(query, processRecord(visit, function(visit) {
        result.push(visit);
        asyncCalls--;
        if (!asyncCalls) {
          callback(result);
        }
      }));
    }
  },

  getOptometrics: function getOptometrics(patient, callback) {

    function processOptometric(visit, callback) {
      return function(error, rows, fields) {
        if (error) {
          throwError(error);
        }

        if (rows && rows.length) {
          visit.optometrics = [];
          for (var j = 0; j < rows.length; j++) {
            visit.optometrics.push({
              date: rows[j].FECHA,
              right_eye: rows[j].OD,
              left_eye: rows[j].OI,
              right_eye_sc: rows[j].OD_SC,
              left_eye_sc: rows[j].OI_SC
            });
          }
          callback(visit);
        } else{
          callback(visit);
        }
      };
    }

    var visits = patient.visits;
    var codPac = patient.codPac;
    require('./date.js');
    if (!visits.length) {
      callback(visits);
    }

    var result = [];
    var asyncCalls = 0;
    for (var i = 0; i < visits.length; i++) {
      var query = 'SELECT FECHA, OD, OI, OD_SC, OI_SC FROM AGUDEZAS ' +
                  'WHERE COD_PAC=' + codPac + ' AND FECHA=\"' +
                  visits[i].date.toString('yyyy-MM-d') + '\"';
      asyncCalls++;
      mysqlConnection.query(query, processOptometric(visits[i],
        function(visit) {
        result.push(visit);
        asyncCalls--;
        if (!asyncCalls) {
          callback(result);
        }
      }));
    }
  },

};

function parseVisits(patient, callback) {

  function getVisits(visits) {
    var result = [];
    console.log('\n' + visits);
    // First of all we get the date of each visit.
    var regExp = /([1-9]|0[1-9]|[12][0-9]|3[01])(\D|\sde\s)([1-9]|0[1-9]|1[012]|Enero|Febrero|Marzo|Abril|Mayo|Junio|Julio|Agosto|Septiembre|Octubre|Noviembre|Diciembre)(\D|\sde\s)(19[0-9][0-9]|20[0-9][0-9])/g;
    do {
      var matches = regExp.exec(visits);
      if (!matches) {
        break;
      }
      var date = parseDate(matches[0]);
      //cconsole.log('#red[Possible: ' + date + ']');
      // Dates might be repeated, so we don't store the index for repeated ones
      if (!result.length || (result[result.length - 1] &&
          result[result.length - 1].date.getTime() != date.getTime())) {
        //cconsole.log('#green[Date found: ' + date + ']');
        // We store the index where the date was found. We will be using this
        // index to retrieve the data contained between dates.
        result.push({
          date: date,
          lastIndex: regExp.lastIndex,
          dateLength: matches[0].length
        });
      }
    } while (visits.substr(regExp.lastIndex).search(regExp));
    return result;
  }

  // The table names containing the details of each visit has a weird
  // naming based in the patients code.
  var tableName = parseFloat(patient.COD_PAC);
  tableName = tableName/100;
  if (isNaN(tableName)) {
    throwError('NaN for ' + patient.COD_PAC);
    return;
  }
  tableName = tableName.toString().split('.')[0];
  while (tableName.length < 5) {
    tableName = '0' + tableName;
  }
  tableName = '_' + tableName;
  query = 'SELECT * FROM ' + tableName + ' WHERE COD_PAC=' + patient.COD_PAC;
  cconsole.log('#bold[Querying table ' + tableName + ' for visits ' +
               'details for patient ' + patient.COD_PAC + ' ' + query + ']');
  mysqlConnection.query(query, function (error, rows, fields) {
    result = [];
    if (error) {
      //cconsole.log('#red[***' + error + ']' + patient.PACIENTE);
      callback(result);
      return;
    }

    if (!rows || rows.length === 0) {
      cconsole.log('#blue[Patient has no visits]');
      callback(result);
      return;
    }

    for (var i = 0; i < rows.length; i++) {
      // Kill the extra white spaces and trim.
      rows[i].CLINICA = rows[i].CLINICA.reduceWhiteSpace().trim().latinise();

      // Parsing visits is not that easy when the format is all screwed up...
      // First of all we try to identify each visit according to its date.
      // We expect dates as 'dd-mm-yyyy', 'dd/mm/yyyy' or 'dd de mm del yyyy'.
      var visits = getVisits(rows[i].CLINICA);
      if (!visits) {
        throwError('We were expecting to parse valid visits for ' +
                   JSON.stringify(rows[i]));
      }

      // Once we have the date of each visit, we can get more data associated
      // to it.
      for (var j = 0; j < visits.length; j++) {
        var index = visits[j].lastIndex;
        // Store the clinic content.
        if (!visits[j+1]) {
          visits[j].clinic = rows[i].CLINICA.substr(index);
        } else {
          var nextIndex = visits[j+1].lastIndex;
          visits[j].clinic = rows[i].CLINICA.substr(index, nextIndex - index -
                                                    visits[j].dateLength);
        }
        // Remove not needed indexes.
        delete visits[j].lastIndex;
        delete visits[j].dateLength;

        var tonometry = parseTonometry(visits[j].clinic);
        if (tonometry.length) {
          visits[j].clinic = tonometry[tonometry.length - 1].clinic;
          for (var k = 0; k < tonometry.length; k++) {
            delete tonometry[k].clinic;
          }
          visits[j].tonometry = tonometry;
        }

        // Get the donders data which should contain optometric information.
        var donder = parseDonder(visits[j].clinic);
        if (donder) {
          visits[j].clinic = donder.clinic;
          delete donder.clinic;
          visits[j].donder = donder;
        }

        result.push(visits[j]);
      } // for end

    } // for end

    callback && callback(result);
  });
}

function populatePatientWithVisits(row, patient, callback) {
  return function() {
    parseVisits(row, function(visits) {
      patient.visits = result;
      cconsole.log('#yellow[PATIENT] ' + JSON.stringify(patient, null, 2));
      callback(patient);
    });
  }
}

function migratePatients(callback) {
  if (!mysqlConnection) {
    return;
  }

  console.log('Retrieving patients data...');
  var query = 'SELECT PACIENTE, DIRECCION, POBLACION, PROVINCIA, TELEFONO, ' +
              'FECHAN, PROFESION, PRESENTADO, DNI, CODPOS, TEXTO, PAIS, ' +
              'GAF, COD_PAC, NOMBRE FROM PACIENTE';

  var patients = [];
  mysqlConnection.query(query, function (error, rows, fields) {
    if (error) {
      throwError(error);
    }

    var asyncCalls = rows.length;
    // Process each patient.
    for (var i = 0; i < rows.length; i++) {
      cconsole.log('#blue[Patient ' + i + ' of ' + rows.length + ']: ' +
                   JSON.stringify(rows[i]));

      // Normalize patient personal data and create the patient object which
      // would be populated with the data from other tables from the legacy
      // database.
      var patient;
      try {
        patient = normalizePatientData(rows[i]);
      } catch (e) {
        cconsole.log('#red[' + e + ']');
        process.exit();
      }

      patient = parsePatient(patient);

      populatePatientWithVisits(rows[i], patient, function(result) {
        patients.push(result);
        asyncCalls--;
        if (asyncCalls) {
          return;
        }
        // At this point all patients are well formed with the MongoDB
        // format and contain the array of visits. After this we need to
        // process each visit.
        callback(patients);
      })();
    }
  });
}

function migrateVisitStuff(stuff, patients, callback) {
  var result = [];

  function onStuff(patient, callback) {
    return function(visits) {
      patient.visits = visits;
      callback(patient);
    }
  }

  console.log('Migrating ' + stuff + ' from ' + patients.length);

  var asyncCalls = 0;
  for (var i = 0; i < patients.length; i++) {
    if (!patients[i].visits || !patients[i].visits.length) {
      result.push(patients[i]);
      continue;
    }
    asyncCalls++;
    var func = 'get' + stuff;
    if (!visitGetters[func]) {
      throwError('Function does not exist');
    }
    visitGetters[func](patients[i], onStuff(patients[i], function(patient) {
      asyncCalls--;
      result.push(patient);
      console.log('Patient ' + patient.codPac + ' - visits ' + patient.visits.length);
      if (!asyncCalls) {
        callback && callback(result);
      }
    }));
  }
}

function migrateTonometries(patients, callback) {

  var result = [];

  function onTonometries(patient, callback) {
    return function(visits) {
      patient.visits = visits;
      callback(patient);
    }
  }

  console.log('Migrating tonometries ' + patients.length);

  var asyncCalls = 0;
  for (var i = 0; i < patients.length; i++) {
    if (!patients[i].visits || !patients[i].visits.length) {
      result.push(patients[i]);
      continue;
    }
    asyncCalls++;
    getTonometries(patients[i], onTonometries(patients[i], function(patient) {
      asyncCalls--;
      result.push(patient);
      console.log('Patient ' + patient.codPac + ' - visits ' + patient.visits.length);
      if (!asyncCalls) {
        callback && callback(result);
      }
    }));
  }
}

function migrateRecords(patients, callback) {

  var result = [];

  function onRecords(patient, callback) {
    return function(visits) {
      patient.visits = visits;
      callback(patient);
    }
  }

  console.log('Migrating records ' + patients.length);

  var asyncCalls = 0;
  for (var i = 0; i < patients.length; i++) {
    if (!patients[i].visits || !patients[i].visits.length) {
      result.push(patients[i]);
      continue;
    }
    asyncCalls++;
    getRecords(patients[i], onRecords(patients[i], function(patient) {
      asyncCalls--;
      result.push(patient);
      console.log('Patient ' + patient.codPac + ' - visits ' + patient.visits.length);
      if (!asyncCalls) {
        callback && callback(result);
      }
    }));
  }
}

// Check script arguments.
if (process.argv.length < 6) {
  cconsole.log('#red[You are doing it wrong!]\nUsage:\n\t' +
              'migrate.js <host> <port> <database> <user>');
  process.exit(0);
}

// Presenting the beast.
cconsole.log('\n#bold[DSM oftalmo database migration tool]');
cconsole.log('#bold[-------------------------------------]\n');

// Ask for mysql database password.
cconsole.log('#green[Database password]');

var stdin = process.openStdin();
var tty = require('tty');
process.stdin.setRawMode(true);
stdin.resume();
stdin.setEncoding('utf8');

var password = '';
stdin.on('data', function (ch) {
  ch = ch + '';
  switch(ch) {
    case '\n':
    case '\r':
    case '\u0004':
      // They've finished typing their password, so we open the mysql db.
      tty.setRawMode(false);
      stdin.pause();
      console.log('\n');
      createMongo(
        connectToMysql(password, function() {
          migratePatients(function(patients) {
            migrateVisitStuff('Tonometries', patients, function(patients) {
              migrateVisitStuff('Records', patients, function(patients) {
                migrateVisitStuff('Optometrics', patients, function(patients) {
                  patients.forEach(function(patient) {
                    mongoConnection.patients.save(patient, function(err, result) {
                      if (err || !result) {
                        console.log('Oh crap! ', err);                        
                      }
                      console.log('MONGO - ', JSON.stringify(result));
                    });
                  });
                });
              });
            });
          });
        })
      );
      break;
    case '\u0003':
      // Ctrl-C
      process.exit();
      break;
    default:
      // More password characters.
      process.stdout.write('*');
      password += ch;
      break;
  }
});

process.on('exit', function () {
  console.log('Cleaning stuff');
  mysqlConnection.end();
  mongoConnection.close();
});

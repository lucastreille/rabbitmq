const amqplib = require('amqplib');
const config = require('./config');


function isValidNumber(value) {
  return !isNaN(value) && value !== '';
}


function isValidOperation(op) {
  return ['add', 'sub', 'mul', 'div', 'all'].includes(op);
}


console.log('---------------------------------------');
console.log('Bienvenue dans le calculateur distribué!');
console.log('Vous pouvez envoyer des requêtes de calcul au système.');
console.log('---------------------------------------');


async function sendOperation(n1, n2, operation) {

  try {

    const connection = await amqplib.connect(config.RABBITMQ_URL);
    const channel = await connection.createChannel();
    
    await channel.assertExchange(config.OPERATION_EXCHANGE, config.EXCHANGE_TYPES.DIRECT, { durable: true });
    await channel.assertExchange(config.OPERATIONS.ALL, config.EXCHANGE_TYPES.FANOUT, { durable: true });
    
    const task = {
      n1: n1,
      n2: n2,
      op: operation
    };
    

    const message = JSON.stringify(task);
    

    if (operation === 'all') {

        channel.publish(config.OPERATIONS.ALL, '', Buffer.from(message), { persistent: true });
        console.log(`[x] Envoi de la requête à tous les workers: ${message}`);

    } else {

        channel.publish(config.OPERATION_EXCHANGE, operation, Buffer.from(message), { persistent: true });
        console.log(`[x] Requête envoyée pour opération ${operation}: ${message}`);

    }

    
    console.log('\nEn attente des résultats...');
    console.log('(Les résultats seront affichés par le client récepteur)\n');
    
    setTimeout(() => {
      connection.close();
    }, 500);
    
    
  } catch (error) {
    console.error('Erreur de connexion:', error);
  }

}

const args = process.argv.slice(2);

if (args.length < 3) {
  console.log('Usage: node user-client.js [nombre1] [nombre2] [operation]');
  console.log('Exemple: node user-client.js 5 3 add');
  console.log('Operations disponibles: add, sub, mul, div, all');
  process.exit(1);
}

const n1 = parseFloat(args[0]);
const n2 = parseFloat(args[1]);
const operation = args[2].toLowerCase();

if (!isValidNumber(n1)) {
  console.error('Erreur: Le premier nombre n\'est pas valide');
  process.exit(1);
}

if (!isValidNumber(n2)) {
  console.error('Erreur: Le deuxième nombre n\'est pas valide');
  process.exit(1);
}

if (!isValidOperation(operation)) {
  console.error('Erreur: L\'opération n\'est pas valide');
  console.error('Opérations disponibles: add, sub, mul, div, all');
  process.exit(1);
}

sendOperation(n1, n2, operation).then(() => {
  console.log('Opération envoyée avec succès');
  console.log('Vous pouvez lancer d\'autres opérations en exécutant à nouveau cette commande');
});
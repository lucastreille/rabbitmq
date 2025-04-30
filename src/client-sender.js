const amqplib = require('amqplib');
const config = require('./config');


function getRandomInt(min, max) {

  min = Math.ceil(min);
  max = Math.floor(max);
  return Math.floor(Math.random() * (max - min + 1)) + min;

}


function getRandomOperation() {

  const operations = [
    config.OPERATIONS.ADD,
    config.OPERATIONS.SUB,
    config.OPERATIONS.MUL,
    config.OPERATIONS.DIV
  ];

  return operations[getRandomInt(0, operations.length - 1)];

}


function getRandomOperationWithAll() {

  const operations = [
    config.OPERATIONS.ADD,
    config.OPERATIONS.SUB,
    config.OPERATIONS.MUL,
    config.OPERATIONS.DIV,
    config.OPERATIONS.ALL
  ];

  return operations[getRandomInt(0, operations.length - 1)];

}

async function sendBasicRequests() {

  try {

    const connection = await amqplib.connect(config.RABBITMQ_URL);
    const channel = await connection.createChannel();
    


    await channel.assertExchange(config.TASK_EXCHANGE, config.EXCHANGE_TYPES.DIRECT, { durable: true });
    await channel.assertQueue(config.TASK_QUEUE, { durable: true });
    await channel.bindQueue(config.TASK_QUEUE, config.TASK_EXCHANGE, 'task');

    
    console.log('Client émetteur connecté à RabbitMQ');
    console.log('Envoi d\'une requête toutes les 5 secondes');
    console.log('Mode: Addition uniquement (basic)');
    

    setInterval(() => {

      const n1 = getRandomInt(1, 100);
      const n2 = getRandomInt(1, 100);
      
      const task = {
        n1: n1,
        n2: n2
      };
      
      const message = JSON.stringify(task);
      channel.publish(config.TASK_EXCHANGE, 'task', Buffer.from(message), { persistent: true });
      
      console.log(`[x] Requête envoyée: ${message}`);

    }, config.REQUEST_INTERVAL);

  } catch (error) {
    console.error('Erreur de connexion:', error);
  }

}


async function sendRequestsWithOperations() {

  try {

    const connection = await amqplib.connect(config.RABBITMQ_URL);
    const channel = await connection.createChannel();
    

    await channel.assertExchange(config.OPERATION_EXCHANGE, config.EXCHANGE_TYPES.DIRECT, { durable: true });
    

    await channel.assertQueue(config.ADD_QUEUE, { durable: true });
    await channel.bindQueue(config.ADD_QUEUE, config.OPERATION_EXCHANGE, config.OPERATIONS.ADD);
    
    await channel.assertQueue(config.SUB_QUEUE, { durable: true });
    await channel.bindQueue(config.SUB_QUEUE, config.OPERATION_EXCHANGE, config.OPERATIONS.SUB);
    
    await channel.assertQueue(config.MUL_QUEUE, { durable: true });
    await channel.bindQueue(config.MUL_QUEUE, config.OPERATION_EXCHANGE, config.OPERATIONS.MUL);
    
    await channel.assertQueue(config.DIV_QUEUE, { durable: true });
    await channel.bindQueue(config.DIV_QUEUE, config.OPERATION_EXCHANGE, config.OPERATIONS.DIV);
    

    console.log('Client émetteur connecté à RabbitMQ');
    console.log('Envoi de requêtes avec différentes opérations');
    console.log('Mode: Opérations variées (ops)');

    
    setInterval(() => {

      const n1 = getRandomInt(1, 100);
      const n2 = getRandomInt(1, 100);
      const operation = getRandomOperation();
      
      const task = {
        n1: n1,
        n2: n2,
        op: operation
      };
      
      const message = JSON.stringify(task);
      
      channel.publish(config.OPERATION_EXCHANGE, operation, Buffer.from(message), { persistent: true });
      console.log(`[x] Requête envoyée pour opération ${operation}: ${message}`);

    }, getRandomInt(1000, 5000));

  } catch (error) {
    console.error('Erreur de connexion:', error);
  }

}


async function sendRequestsWithAll() {

  try {

    const connection = await amqplib.connect(config.RABBITMQ_URL);
    const channel = await connection.createChannel();
    

    await channel.assertExchange(config.OPERATION_EXCHANGE, config.EXCHANGE_TYPES.DIRECT, { durable: true });
    
    await channel.assertExchange(config.OPERATIONS.ALL, config.EXCHANGE_TYPES.FANOUT, { durable: true });
    

    await channel.assertQueue(config.ADD_QUEUE, { durable: true });
    await channel.bindQueue(config.ADD_QUEUE, config.OPERATION_EXCHANGE, config.OPERATIONS.ADD);
    await channel.bindQueue(config.ADD_QUEUE, config.OPERATIONS.ALL, '');
    
    await channel.assertQueue(config.SUB_QUEUE, { durable: true });
    await channel.bindQueue(config.SUB_QUEUE, config.OPERATION_EXCHANGE, config.OPERATIONS.SUB);
    await channel.bindQueue(config.SUB_QUEUE, config.OPERATIONS.ALL, '');
    
    await channel.assertQueue(config.MUL_QUEUE, { durable: true });
    await channel.bindQueue(config.MUL_QUEUE, config.OPERATION_EXCHANGE, config.OPERATIONS.MUL);
    await channel.bindQueue(config.MUL_QUEUE, config.OPERATIONS.ALL, '');
    
    await channel.assertQueue(config.DIV_QUEUE, { durable: true });
    await channel.bindQueue(config.DIV_QUEUE, config.OPERATION_EXCHANGE, config.OPERATIONS.DIV);
    await channel.bindQueue(config.DIV_QUEUE, config.OPERATIONS.ALL, '');
    

    console.log('Client émetteur connecté à RabbitMQ');
    console.log('Envoi de requêtes avec opération ALL incluse');
    console.log('Mode: Incluant ALL (all)');

    
    setInterval(() => {

      const n1 = getRandomInt(1, 100);
      const n2 = getRandomInt(1, 100);
      const operation = getRandomOperationWithAll();
      
      const task = {
        n1: n1,
        n2: n2,
        op: operation
      };
      
      const message = JSON.stringify(task);
      
      if (operation === config.OPERATIONS.ALL) {
        // Pour ALL, on publie sur l'exchange fanout
        channel.publish(config.OPERATIONS.ALL, '', Buffer.from(message), { persistent: true });
        console.log(`[x] Requête ALL envoyée à tous les workers: ${message}`);
      } else {
        // Pour les autres opérations, on publie sur l'exchange direct
        channel.publish(config.OPERATION_EXCHANGE, operation, Buffer.from(message), { persistent: true });
        console.log(`[x] Requête envoyée pour opération ${operation}: ${message}`);
      }

    }, getRandomInt(1000, 5000));

  } catch (error) {
    console.error('Erreur de connexion:', error);
  }

}



const args = process.argv.slice(2);
const mode = args[0] || 'basic';


switch(mode) {

  case 'basic':
    sendBasicRequests();
    break;

  case 'ops':
    sendRequestsWithOperations();
    break;

  case 'all':
    sendRequestsWithAll();
    break;

  default:
    console.error('Mode non reconnu. Utilisez: basic, ops ou all');
    console.error('Utilisation: node client-sender.js [mode]');
    process.exit(1);
    
}